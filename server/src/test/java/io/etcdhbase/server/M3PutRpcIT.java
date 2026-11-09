package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.hbase.storage.HBaseMaterializationStore;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Pinned Go client against the real Java Put handler and disposable HBase layouts. */
class M3PutRpcIT {
  @Test
  void realGoPutMatchesNativeAcrossLayouts() throws Exception {
    Path root = WireProcesses.repositoryRoot();
    String configuredRun = System.getenv("M3_RUN_ID");
    String runId = configuredRun == null ? "rpc-" + UUID.randomUUID() : configuredRun;
    if (!runId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid M3 fixture run ID");
    }
    String configuredRoot = System.getenv("M3_ARTIFACT_ROOT");
    Path artifacts = configuredRoot == null
        ? root.resolve(".artifacts/m3").resolve(runId) : Path.of(configuredRoot).toAbsolutePath();
    if (!artifacts.normalize().startsWith(root.resolve(".artifacts/m3").toAbsolutePath().normalize())
        || !artifacts.getFileName().toString().equals(runId)) {
      throw new IllegalArgumentException("M3 fixture output must be under its run directory");
    }
    Files.createDirectories(artifacts);
    String prefix = "etcd-hbase-m3-test/" + runId + "/";
    Path nativeTrace = artifacts.resolve("native-trace.jsonl");
    if (!Files.exists(nativeTrace)) {
      runDriver(root, artifacts.resolve("fixture-native.log"), "--native-data-dir",
          artifacts.resolve("native-data").toString(), "--scope", "native-etcd", "--prefix", prefix,
          "--trace", nativeTrace.toString(), "--metadata", artifacts.resolve("native-metadata.json").toString());
    }
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      String suffix = layout.name().toLowerCase(Locale.ROOT);
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout);
           M3PutCommitIT.Engine engine = new M3PutCommitIT.Engine(fixture);
           Bootstrap server = Bootstrap.startConfigured(
               new RuntimeConfiguration.Listener("m3-test", "127.0.0.1", 0,
                   fixture.system().tableGroupId()),
               new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000,
                   4 << 20, 4 << 20, 16, 0), engine.putService())) {
        assertForeignTableCannotJoinGroup(fixture);
        runDriver(root, artifacts.resolve("target-" + suffix + ".log"),
            "--endpoint", "127.0.0.1:" + server.boundListener().port(), "--scope", "target-hbase",
            "--layout", layout.name(), "--prefix", prefix,
            "--trace", artifacts.resolve("target-" + suffix + "-trace.jsonl").toString(),
            "--metadata", artifacts.resolve("target-" + suffix + "-metadata.json").toString());
        assertLeaseFailsBeforePublication(fixture, server, prefix);
        assertPhysicalRecords(fixture, prefix);
        assertLostOwnerReplyUsesOriginalResult(fixture, engine, server, prefix);
      }
      run(root, artifacts.resolve("compare-" + suffix + ".log"), List.of(
          root.resolve("scripts/dev/go-program.sh").toString(), "native-etcd", "compare", "--m3-put",
          "--left", nativeTrace.toString(), "--right",
          artifacts.resolve("target-" + suffix + "-trace.jsonl").toString()));
    }
    String source = System.getenv("M3_SOURCE_COMMIT");
    if (source != null) {
      if (!source.matches("[0-9a-f]{40}")) throw new IllegalArgumentException("invalid source commit");
      String metadata = "{\"kind\":\"java-grpc-hbase-minicluster\",\"run_id\":\"" + runId
          + "\",\"source_commit\":\"" + source
          + "\",\"authentication\":\"dev-no-auth\",\"layouts\":[\"HISTORY\",\"NATIVE\"]}\n";
      Files.writeString(artifacts.resolve("target-fixture.json"), metadata,
          java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE);
    }
  }

  private static void assertForeignTableCannotJoinGroup(M3TestEnvironment fixture) throws Exception {
    var foreign = fixture.createForeignPrimary();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> HBaseMaterializationStore.openForTableGroup(fixture.factory(), fixture.identity(),
              fixture.system(), foreign, fixture.current(), scope).join());
      var backend = assertInstanceOf(BackendFailure.class, failure.getCause());
      assertEquals(BackendFailure.Category.UNKNOWN, backend.category());
      var mismatch = assertInstanceOf(IllegalArgumentException.class, backend.getCause());
      assertEquals("data table does not belong to the configured table group", mismatch.getMessage());
      assertEquals(1, fixture.control().read(scope).join().orElseThrow().state().publishedRevision());
    }
  }

  private static void assertLeaseFailsBeforePublication(
      M3TestEnvironment fixture, Bootstrap server, String prefix) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
        .usePlaintext().build();
    try {
      var request = Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8(prefix + "lease"))
          .setValue(ByteString.copyFromUtf8("v")).setLease(1).build();
      var failure = assertThrows(StatusRuntimeException.class,
          () -> KVGrpc.newBlockingStub(channel).withDeadlineAfter(8, TimeUnit.SECONDS).put(request));
      assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode());
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
        assertEquals(10, fixture.control().read(scope).join().orElseThrow().state().publishedRevision());
      }
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertPhysicalRecords(M3TestEnvironment fixture, String prefix) throws Exception {
    byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
    byte[] key = Arrays.copyOf(prefixBytes, prefixBytes.length + 2);
    key[key.length - 1] = (byte) 0xff;
    byte[] longKey = Arrays.copyOf(prefixBytes, prefixBytes.length + 8192);
    Arrays.fill(longKey, prefixBytes.length, longKey.length, (byte) 'x');
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var store = fixture.openPrimary(scope);
      var current = store.get(ByteKey.copyOf(key), Long.MAX_VALUE, ReadProjection.FULL).join().orElseThrow();
      assertArrayEquals(new byte[] {'n', 0, (byte) 0x80}, current.value().orElseThrow());
      assertEquals(2, current.metadata().createRevision());
      assertEquals(9, current.metadata().modificationRevision());
      assertEquals(8, current.metadata().version());
      var large = store.get(ByteKey.copyOf(longKey), Long.MAX_VALUE, ReadProjection.FULL).join().orElseThrow();
      assertArrayEquals(new byte[] {(byte) 0xff}, large.value().orElseThrow());
      assertEquals(10, large.metadata().modificationRevision());
      assertEquals(10, fixture.control().read(scope).join().orElseThrow().state().publishedRevision());
    }
  }

  private static void assertLostOwnerReplyUsesOriginalResult(M3TestEnvironment fixture,
      M3PutCommitIT.Engine engine, Bootstrap server, String prefix) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
        .usePlaintext().build();
    try {
      var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS);
      ByteString key = ByteString.copyFromUtf8(prefix + "lost-owner-reply");
      var first = stub.put(Rpc.PutRequest.newBuilder().setKey(key)
          .setValue(ByteString.copyFromUtf8("before")).build());
      long sequence;
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
        sequence = fixture.control().read(scope).join().orElseThrow().state().publishedSequence();
      }
      engine.dropNextOwnerReply();
      var reply = stub.put(Rpc.PutRequest.newBuilder().setKey(key)
          .setValue(ByteString.copyFromUtf8("after")).setPrevKv(true).build());
      assertEquals(first.getHeader().getRevision() + 1, reply.getHeader().getRevision());
      assertTrue(reply.hasPrevKv());
      assertEquals(key, reply.getPrevKv().getKey());
      assertEquals(ByteString.copyFromUtf8("before"), reply.getPrevKv().getValue());
      assertEquals(first.getHeader().getRevision(), reply.getPrevKv().getModRevision());
      assertEquals(1, reply.getPrevKv().getVersion());
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
        var state = fixture.control().read(scope).join().orElseThrow().state();
        assertEquals(sequence + 1, state.publishedSequence());
        assertEquals(reply.getHeader().getRevision(), state.publishedRevision());
      }
      engine.failNextOwnerSend();
      var failed = assertThrows(StatusRuntimeException.class, () -> stub.put(
          Rpc.PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8("not-written")).build()));
      assertEquals(Status.Code.INTERNAL, failed.getStatus().getCode());
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
        var state = fixture.control().read(scope).join().orElseThrow().state();
        assertEquals(sequence + 1, state.publishedSequence());
        assertEquals(reply.getHeader().getRevision(), state.publishedRevision());
      }
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void runDriver(Path root, Path log, String... args) throws Exception {
    var command = new java.util.ArrayList<String>();
    command.add(root.resolve("scripts/dev/go-program.sh").toString());
    command.add("m3driver");
    command.add("--scenario");
    command.add(root.resolve("testkit/scenarios/m3-put.yaml").toString());
    command.addAll(List.of(args));
    run(root, log, command);
  }

  private static void run(Path root, Path log, List<String> command) throws Exception {
    Process process = new ProcessBuilder(command).directory(root.toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (!process.waitFor(180, TimeUnit.SECONDS)) throw new IllegalStateException("M3 driver timed out");
      assertEquals(0, process.exitValue(), "M3 driver failed; inspect its local run log");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }

}
