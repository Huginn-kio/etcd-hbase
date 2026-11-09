package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the configured server activation against a disposable HBase cluster. */
@Timeout(300)
class M3RuntimeIT {
  @TempDir Path directory;

  @Test
  void startsConfiguredServiceAndRecoversWithStableHeaders() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      var initialization = initialization(cluster.clientConfiguration()
          .getInt("hbase.zookeeper.property.clientPort", -1));
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      var plan = M3TableGroupPlan.from(source);
      var authScope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new BackendCredentials("m3-initializer", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture().get(30, TimeUnit.SECONDS);
        var firstActivation = activation("9223372036854775808");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
        var config = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m3-local", "127.0.0.1", 0, "local")),
            initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(),
            new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000, 4 << 20, 4 << 20, 4, 0));
        Rpc.PutResponse first;
        var server = RuntimeServer.startM3(config, firstActivation);
        var metrics = server.metrics().getFirst();
        try {
          assertTrue(server.dataReady());
          first = put(server, "before");
          assertEquals(initialized.clusterId(), first.getHeader().getClusterId());
          assertEquals(Long.MIN_VALUE, first.getHeader().getMemberId());
          assertEquals(2, first.getHeader().getRevision());
        } finally {
          server.close();
        }
        assertFalse(server.dataReady());
        for (var stage : RequestMetrics.PutStage.values()) {
          assertTrue(metrics.putStages().get(stage).count() > 0, "missing observed stage " + stage);
        }
        assertTrue(metrics.putOutputBytes() > 0);
        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
        try (var restarted = RuntimeServer.startM3(config, activation("2"))) {
          var second = put(restarted, "after");
          assertEquals(first.getHeader().getClusterId(), second.getHeader().getClusterId());
          assertEquals(2, second.getHeader().getMemberId());
          assertEquals(3, second.getHeader().getRevision());
          assertTrue(second.hasPrevKv());
          assertEquals(ByteString.copyFromUtf8("before"), second.getPrevKv().getValue());
        }
        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
        int port;
        try (var reservation = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
          port = reservation.getLocalPort();
        }
        Path runtimeFile = writeRuntimeConfiguration(initialization, port);
        Process child = startRuntimeProcess(runtimeFile, writeActivation("3"), "first");
        try {
          var third = put(port, "child-one");
          assertEquals(first.getHeader().getClusterId(), third.getHeader().getClusterId());
          assertEquals(3, third.getHeader().getMemberId());
          assertEquals(4, third.getHeader().getRevision());
          assertEquals(ByteString.copyFromUtf8("after"), third.getPrevKv().getValue());
        } finally {
          stopRuntimeProcess(child);
        }
        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
        child = startRuntimeProcess(runtimeFile, writeActivation("4"), "second");
        try {
          var fourth = put(port, "child-two");
          assertEquals(first.getHeader().getClusterId(), fourth.getHeader().getClusterId());
          assertEquals(4, fourth.getHeader().getMemberId());
          assertEquals(5, fourth.getHeader().getRevision());
          assertEquals(ByteString.copyFromUtf8("child-one"), fourth.getPrevKv().getValue());
        } finally {
          stopRuntimeProcess(child);
        }
      }
    }
  }

  private static Rpc.PutResponse put(RuntimeServer server, String value) throws Exception {
    return put(server.boundListeners().getFirst().port(), value);
  }

  private static Rpc.PutResponse put(int port, String value) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    try {
      return KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS).put(
          Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("configured-runtime-key"))
              .setValue(ByteString.copyFromUtf8(value)).setPrevKv(true).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private Path writeRuntimeConfiguration(InitializationConfiguration initial, int port) throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "m3", "server.json")));
    var profile = initial.hbaseProfiles().getFirst();
    ((ObjectNode) tree.withArray("hbase_profiles").get(0))
        .put("namespace", profile.namespace()).put("client_port", profile.clientPort());
    ((ObjectNode) tree.withArray("listeners").get(0)).put("port", port);
    Path file = directory.resolve("server.json");
    Files.write(file, mapper.writeValueAsBytes(tree));
    return file;
  }

  private Path writeActivation(String memberId) throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "m3", "activation.json")));
    tree.put("member_id", memberId).put("owner_lease_ms", 3_000).put("heartbeat_ms", 300);
    Path file = directory.resolve("activation-" + memberId + ".json");
    Files.write(file, mapper.writeValueAsBytes(tree));
    return file;
  }

  private Process startRuntimeProcess(Path runtime, Path activation, String name) throws Exception {
    Path log = directory.resolve("runtime-" + name + ".log");
    var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", System.getProperty("java.class.path"), RuntimeServer.class.getName(),
        "--config", runtime.toString(), "--m3-config", activation.toString())
        .redirectErrorStream(true).redirectOutput(log.toFile());
    Process process = builder.start();
    boolean ready = false;
    try {
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      while (System.nanoTime() - until < 0) {
        if (!process.isAlive()) throw new AssertionError("runtime process exited before readiness; see " + log);
        if (Files.exists(log) && Files.readString(log).contains("data_readiness=READY")) {
          ready = true;
          return process;
        }
        Thread.sleep(100);
      }
      throw new AssertionError("runtime process did not become ready; see " + log);
    } finally {
      if (!ready) stopRuntimeProcess(process);
    }
  }

  private static void stopRuntimeProcess(Process process) throws Exception {
    if (process.isAlive()) process.destroyForcibly();
    assertTrue(process.waitFor(10, TimeUnit.SECONDS));
  }

  private static M3RuntimeConfiguration activation(String memberId) {
    return new M3RuntimeConfiguration(memberId, 3_000, 300, 60_000, 30_000,
        512L << 20, 1_572_864, 2, 16, 60_000);
  }

  private static InitializationConfiguration initialization(int port) throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "j05", "native-cell-versions.json")));
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("namespace", "m3_server_" + UUID.randomUUID().toString().replace("-", ""))
        .put("quorum", "127.0.0.1").put("client_port", port);
    ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
    ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
    return new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }
}
