package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** An independent reader JVM keeps a cache while another process publishes into HBase. */
@Timeout(600)
class M3CacheProcessIT {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path temporaryDirectory;

  @Test
  void independentProcessesObserveWritesAndProveUnrelatedKeysOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      exerciseLayout(layout);
    }
  }

  private void exerciseLayout(StorageTableBinding.Layout layout) throws Exception {
    try (var fixture = M3TestEnvironment.start(layout); var writer = new M3PutCommitIT.Engine(fixture)) {
      writer.put("before".getBytes(StandardCharsets.UTF_8), false);
      Path directory = Files.createDirectory(temporaryDirectory.resolve(layout.name()));
      Path configuration = directory.resolve("hbase-site.xml");
      Path hosts = directory.resolve("hosts");
      Path log = directory.resolve("reader.log");
      fixture.writeClientConfiguration(configuration);
      Files.writeString(hosts, "127.0.0.1 localhost\n::1 localhost\n");
      try (var listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
        listener.setSoTimeout(60_000);
        runReader(fixture, writer, configuration, hosts, log, listener);
      }
    }
  }

  private static void runReader(M3TestEnvironment fixture, M3PutCommitIT.Engine writer,
      Path configuration, Path hosts, Path log, ServerSocket listener) throws Exception {
    Process reader = startReader(fixture, configuration, hosts, log, listener.getLocalPort());
    try (var socket = listener.accept();
         var input = new DataInputStream(socket.getInputStream());
         var output = new DataOutputStream(socket.getOutputStream())) {
      socket.setSoTimeout(90_000);
      assertTrue(reader.pid() > 0 && reader.pid() != ProcessHandle.current().pid());
      assertEquals("ready", input.readUTF());
      verifyIndependentReader(fixture, writer, input, output);
      output.writeUTF("stop");
      output.flush();
      assertTrue(reader.waitFor(60, TimeUnit.SECONDS), "cache reader did not stop; log: " + log);
      assertEquals(0, reader.exitValue(), "cache reader failed; log: " + log);
    } finally {
      if (reader.isAlive()) {
        reader.destroyForcibly();
        assertTrue(reader.waitFor(10, TimeUnit.SECONDS), "cache reader did not terminate");
      }
    }
  }

  private static void verifyIndependentReader(M3TestEnvironment fixture, M3PutCommitIT.Engine writer,
      DataInputStream input, DataOutputStream output) throws Exception {
    var missing = unrelatedKeyInSameBucket(fixture);
    assertEquals(new Observation(2, Optional.of("before"), 0), read(input, output, M3PutCommitIT.KEY));
    assertEquals(new Observation(2, Optional.of("before"), 1), read(input, output, M3PutCommitIT.KEY));
    assertEquals(new Observation(2, Optional.empty(), 1), read(input, output, missing));
    writer.put("after".getBytes(StandardCharsets.UTF_8), false);
    assertEquals(new Observation(3, Optional.empty(), 2), read(input, output, missing));
    assertEquals(new Observation(3, Optional.of("after"), 2), read(input, output, M3PutCommitIT.KEY));
    assertEquals(new Observation(3, Optional.of("after"), 3), read(input, output, M3PutCommitIT.KEY));
  }

  private static Process startReader(M3TestEnvironment fixture, Path configuration, Path hosts,
      Path log, int port) throws Exception {
    String current = fixture.current().map(value -> value.table().getNameAsString()).orElse("-");
    return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djdk.net.hosts.file=" + hosts.toAbsolutePath(), "-cp", System.getProperty("java.class.path"),
        M3CacheReaderProcess.class.getName(), configuration.toString(), fixture.system().tableGroupId(),
        fixture.system().table().getNameAsString(), fixture.saltProfileId(), fixture.primary().layout().name(),
        fixture.primary().table().getNameAsString(), current, Integer.toString(port))
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  private static Observation read(DataInputStream input, DataOutputStream output, ByteKey key) throws Exception {
    output.writeUTF("read");
    output.writeUTF(HexFormat.of().formatHex(key.bytes()));
    output.flush();
    long revision = input.readLong();
    Optional<String> value = input.readBoolean() ? Optional.of(input.readUTF()) : Optional.empty();
    return new Observation(revision, value, input.readLong());
  }

  private static ByteKey unrelatedKeyInSameBucket(M3TestEnvironment fixture) {
    int bucket = fixture.primary().codec().bucket(M3PutCommitIT.KEY);
    for (int index = 0; index < 10_000; index++) {
      var key = ByteKey.copyOf(("cache-process-" + index).getBytes(StandardCharsets.UTF_8));
      if (fixture.primary().codec().bucket(key) == bucket) return key;
    }
    throw new IllegalStateException("no same-bucket fixture key found");
  }

  private record Observation(long revision, Optional<String> value, long hits) {}
}
