package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Runs the pinned Go Watch client against the real M3 runtime and both HBase layouts. */
@Timeout(900)
class M4WatchRpcIT {
  private static final String WATCH_ENDPOINT_ENV = "ETCD_HBASE_M4_WATCH_ENDPOINT";
  private static final String WATCH_PREFIX_ENV = "ETCD_HBASE_M4_WATCH_PREFIX";
  private static final String GO_WATCH_TEST =
      "TestM4WatchJavaServiceFragmentReplayAutoResumeAndLiveHandoff";
  // Seed values are written in bounded batches. A small atomic update then produces a large
  // PrevKV Watch revision, allowing transport limits to exercise first, middle, and final fragments.
  private static final int WATCH_RPC_BUDGET_BYTES = 16 * 1024;
  private static final int MAX_REQUEST_BYTES = WATCH_RPC_BUDGET_BYTES;
  private static final int MAX_DIAGNOSTIC_BYTES = 16 * 1024;
  private static final Duration GO_TEST_TIMEOUT = Duration.ofMinutes(4);
  private static final List<String> LAYOUT_FILES =
      List.of("history-table.json", "native-cell-versions.json");

  @TempDir Path testDirectory;

  @Test
  void pinnedGoClientVerifiesFragmentAutoResumeAndLiveHandoffOnBothLayouts() throws Exception {
    Path root = WireProcesses.repositoryRoot();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration()
          .getInt("hbase.zookeeper.property.clientPort", -1);
      for (String layoutFile : LAYOUT_FILES) {
        runLayout(root, cluster, layoutFile, clientPort);
      }
    }
  }

  private void runLayout(Path root, TemporaryHBaseClusterProcess cluster, String layoutFile,
      int clientPort) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    AuthenticationScope authScope = new AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    String layoutName = layoutFile.equals("history-table.json") ? "history" : "native";
    try (var provider = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces()
                .thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m4-watch-interop", new char[0])) {
      var identity = provider.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration activation = activation(root);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + source.fingerprint(),
          identity, provider, activation.pinConfiguration());
      RuntimeConfiguration configuration = runtimeConfiguration(
          initialization, source.tableGroupId());

      try (RuntimeServer server = RuntimeServer.startM3(configuration, activation)) {
        assertTrue(server.dataReady(), "M4 Watch runtime must recover before Go requests are sent");
        int port = server.boundListeners().getFirst().port();
        String endpoint = "127.0.0.1:" + port;
        String prefix = "etcd-hbase-m4-test/" + UUID.randomUUID().toString().replace("-", "")
            + "/";
        Path log = testDirectory.resolve(layoutName + "-go-watch.log");
        runGoWatchTest(root, endpoint, prefix, log, layoutName);
      }
    }
  }

  private static InitializationConfiguration initialization(String layoutFile, int clientPort)
      throws IOException {
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/j05").resolve(layoutFile);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("namespace", "m4watch_" + UUID.randomUUID().toString().replace("-", ""))
        .put("quorum", "127.0.0.1")
        .put("client_port", clientPort);
    ObjectNode systemTable = (ObjectNode) tree.withArray("table_groups").get(0).get("system_table");
    boolean hasResultCategory = false;
    for (var category : systemTable.withArray("record_categories")) {
      if (category.asText().equals("RESULT")) {
        hasResultCategory = true;
        break;
      }
    }
    if (!hasResultCategory) systemTable.withArray("record_categories").add("RESULT");
    return new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static RuntimeConfiguration runtimeConfiguration(
      InitializationConfiguration initialization, String tableGroupId) {
    var resources = new RuntimeConfiguration.ResourceBudgets(
        65_536, 4, 30_000, WATCH_RPC_BUDGET_BYTES, MAX_REQUEST_BYTES, 8, 0);
    return new RuntimeConfiguration(initialization.schemaVersion(),
        List.of(new RuntimeConfiguration.Listener("m4-watch", "127.0.0.1", 0, tableGroupId)),
        initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(),
        resources);
  }

  private static M3RuntimeConfiguration activation(Path root) throws IOException {
    Path path = root.resolve("config/examples/m4/activation.json");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    tree.put("owner_lease_ms", 60_000)
        .put("heartbeat_ms", 5_000)
        .put("max_etcd_request_bytes", MAX_REQUEST_BYTES);
    return new M3RuntimeConfigurationLoader().load(
        new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static void runGoWatchTest(Path root, String endpoint, String prefix, Path log,
      String layoutName) throws Exception {
    List<String> command = List.of(root.resolve("scripts/dev/go-m0.sh").toString(),
        "test", "-v", "-count=1", "-run", "^" + GO_WATCH_TEST + "$", "./internal/scenario");
    ProcessBuilder builder = new ProcessBuilder(command).directory(root.toFile());
    builder.environment().put(WATCH_ENDPOINT_ENV, endpoint);
    builder.environment().put(WATCH_PREFIX_ENV, prefix);
    Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (!process.waitFor(GO_TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        terminate(process);
        throw new AssertionError("Go Watch test timed out for " + layoutName + " layout; output:\n"
            + readDiagnosticTail(log));
      }
      assertEquals(0, process.exitValue(), () -> "Go Watch test failed for " + layoutName
          + " layout; output:\n" + readDiagnosticTail(log));
      String output = readDiagnosticTail(log);
      assertTrue(output.contains("--- PASS: " + GO_WATCH_TEST),
          () -> "Go test did not execute the required integration case for " + layoutName
              + " layout; output:\n" + output);
      assertTrue(!output.contains("--- SKIP: " + GO_WATCH_TEST),
          () -> "Go test skipped the required integration case for " + layoutName
              + " layout; output:\n" + output);
    } finally {
      terminate(process);
    }
  }

  private static void terminate(Process process) throws InterruptedException {
    if (!process.isAlive()) return;
    process.descendants().forEach(ProcessHandle::destroyForcibly);
    process.destroyForcibly();
    assertTrue(process.waitFor(5, TimeUnit.SECONDS),
        "Go test process did not stop after termination");
  }

  private static String readDiagnosticTail(Path path) {
    if (!Files.isRegularFile(path)) return "Go test output is unavailable";
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      long size = channel.size();
      long start = Math.max(0, size - MAX_DIAGNOSTIC_BYTES);
      channel.position(start);
      ByteBuffer diagnostic = ByteBuffer.allocate((int) (size - start));
      while (diagnostic.hasRemaining()) {
        if (channel.read(diagnostic) < 0) break;
      }
      diagnostic.flip();
      String prefix = start == 0 ? "" : "[earlier output omitted]\n";
      return prefix + StandardCharsets.UTF_8.decode(diagnostic);
    } catch (IOException failure) {
      return "Go test output could not be read: " + failure.getClass().getSimpleName();
    }
  }
}
