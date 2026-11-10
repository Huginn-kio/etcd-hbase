package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Exercises a real Go client, Java gRPC server and temporary HBase for both layouts. */
@Timeout(600)
class M3HistoryRpcIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);

  @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path artifacts;

  @Test
  void pointAndNestedTxnHistoryUsesRealHBaseOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        runLayout(cluster, layout);
      }
    }
  }

  private void runLayout(TemporaryHBaseClusterProcess cluster, String layout) throws Exception {
    var plan = M3TableGroupRuntimeIT.plan(layout,
        cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
    var auth = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), auth,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-history-fixture", new char[0])) {
      var identity = provider.authenticate(credentials, auth).toCompletableFuture().get(30, TimeUnit.SECONDS);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
      var limits = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
      var budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
      try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
        M3TableGroupRuntime runtime;
        try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
          runtime = M3TableGroupRuntime.open(plan, M3TableGroupRuntimeIT.activation("1"),
              RESOURCES, provider, identity, budgets, scans, startup);
        }
        try {
          try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
              "m3-history", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
            runGoClient(server.boundListener().port(), layout);
          }
        } finally {
          runtime.close();
          runtime.drained().get(30, TimeUnit.SECONDS);
        }
      }
      assertEquals(0, budgets.snapshot().reservedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
  }

  private void runGoClient(int port, String layout) throws Exception {
    Path root = WireProcesses.repositoryRoot();
    Path output = artifacts.resolve(layout + ".json");
    Path log = artifacts.resolve(layout + ".log");
    String prefix = "etcd-hbase-m3-test/history-" + UUID.randomUUID() + "/";
    Process process = new ProcessBuilder(root.resolve("scripts/dev/go-program.sh").toString(), "m3history",
        "--endpoint", "127.0.0.1:" + port, "--prefix", prefix, "--output", output.toString())
        .directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (!process.waitFor(180, TimeUnit.SECONDS)) throw new IllegalStateException("history driver timed out");
      assertEquals(0, process.exitValue(), "history driver failed; inspect the local test log");
      String evidence = Files.readString(output);
      assertTrue(evidence.contains("\"schema\":\"etcd-hbase-j28-point-txn-history/v1\""));
      assertTrue(evidence.contains("\"Method\":\"Txn\""));
      assertTrue(evidence.contains("\"Verdict\":\"linearizable\""));
      M3HistoryEvidence.save(output, "rpc-" + layout, Map.of("layout", layout, "fault", "none"));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }
}
