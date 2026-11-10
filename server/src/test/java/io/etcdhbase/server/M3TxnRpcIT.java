package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Runs the pinned Go/native comparison against the assembled Java RPC and real HBase layouts. */
@Timeout(600)
class M3TxnRpcIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);

  @Test
  void goTransactionsMatchNativeOnBothLayouts() throws Exception {
    runScenario("j31", "m3-txn.yaml");
  }

  static void runScenario(String task, String scenario) throws Exception {
    Path root = WireProcesses.repositoryRoot();
    String configuredRun = System.getenv("M3_RUN_ID");
    String runId = configuredRun == null ? "txn-" + UUID.randomUUID() : configuredRun;
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
    Path nativeTrace = artifacts.resolve(task + "-native-trace.jsonl");
    runDriver(root, artifacts.resolve(task + "-native.log"), scenario, "--native-data-dir",
        artifacts.resolve(task + "-native-data").toString(), "--scope", "native-etcd", "--prefix", prefix,
        "--trace", nativeTrace.toString(), "--metadata", artifacts.resolve(task + "-native-metadata.json").toString());
    var run = new ScenarioRun(task, scenario, root, artifacts, prefix, nativeTrace);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        runTargetLayout(cluster, file, run);
      }
    }
    String source = System.getenv("M3_SOURCE_COMMIT");
    if (source != null) {
      if (!source.matches("[0-9a-f]{40}")) throw new IllegalArgumentException("invalid source commit");
      String metadata = "{\"kind\":\"java-grpc-hbase-minicluster\",\"run_id\":\"" + runId
          + "\",\"source_commit\":\"" + source
          + "\",\"authentication\":\"dev-no-auth\",\"layouts\":[\"HISTORY\",\"NATIVE\"]}\n";
      Files.writeString(artifacts.resolve(task + "-target-fixture.json"), metadata,
          java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE);
    }
  }

  private record ScenarioRun(String task, String scenario, Path root, Path artifacts,
      String prefix, Path nativeTrace) {}

  private static void runTargetLayout(TemporaryHBaseClusterProcess cluster, String file,
      ScenarioRun run) throws Exception {
    var plan = M3TableGroupRuntimeIT.plan(file, cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
    String layout = file.startsWith("history") ? "HISTORY" : "NATIVE";
    String suffix = layout.toLowerCase(Locale.ROOT);
    var auth = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), auth,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-txn-fixture", new char[0])) {
      var identity = provider.authenticate(credentials, auth).toCompletableFuture().get(30, TimeUnit.SECONDS);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
      var limits = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
      var budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
      runTargetRuntime(plan, provider, identity, budgets, run, layout, suffix);
      assertEquals(0, budgets.snapshot().reservedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
    run(run.root(), run.artifacts().resolve(run.task() + "-compare-" + suffix + ".log"), List.of(
        run.root().resolve("scripts/dev/go-program.sh").toString(), "native-etcd", "compare", "--m3-txn",
        "--left", run.nativeTrace().toString(), "--right",
        run.artifacts().resolve(run.task() + "-target-" + suffix + "-trace.jsonl").toString()));
  }

  private static void runTargetRuntime(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScenarioRun run,
      String layout, String suffix) throws Exception {
    try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
      M3TableGroupRuntime runtime;
      try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        runtime = M3TableGroupRuntime.open(plan, M3TableGroupRuntimeIT.activation("1"),
            RESOURCES, provider, identity, budgets, scans, startup);
      }
      try {
        runTargetServer(runtime, plan, provider, identity, run, layout, suffix);
      } finally {
        runtime.close();
        runtime.drained().get(30, TimeUnit.SECONDS);
      }
    }
  }

  private static void runTargetServer(M3TableGroupRuntime runtime, M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider, AuthorizedBackendIdentity identity, ScenarioRun run,
      String layout, String suffix) throws Exception {
    try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
        "m3-txn", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
      runDriver(run.root(), run.artifacts().resolve(run.task() + "-target-" + suffix + ".log"), run.scenario(),
          "--endpoint", "127.0.0.1:" + server.boundListener().port(), "--scope", "target-hbase",
          "--layout", layout, "--prefix", run.prefix(),
          "--trace", run.artifacts().resolve(run.task() + "-target-" + suffix + "-trace.jsonl").toString(),
          "--metadata", run.artifacts().resolve(run.task() + "-target-" + suffix + "-metadata.json").toString());
      if (run.task().equals("j31")) assertPhysicalRowLimits(server, provider, identity, plan);
    }
  }

  private static void assertPhysicalRowLimits(Bootstrap server, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, M3TableGroupPlan plan) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
        .usePlaintext().build();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS);
      int maximum = (org.apache.hadoop.hbase.HConstants.MAX_ROW_LENGTH
          - plan.system().saltProfile().saltWidthBytes() - 1 - 16 - 2) / 2;
      var accepted = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
          .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(new byte[maximum])))).build());
      var afterAccepted = control.read(scope).join().orElseThrow().state();
      assertEquals(accepted.getHeader().getRevision(), afterAccepted.publishedRevision());
      for (int length : new int[] {maximum + 1, 16_384}) {
        var request = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
            .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(new byte[length])))).build();
        var failure = assertThrows(StatusRuntimeException.class, () -> stub.txn(request));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, failure.getStatus().getCode());
        var afterFailure = control.read(scope).join().orElseThrow().state();
        assertEquals(afterAccepted.publishedSequence(), afterFailure.publishedSequence());
        assertEquals(afterAccepted.publishedRevision(), afterFailure.publishedRevision());
      }
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void runDriver(Path root, Path log, String scenario, String... args) throws Exception {

    var command = new ArrayList<String>();
    command.add(root.resolve("scripts/dev/go-program.sh").toString());
    command.add("m3driver");
    command.add("--scenario");
    command.add(root.resolve("testkit/scenarios").resolve(scenario).toString());
    command.addAll(List.of(args));
    run(root, log, command);
  }

  private static void run(Path root, Path log, List<String> command) throws Exception {
    Process process = new ProcessBuilder(command).directory(root.toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      if (!process.waitFor(180, TimeUnit.SECONDS)) throw new IllegalStateException("M3 Txn driver timed out");
      assertEquals(0, process.exitValue(), "M3 Txn driver or comparator failed; inspect its local run log");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }
}
