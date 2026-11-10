package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Unknown RPC results across two real server processes sharing a live HBase. */
@Timeout(600)
class M3HistoryFailoverIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path artifacts;

  @Test
  void goHistorySurvivesOwnerProcessDeathOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        for (String mode : List.of("before-dispatch", "after-commit")) {
          runLayout(cluster, layout, mode);
        }
      }
    }
  }

  @Test
  void goHistoryRecoversAfterSelectedControlCasAckIsLost() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        runLayout(cluster, layout, "cas-ack-lost");
      }
    }
  }

  @Test
  void goHistoryRecoversAfterOneOfTwoRowsIsMaterialized() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        runLayout(cluster, layout, "partial-materialization");
      }
    }
  }

  @Test
  void resumedOldOwnerCannotOverwriteNewOwnerOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        runLayout(cluster, layout, "old-owner-resume");
      }
    }
  }

  @Test
  void unpublishedPreparationNeverBecomesVisibleAfterOwnerDeath() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        for (String mode : List.of("candidate-preparation", "result-preparation", "sealed-before-publish")) {
          runLayout(cluster, layout, mode);
        }
      }
    }
  }

  @Test
  void canceledClientKeepsResourcesUntilTheSelectedWriteDrains() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        runLayout(cluster, layout, "cancel-after-cas");
      }
    }
  }

  private void runLayout(TemporaryHBaseClusterProcess cluster, String layout, String mode) throws Exception {
    Path run = Files.createDirectory(artifacts.resolve(layout + "-" + mode));
    int port = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
    String namespace = "m3_history_" + UUID.randomUUID().toString().replace("-", "");
    var plan = M3TableGroupRuntimeIT.plan(layout, port, namespace);
    var auth = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    Path configuration = run.resolve("hbase-site.xml");
    Files.writeString(run.resolve("hosts"), "127.0.0.1 localhost\n::1 localhost\n");
    try (var out = Files.newOutputStream(configuration)) { cluster.clientConfiguration().writeXml(out); }
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), auth,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-history-parent", new char[0])) {
      var identity = provider.authenticate(credentials, auth).toCompletableFuture().get(30, TimeUnit.SECONDS);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
      exerciseRestart(plan, provider, identity, run, configuration, layout, port, namespace, mode);
    }
  }

  private void exerciseRestart(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, Path run, Path configuration, String layout, int port, String namespace, String mode)
      throws Exception {
    Path firstReady = run.resolve("first-ready");
    Process first = startServer(configuration, layout, port, namespace, "1", mode, firstReady, run.resolve("first.log"));
    Process second = null;
    Process driver = null;
    try {
      awaitFile(firstReady, first);
      long originalEpoch = ownerEpoch(plan, provider, identity);
      Path output = run.resolve("history.json");
      Path root = WireProcesses.repositoryRoot();
      String prefix = crossBucketPrefix(plan);
      driver = new ProcessBuilder(root.resolve("scripts/dev/go-program.sh").toString(), "m3history",
          "--endpoint", Files.readString(firstReady), "--prefix", prefix, "--output", output.toString(),
          "--restart-dir", run.toString(), "--restart-mode", mode)
          .directory(root.toFile()).redirectErrorStream(true).redirectOutput(run.resolve("driver.log").toFile()).start();
      Budget beforeCancellation = null;
      Budget heldCancellation = null;
      if (mode.equals("cancel-after-cas")) {
        awaitFile(run.resolve("before-fault"), first, driver);
        Files.createFile(run.resolve("measure-baseline"));
        awaitFile(run.resolve("baseline-budget"), first, driver);
        beforeCancellation = budget(run.resolve("baseline-budget"));
        Files.createFile(run.resolve("fault-ready"));
      }
      awaitFile(run.resolve("rpc-held"), first, driver);
      assertTrue(first.pid() != ProcessHandle.current().pid());
      if (mode.equals("old-owner-resume")) {
        signalProcess(first, "-STOP");
      } else if (mode.equals("cancel-after-cas")) {
        Files.createFile(run.resolve("cancel-client"));
      } else {
        first.destroyForcibly();
        assertTrue(first.waitFor(10, TimeUnit.SECONDS));
      }
      awaitFile(run.resolve("before-restart"), driver);
      // This read proves that HBase and its authoritative control row survived the owner process.
      CommitState stopped = controlState(plan, provider, identity);
      assertEquals(originalEpoch, stopped.ownerEpoch());
      if (mode.equals("cas-ack-lost") || mode.equals("partial-materialization")
          || mode.equals("old-owner-resume") || mode.equals("cancel-after-cas")) {
        assertTrue(stopped.publication().isPresent());
        assertTrue(stopped.publishedSequence() > stopped.finalizedSequence());
      }
      if (Set.of("candidate-preparation", "result-preparation", "sealed-before-publish").contains(mode)) {
        assertTrue(stopped.publication().isEmpty());
        assertEquals(stopped.publishedSequence(), stopped.finalizedSequence());
      }
      if (mode.equals("partial-materialization")) {
        assertOnePhysicalRow(plan, provider, identity, prefix, stopped.publishedRevision());
      }
      long replacementEpoch = originalEpoch;
      Path nextEndpoint = firstReady;
      if (mode.equals("cancel-after-cas")) {
        Files.createFile(run.resolve("measure-held"));
        awaitFile(run.resolve("held-budget"), first, driver);
        heldCancellation = budget(run.resolve("held-budget"));
        assertTrue(heldCancellation.allocated() > beforeCancellation.allocated()
                || heldCancellation.reserved() > beforeCancellation.reserved(),
            "in-flight producer must remain charged after client cancellation");
        Files.createFile(run.resolve("release-old-ack"));
        awaitFile(run.resolve("old-ack-delivered"), first, driver);
        waitFinalized(plan, provider, identity, stopped.publishedSequence());
      } else {
        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, provider, identity);
        Path secondReady = run.resolve("second-ready");
        second = startServer(configuration, layout, port, namespace, "2", mode, secondReady, run.resolve("second.log"));
        awaitFile(secondReady, second);
        assertTrue(second.pid() != first.pid() && second.pid() != ProcessHandle.current().pid());
        replacementEpoch = ownerEpoch(plan, provider, identity);
        assertTrue(replacementEpoch > originalEpoch);
        nextEndpoint = secondReady;
      }
      Path next = run.resolve("next.tmp");
      Files.writeString(next, Files.readString(nextEndpoint));
      Files.move(next, run.resolve("after-restart"), StandardCopyOption.ATOMIC_MOVE);
      if (mode.equals("old-owner-resume")) {
        awaitFile(run.resolve("new-owner-written"), first, second, driver);
        signalProcess(first, "-CONT");
        Files.createFile(run.resolve("release-old-ack"));
        awaitFile(run.resolve("old-ack-delivered"), first, driver);
        Files.createFile(run.resolve("shutdown-old"));
        awaitFile(run.resolve("old-drained"), first, driver);
        assertEquals(replacementEpoch, ownerEpoch(plan, provider, identity));
      }
      assertTrue(driver.waitFor(90, TimeUnit.SECONDS), "history driver timed out");
      assertEquals(0, driver.exitValue(), "history driver failed; inspect the local fixture log");
      if (mode.equals("cancel-after-cas")) {
        Files.createFile(run.resolve("shutdown-old"));
        awaitFile(run.resolve("old-drained"), first);
      }
      String evidence = Files.readString(output);
      assertTrue(evidence.contains("\"restart_mode\":\"" + mode + "\""));
      assertTrue(evidence.contains("\"Verdict\":\"linearizable\""));
      var details = new LinkedHashMap<String, Object>();
      details.put("layout", layout);
      details.put("fault", mode);
      details.put("first_pid", first.pid());
      details.put("second_pid", second == null ? 0 : second.pid());
      details.put("first_epoch", originalEpoch);
      details.put("second_epoch", replacementEpoch);
      details.put("hbase_control_survived", true);
      details.put("first_bucket", plan.primary().codec().bucket(key(prefix + "lost-response")));
      details.put("second_bucket", plan.primary().codec().bucket(key(prefix + "lost-response-2")));
      details.put("partial_rows", mode.equals("partial-materialization") ? "one" : "not-checked");
      details.put("selected_before_recovery", stopped.publication().isPresent());
      if (mode.equals("cancel-after-cas")) {
        details.put("baseline_allocated", beforeCancellation.allocated());
        details.put("baseline_reserved", beforeCancellation.reserved());
        details.put("held_allocated", heldCancellation.allocated());
        details.put("held_reserved", heldCancellation.reserved());
        details.put("resources_drained", true);
      }
      if (mode.equals("old-owner-resume")) details.put("old_owner_resumed_and_drained", true);
      M3HistoryEvidence.save(output, "failover-" + layout + "-" + mode, details);
    } finally {
      stop(driver);
      stop(second);
      stop(first);
    }
  }

  private static Process startServer(Path configuration, String layout, int port, String namespace,
      String member, String mode, Path ready, Path log) throws Exception {
    return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djdk.net.hosts.file=" + ready.resolveSibling("hosts").toAbsolutePath(),
        "-cp", System.getProperty("java.class.path"), M3HistoryServerMain.class.getName(),
        configuration.toString(), layout, Integer.toString(port), namespace, member, ready.toString(), mode)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  private static long ownerEpoch(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity) throws Exception {
    return controlState(plan, provider, identity).ownerEpoch();
  }

  private static String crossBucketPrefix(M3TableGroupPlan plan) {
    for (int attempt = 0; attempt < 256; attempt++) {
      String prefix = "etcd-hbase-m3-test/restart-" + UUID.randomUUID() + "/";
      if (plan.primary().codec().bucket(key(prefix + "lost-response"))
          != plan.primary().codec().bucket(key(prefix + "lost-response-2"))) return prefix;
    }
    throw new IllegalStateException("could not create a cross-bucket history fixture");
  }

  private static void assertOnePhysicalRow(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, String prefix, long revision) throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var store = HBaseRecordStore.open(provider, identity, plan.primary(), scope, Runnable::run)
          .get(30, TimeUnit.SECONDS);
      int present = 0;
      for (String suffix : List.of("lost-response", "lost-response-2")) {
        if (store.get(key(prefix + suffix), revision, ReadProjection.FULL).get(30, TimeUnit.SECONDS).isPresent()) present++;
      }
      assertEquals(1, present, "exactly one transaction row must be materialized before recovery");
    }
  }

  private static ByteKey key(String text) {
    return ByteKey.copyOf(text.getBytes(StandardCharsets.UTF_8));
  }

  private static CommitState controlState(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity) throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      return control.read(scope).join().orElseThrow().state();
    }
  }

  private record Budget(long allocated, long reserved, long requests) {}

  private static Budget budget(Path file) throws Exception {
    String[] values = Files.readString(file).split(",", -1);
    if (values.length != 3) throw new IllegalStateException("invalid budget evidence");
    return new Budget(Long.parseLong(values[0]), Long.parseLong(values[1]), Long.parseLong(values[2]));
  }

  private static void waitFinalized(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, long sequence) throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      while (true) {
        var state = control.read(scope).get(30, TimeUnit.SECONDS).orElseThrow().state();
        if (state.finalizedSequence() == sequence && state.publication().isEmpty()) return;
        if (scope.isDeadlineExceeded()) throw new IllegalStateException("canceled transaction did not finalize");
        Thread.sleep(25);
      }
    }
  }

  private static void awaitFile(Path path, Process... processes) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (!Files.exists(path)) {
      for (Process process : processes) {
        if (!process.isAlive()) throw new IllegalStateException("fixture process exited before readiness: " + path.getFileName());
      }
      if (System.nanoTime() >= deadline) throw new IllegalStateException("fixture readiness timed out: " + path.getFileName());
      Thread.sleep(25);
    }
  }

  private static void stop(Process process) throws Exception {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
    }
  }

  private static void signalProcess(Process process, String signal) throws Exception {
    if (!process.isAlive() || process.pid() == ProcessHandle.current().pid()) {
      throw new IllegalStateException("invalid isolated owner process");
    }
    Process signaler = new ProcessBuilder("/bin/kill", signal, Long.toString(process.pid())).start();
    assertTrue(signaler.waitFor(10, TimeUnit.SECONDS), "owner signal timed out");
    assertEquals(0, signaler.exitValue(), "owner signal failed");
  }
}
