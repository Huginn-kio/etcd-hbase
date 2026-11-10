package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseMaterializationStore;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises fresh M3 initialization only against a disposable development HBase cluster. */
class M3InitializeTablesIT {
  @TempDir Path directory;
  private static final Duration TIMEOUT = Duration.ofMinutes(2);
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));

  @Test
  void initializesBothLayoutsAndNeverAdoptsExistingTables() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        String namespace = "m3_init_" + UUID.randomUUID().toString().replace("-", "");
        ObjectNode tree = InitializationFixtures.example(file, namespace);
        ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
        ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
        ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
        profile.put("quorum", "127.0.0.1");
        profile.put("client_port", cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
        var source = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
        var plan = M3TableGroupPlan.from(source);
        var authScope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
        try (var provider = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
                cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
                (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
                1, 1, Duration.ofSeconds(30));
             var credentials = new BackendCredentials("m3-initializer", new char[0]);
             var scope = RequestScope.withTimeout(TIMEOUT)) {
          var identity = await(provider.authenticate(credentials, authScope).toCompletableFuture());
          String confirmation = "sha256:" + source.fingerprint();
          var initialized = M3TableGroupInitializer.initializeFresh(
              plan, confirmation, identity, provider, PINS);
          assertNotEquals(0, initialized.clusterId());
          assertEquals(source.fingerprint(), initialized.planFingerprint());
          assertEquals(source.tables().size(), initialized.tables().size());

          var records = await(HBaseSystemRecordStore.open(provider, identity, plan.system(), scope));
          assertEquals(initialized.clusterId(), await(new TableGroupIdentityService(records).readExisting(scope)));
          assertNotNull(await(SnapshotPinManager.open(records, PINS, scope)));
          var control = await(HBaseCommitControlStore.open(provider, identity, plan.system(), scope));
          var state = await(control.read(scope)).orElseThrow().state();
          assertEquals(1, state.publishedRevision());
          assertEquals(0, state.publishedSequence());
          assertTrue(state.ownerInstance().isEmpty());
          assertNotNull(await(HBaseMaterializationStore.openForTableGroup(provider, identity,
              plan.system(), plan.primary(), plan.current(), scope)));

          assertThrows(IllegalArgumentException.class, () -> M3TableGroupInitializer.initializeFresh(
              plan, confirmation, identity, provider, PINS));
          assertEquals(initialized.clusterId(), await(new TableGroupIdentityService(records).readExisting(scope)));
          assertEquals(0, await(control.read(scope)).orElseThrow().state().publishedSequence());
        }
      }
    }
  }

  @Test
  void commandInitializesFreshLocalTablesAndRejectsRepeat() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      String namespace = "m3_cli_" + UUID.randomUUID().toString().replace("-", "");
      ObjectNode tree = InitializationFixtures.example("native-cell-versions.json", namespace);
      ((ObjectNode) tree.withArray("table_groups").get(0).get("system_table"))
          .withArray("record_categories").add("RESULT");
      ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
      profile.put("quorum", "127.0.0.1");
      profile.put("client_port", cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
      Path config = directory.resolve("init.json");
      Files.write(config, new ObjectMapper().writeValueAsBytes(tree));
      var output = new ByteArrayOutputStream();
      var error = new ByteArrayOutputStream();
      assertEquals(0, M3InitializeTablesCommand.run(new String[] {"init", "plan", "--config", config.toString(),
          "--table-group", "local", "--pin-registry", "2:16:60000"},
          new PrintStream(output), new PrintStream(error)), error.toString());
      String fingerprint = output.toString().lines()
          .filter(line -> line.startsWith("confirmation_fingerprint: "))
          .findFirst().orElseThrow().substring("confirmation_fingerprint: ".length());
      String[] apply = {"init", "apply", "--config", config.toString(), "--confirm-table-group", "local",
          "--confirm-plan", fingerprint, "--pin-registry", "2:16:60000"};
      output.reset();
      error.reset();
      assertEquals(0, M3InitializeTablesCommand.run(apply, new PrintStream(output), new PrintStream(error)),
          error.toString());
      assertTrue(output.toString().contains("cluster_id="));
      assertNotEquals(0, M3InitializeTablesCommand.run(apply, new PrintStream(output), new PrintStream(error)));
    }
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }
}
