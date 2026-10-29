package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;
import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Uses actual disposable HBase servers; no target credentials, grants or data are involved. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class InitializeTablesIT {
  private static final byte[] ROW = new byte[] {0, 0, 0x11};
  private static final byte[] VALUE = new byte[] {0, (byte) 0xff, 0x33};
  private static final byte[] QUALIFIER = Bytes.toBytes("j05-test");
  private final Set<String> ownedNamespaces = new LinkedHashSet<>();
  private TemporaryHBaseClusterProcess localCluster;
  private ProviderSession localOperator;
  @TempDir private Path files;

  @BeforeAll
  void startTheDisposableLocalServerAndAnExplicitOperatorConnection() throws Exception {
    localCluster = TemporaryHBaseClusterProcess.start();
    localOperator =
        simpleSession(
            localCluster.clientConfiguration(),
            new AuthenticationScope("fixture-cleanup", "local-hbase"));
    System.out.printf(
        "j05-hbase server=%s client.jvm=%s%n",
        await(localOperator.connection().getAdmin().getClusterMetrics(), OPERATION_TIMEOUT)
            .getHBaseVersion(),
        Runtime.version());
  }

  @AfterEach
  void removeOnlyTheNamespacesOwnedByThisTest() throws Exception {
    for (String namespace : ownedNamespaces) {
      cleanupNamespace(localOperator.connection(), namespace);
    }
    ownedNamespaces.clear();
  }

  @AfterAll
  void closeTheOperatorBeforeStoppingTheServer() throws Exception {
    try {
      if (localOperator != null) {
        localOperator.close();
      }
    } finally {
      if (localCluster != null) {
        localCluster.close();
      }
    }
  }

  @Test
  void actualCliCreatesBothLayoutsAndRepeatsWithoutChangingPersistedData() throws Exception {
    for (String name : List.of("history-table.json", "native-cell-versions.json")) {
      String namespace = ownedNamespace("layout");
      ObjectNode tree = localConfiguration(name, namespace, localCluster.clientConfiguration());
      Path configuration = files.resolve(namespace + ".json");
      Files.write(
          configuration, new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(tree));
      var plan = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
      assertEquals(0, invokeCli("init", "plan", "--config", configuration.toString()).exitCode());
      CommandResult created = applyCli(configuration, plan);
      assertEquals(0, created.exitCode(), created.error());
      assertTrue(created.output().contains("status=created"));
      assertProvisioned(localOperator.connection(), plan);
      putProbe(localOperator.connection(), plan);
      CommandResult repeated = applyCli(configuration, plan);
      assertEquals(0, repeated.exitCode(), repeated.error());
      assertTrue(repeated.output().contains("status=already-provisioned"));
      assertProbe(localOperator.connection(), plan);
      System.out.printf(
          "j05-real layout=%s tables=%d regions_per_table=16 repeated=true persisted=true"
              + " status=passed%n",
          plan.historyLayout(), plan.tables().size());
    }
  }

  @Test
  void realPartialCreationResumesAndConfiguredSiblingsKeepTheirOwnDataAndDescriptors()
      throws Exception {
    String namespace = ownedNamespace("recovery");
    ObjectNode tree = InitializationFixtures.withSibling(namespace);
    configureBackend(tree, localCluster.clientConfiguration());
    var configuration = InitializationFixtures.load(tree);
    var plan = TableGroupPlanFactory.create(configuration, "local", null);
    var sibling = TableGroupPlanFactory.create(configuration, "sibling", null);
    try (var session =
        simpleSession(
            localCluster.clientConfiguration(), new AuthenticationScope("local", "local-hbase"))) {
      var admin = new AsyncTableGroupAdmin(session.connection());
      var failure =
          assertThrows(
              TableGroupInitializer.PartialInitializationException.class,
              () -> TableGroupInitializer.initialize(plan, new FailSecondCreate(admin)));
      assertEquals(1, failure.createdTables().size());
      assertEquals(1, admin.listTableNames(namespace).size());
      assertEquals(2, TableGroupInitializer.initialize(plan, admin).createdTables().size());
      assertProvisioned(session.connection(), plan);
      putProbe(session.connection(), plan);
      initializeSiblingAndCheckParent(plan, sibling, session.connection());
    }
    System.out.println(
        "j05-real recovery=partial-resume siblings-preserved=true" + " status=passed");
  }

  @Test
  void deletedRecordsDoNotMakeAPartialGroupPristineAgain() throws Exception {
    String namespace = ownedNamespace("deleted");
    ObjectNode tree =
        localConfiguration("history-table.json", namespace, localCluster.clientConfiguration());
    ((ObjectNode) tree.at("/table_groups/0/system_table/column_families/0"))
        .put("keep_deleted_cells", "TRUE");
    var plan = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    try (var session =
        simpleSession(
            localCluster.clientConfiguration(), new AuthenticationScope("local", "local-hbase"))) {
      var admin = new AsyncTableGroupAdmin(session.connection());
      assertThrows(
          TableGroupInitializer.PartialInitializationException.class,
          () -> TableGroupInitializer.initialize(plan, new FailSecondCreate(admin)));
      putProbe(session.connection(), plan);
      assertEquals(
          TableGroupInitializer.ConflictReason.PARTIAL_DATA,
          assertThrows(
                  TableGroupInitializer.ConflictException.class,
                  () -> TableGroupInitializer.initialize(plan, admin))
              .reason());
      var table = session.connection().getTable(systemTable(plan));
      await(table.delete(new Delete(ROW)), OPERATION_TIMEOUT);
      assertTrue(await(table.get(new Get(ROW)), OPERATION_TIMEOUT).isEmpty());
      assertFalse(
          await(
                  table.scanAll(new Scan().setRaw(true).readAllVersions().setLimit(1)),
                  OPERATION_TIMEOUT)
              .isEmpty(),
          "the test requires a real retained version or delete marker");
      assertEquals(
          TableGroupInitializer.ConflictReason.PARTIAL_DATA,
          assertThrows(
                  TableGroupInitializer.ConflictException.class,
                  () -> TableGroupInitializer.initialize(plan, admin))
              .reason());
      assertEquals(1, admin.listTableNames(namespace).size());
    }
    System.out.println("j05-real recovery=deleted-records-refused raw-scan=true status=passed");
  }

  @Test
  void suppliedKerberosIdentityProvisionsSchemasAndAnUnprivilegedIdentityIsRejected()
      throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      Configuration backend = cluster.clientConfiguration();
      String namespace = namespace("kerberos");
      ObjectNode tree = localConfiguration("native-cell-versions.json", namespace, backend);
      configureSecureProvider(tree);
      var plan = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
      String realm = backend.get("etcdhbase.test.security.realm");
      try (var operator =
          trustedSession(
              backend,
              plan,
              () ->
                  KerberosUserContext.fromKeytab(
                      "admin@" + realm,
                      Path.of(backend.get("etcdhbase.test.security.admin.keytab"))))) {
        try {
          var result =
              TableGroupInitializer.initialize(
                  plan, "sha256:" + plan.fingerprint(), operator.identity(), operator.provider());
          assertEquals(2, result.createdTables().size());
          assertProvisioned(operator.connection(), plan);
          assertSame(
              operator.connection(),
              await(
                  operator.provider().connectionFor(operator.identity()).toCompletableFuture(),
                  OPERATION_TIMEOUT));
          assertUnprivilegedActivationRejected(backend, tree, realm, operator.connection());
        } finally {
          cleanupNamespace(operator.connection(), namespace);
        }
      }
    }
    System.out.println(
        "j05-real authentication=kerberos-keytab acl-enforced=true caller-owns-connection=true"
            + " status=passed");
  }

  private void initializeSiblingAndCheckParent(
      TableGroupInitializationPlan parent,
      TableGroupInitializationPlan sibling,
      AsyncConnection parentConnection)
      throws Exception {
    String fingerprint =
        await(parentConnection.getAdmin().getDescriptor(systemTable(parent)), OPERATION_TIMEOUT)
            .getValue("etcdhbase.m0.plan-fingerprint");
    try (var session =
        simpleSession(
            localCluster.clientConfiguration(),
            new AuthenticationScope("sibling", "local-hbase"))) {
      var created =
          TableGroupInitializer.initialize(
              sibling, "sha256:" + sibling.fingerprint(), session.identity(), session.provider());
      assertEquals(3, created.createdTables().size());
      assertProvisioned(session.connection(), sibling);
    }
    assertProbe(parentConnection, parent);
    assertEquals(
        fingerprint,
        await(parentConnection.getAdmin().getDescriptor(systemTable(parent)), OPERATION_TIMEOUT)
            .getValue("etcdhbase.m0.plan-fingerprint"));
    assertEquals(
        6,
        await(
                parentConnection.getAdmin().listTableNamesByNamespace(parent.namespace()),
                OPERATION_TIMEOUT)
            .size());
  }

  private static void assertUnprivilegedActivationRejected(
      Configuration backend, ObjectNode original, String realm, AsyncConnection operator)
      throws Exception {
    ObjectNode tree = original.deepCopy();
    String deniedNamespace = namespace("denied");
    ((ObjectNode) tree.withArray("hbase_profiles").get(0)).put("namespace", deniedNamespace);
    var denied = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    var failure =
        assertThrows(
            BackendIdentityException.class,
            () ->
                trustedSession(
                    backend,
                    denied,
                    () ->
                        KerberosUserContext.fromKeytab(
                            "keytab-user@" + realm,
                            Path.of(backend.get("etcdhbase.test.security.user.keytab")))));
    assertEquals(BackendIdentityException.Reason.INSUFFICIENT_PERMISSION, failure.reason());
    assertFalse(
        await(operator.getAdmin().listNamespaces(), OPERATION_TIMEOUT).contains(deniedNamespace));
  }

  private static ProviderSession simpleSession(Configuration backend, AuthenticationScope scope)
      throws Exception {
    var provider =
        new HBaseBackendIdentityProvider(
            DEVELOPMENT_NO_AUTH_PROVIDER_ID,
            scope,
            backend,
            HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) ->
                connection.getAdmin().listNamespaces().thenApply(namespaces -> null),
            1,
            1,
            OPERATION_TIMEOUT);
    return openSession(
        provider,
        () -> {
          try (var credentials = new BackendCredentials("j05-test-operator", new char[0])) {
            return provider.authenticate(credentials, scope);
          }
        });
  }

  private static ProviderSession trustedSession(
      Configuration backend,
      TableGroupInitializationPlan plan,
      Callable<? extends HBaseUserContext> source)
      throws Exception {
    Permission permission =
        Permission.newBuilder(plan.namespace())
            .withActions(Action.CREATE, Action.ADMIN, Action.READ, Action.WRITE)
            .build();
    HBaseAccessVerifier verifier =
        (connection, scope) ->
            connection
                .getAdmin()
                .hasUserPermissions(List.of(permission))
                .thenCompose(
                    allowed ->
                        List.of(true).equals(allowed)
                            ? CompletableFuture.completedFuture(null)
                            : CompletableFuture.failedFuture(
                                new BackendIdentityException(
                                    BackendIdentityException.Reason.INSUFFICIENT_PERMISSION)));
    var scope = new AuthenticationScope(plan.tableGroupId(), plan.hbaseProfile().id());
    var provider =
        new HBaseBackendIdentityProvider(
            plan.identityProviderId(),
            scope,
            backend,
            HBaseAuthenticators.kerberosPassword(),
            verifier,
            1,
            1,
            OPERATION_TIMEOUT);
    return openSession(provider, () -> provider.activateTrusted(source));
  }

  private static ProviderSession openSession(
      HBaseBackendIdentityProvider provider,
      Supplier<CompletionStage<AuthorizedBackendIdentity>> activation)
      throws Exception {
    try {
      var identity = await(activation.get().toCompletableFuture(), OPERATION_TIMEOUT);
      var connection =
          await(provider.connectionFor(identity).toCompletableFuture(), OPERATION_TIMEOUT);
      return new ProviderSession(provider, identity, connection);
    } catch (Exception | Error failure) {
      try {
        provider.close();
      } catch (RuntimeException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static void assertProvisioned(
      AsyncConnection connection, TableGroupInitializationPlan plan) throws Exception {
    String clusterId =
        await(connection.getAdmin().getClusterMetrics(), OPERATION_TIMEOUT).getClusterId();
    assertEquals(
        plan.tables().size(),
        plan.tables().stream().filter(table -> table.splitKeysHex().size() == 15).count());
    for (TableSchemaCandidate table : plan.tables()) {
      TableName name = TableName.valueOf(plan.namespace(), table.tableName());
      TableDescriptor actual = await(connection.getAdmin().getDescriptor(name), OPERATION_TIMEOUT);
      assertEquals(plan.fingerprint(), actual.getValue("etcdhbase.m0.plan-fingerprint"));
      assertEquals(clusterId, actual.getValue("etcdhbase.m0.backend-cluster-id"));
      assertEquals(Durability.SYNC_WAL, actual.getDurability());
      assertEquals(table.columnFamilies().size(), actual.getColumnFamilyCount());
      assertFamilies(table, actual);
      List<String> starts =
          await(connection.getAdmin().getRegions(name), OPERATION_TIMEOUT).stream()
              .filter(region -> region.getReplicaId() == 0)
              .map(RegionInfo::getStartKey)
              .sorted(Arrays::compareUnsigned)
              .map(HexFormat.of()::formatHex)
              .toList();
      assertEquals(Stream.concat(Stream.of(""), table.splitKeysHex().stream()).toList(), starts);
    }
  }

  private static void assertFamilies(TableSchemaCandidate table, TableDescriptor actual) {
    for (var expected : table.columnFamilies()) {
      var family = actual.getColumnFamily(Bytes.toBytes(expected.name()));
      assertEquals(expected.minVersions(), family.getMinVersions());
      assertEquals(expected.maxVersions(), family.getMaxVersions());
      assertEquals(
          expected.ttlSeconds() == -1 ? HConstants.FOREVER : expected.ttlSeconds(),
          family.getTimeToLive());
      assertEquals(expected.keepDeletedCells().name(), family.getKeepDeletedCells().name());
    }
  }

  private static void putProbe(AsyncConnection connection, TableGroupInitializationPlan plan)
      throws Exception {
    await(
        connection
            .getTable(systemTable(plan))
            .put(
                new Put(ROW)
                    .addColumn(Bytes.toBytes("s"), QUALIFIER, VALUE)
                    .setDurability(Durability.SYNC_WAL)),
        OPERATION_TIMEOUT);
  }

  private static void assertProbe(AsyncConnection connection, TableGroupInitializationPlan plan)
      throws Exception {
    var observed =
        await(connection.getTable(systemTable(plan)).get(new Get(ROW)), OPERATION_TIMEOUT);
    assertArrayEquals(VALUE, observed.getValue(Bytes.toBytes("s"), QUALIFIER));
  }

  private static TableName systemTable(TableGroupInitializationPlan plan) {
    return TableName.valueOf(plan.namespace(), plan.tables().getFirst().tableName());
  }

  private static void cleanupNamespace(AsyncConnection connection, String namespace)
      throws Exception {
    if (!namespace.startsWith("m0_j05_")) {
      throw new AssertionError("cleanup namespace is not owned by this test");
    }
    var admin = connection.getAdmin();
    if (!await(admin.listNamespaces(), OPERATION_TIMEOUT).contains(namespace)) {
      return;
    }
    for (TableName name : await(admin.listTableNamesByNamespace(namespace), OPERATION_TIMEOUT)) {
      if (await(admin.isTableEnabled(name), OPERATION_TIMEOUT)) {
        await(admin.disableTable(name), OPERATION_TIMEOUT);
      }
      await(admin.deleteTable(name), OPERATION_TIMEOUT);
    }
    await(admin.deleteNamespace(namespace), OPERATION_TIMEOUT);
  }

  private String ownedNamespace(String label) {
    String namespace = namespace(label);
    ownedNamespaces.add(namespace);
    return namespace;
  }

  private static String namespace(String label) {
    return "m0_j05_" + label + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static ObjectNode localConfiguration(String name, String namespace, Configuration backend)
      throws Exception {
    ObjectNode tree = InitializationFixtures.example(name, namespace);
    configureBackend(tree, backend);
    return tree;
  }

  private static void configureBackend(ObjectNode tree, Configuration backend) {
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("quorum", backend.get("hbase.zookeeper.quorum"));
    profile.put("client_port", backend.getInt("hbase.zookeeper.property.clientPort", -1));
  }

  private static void configureSecureProvider(ObjectNode tree) {
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("authentication_mode", "CREDENTIAL_REFERENCE");
    profile.put("credential_reference", "secret://test/operator-keytab");
    ((ObjectNode) tree.withArray("table_groups").get(0))
        .put("identity_provider_id", "operator-keytab");
  }

  private static CommandResult applyCli(Path config, TableGroupInitializationPlan plan) {
    return invokeCli(
        "init",
        "apply",
        "--config",
        config.toString(),
        "--confirm-table-group",
        plan.tableGroupId(),
        "--confirm-plan",
        "sha256:" + plan.fingerprint());
  }

  private static CommandResult invokeCli(String... arguments) {
    var output = new ByteArrayOutputStream();
    var error = new ByteArrayOutputStream();
    int exitCode =
        InitializeTablesCommand.run(arguments, new PrintStream(output), new PrintStream(error));
    return new CommandResult(
        exitCode, output.toString(StandardCharsets.UTF_8), error.toString(StandardCharsets.UTF_8));
  }

  private record CommandResult(int exitCode, String output, String error) {}

  private record ProviderSession(
      HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity,
      AsyncConnection connection)
      implements AutoCloseable {
    @Override
    public void close() {
      provider.close();
    }
  }

  private static final class FailSecondCreate implements TableGroupAdmin {
    private final TableGroupAdmin delegate;
    private int creations;

    private FailSecondCreate(TableGroupAdmin delegate) {
      this.delegate = delegate;
    }

    @Override
    public String clusterId() throws Exception {
      return delegate.clusterId();
    }

    @Override
    public List<String> listNamespaces() throws Exception {
      return delegate.listNamespaces();
    }

    @Override
    public void createNamespace(String namespace) throws Exception {
      delegate.createNamespace(namespace);
    }

    @Override
    public List<TableName> listTableNames(String namespace) throws Exception {
      return delegate.listTableNames(namespace);
    }

    @Override
    public TableDescriptor getDescriptor(TableName table) throws Exception {
      return delegate.getDescriptor(table);
    }

    @Override
    public List<RegionInfo> getRegions(TableName table) throws Exception {
      return delegate.getRegions(table);
    }

    @Override
    public boolean isEmpty(TableName table) throws Exception {
      return delegate.isEmpty(table);
    }

    @Override
    public void createTable(TableDescriptor descriptor, byte[][] splits) throws Exception {
      if (++creations == 2) {
        throw new IOException("injected before the second real create RPC");
      }
      delegate.createTable(descriptor, splits);
    }
  }
}
