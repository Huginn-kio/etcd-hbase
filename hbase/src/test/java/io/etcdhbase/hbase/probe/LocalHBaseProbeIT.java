package io.etcdhbase.hbase.probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalHBaseProbeIT {
  private static final long TIMEOUT_SECONDS = 60;
  private static final String PROFILE_ID = "local-hbase";
  private static final String TABLE_GROUP_ID = "local";

  private final List<HBaseBackendIdentityProvider> providers = new ArrayList<>();
  private TemporaryHBaseClusterProcess cluster;
  private Configuration clientConfiguration;
  private String namespace;
  private TableName tableName;

  @BeforeEach
  void startUnauthenticatedTemporaryCluster() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start();
    clientConfiguration = cluster.clientConfiguration();
    assertEquals("simple", clientConfiguration.get("hbase.security.authentication"));
    assertFalse(clientConfiguration.getBoolean("hbase.security.authorization", true));

    String runId = UUID.randomUUID().toString().replace("-", "");
    namespace = "etcd_hbase_m0_" + runId;
    tableName = TableName.valueOf(namespace, "probe");
  }

  @AfterEach
  void closeConnectionsAndTemporaryCluster() throws Exception {
    Exception cleanupFailure = null;
    if (cluster != null && namespace != null) {
      try {
        AsyncConnection connection = openConnection();
        AsyncAdmin admin = connection.getAdmin();
        if (await(admin.tableExists(tableName))) {
          if (!await(admin.isTableDisabled(tableName))) {
            await(admin.disableTable(tableName));
          }
          await(admin.deleteTable(tableName));
        }
        if (await(admin.listNamespaces()).contains(namespace)) {
          await(admin.deleteNamespace(namespace));
        }
        System.out.printf(
            "hbase-probe cleanup namespace=%s table=%s status=complete%n", namespace, tableName);
      } catch (Exception failure) {
        cleanupFailure = failure;
        System.err.printf(
            "probe-resource-cleanup-failed namespace=%s table=%s%n", namespace, tableName);
      }
    }

    List<AutoCloseable> resources = new ArrayList<>(providers);
    if (cluster != null) {
      resources.add(cluster);
    }
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception failure) {
        if (cleanupFailure == null) {
          cleanupFailure = failure;
        } else {
          cleanupFailure.addSuppressed(failure);
        }
      }
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  @Test
  void verifiesRawBytesCasScanIndependentReadAndConnectionRecreation() throws Exception {
    String runId = namespace.substring("etcd_hbase_m0_".length());
    HBaseBackendIdentityProvider serviceProvider = newProvider(2);
    AuthorizedBackendIdentity alice = authenticate(serviceProvider, "alice");
    AsyncConnection serviceConnection =
        await(serviceProvider.connectionFor(alice).toCompletableFuture());
    assertSame(
        serviceConnection, await(serviceProvider.connectionFor(alice).toCompletableFuture()));
    AsyncAdmin admin = serviceConnection.getAdmin();
    await(admin.createNamespace(NamespaceDescriptor.create(namespace).build()));
    await(
        admin.createTable(
            TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("d"))
                .setDurability(Durability.SYNC_WAL)
                .build()));
    assertEquals(Durability.SYNC_WAL, await(admin.getDescriptor(tableName)).getDurability());
    System.out.printf(
        "hbase-probe resource namespace=%s table=%s family=d auth=development-no-auth%n",
        namespace, tableName);

    AuthorizedBackendIdentity bob = authenticate(serviceProvider, "bob");
    assertNotEquals(alice.contextRef(), bob.contextRef());
    AsyncConnection bobConnection = await(serviceProvider.connectionFor(bob).toCompletableFuture());
    assertNotSame(serviceConnection, bobConnection);
    BackendProbe.ProbeResult result = BackendProbe.execute(serviceConnection, tableName, runId);
    BackendProbe.verifyPersistedValue(bobConnection, tableName, runId);
    verifyRequestScopedScannerCleanup(serviceProvider, alice, serviceConnection);

    assertTrue(result.rawBytesRoundTrip());
    assertTrue(result.compareAndMutateSucceeded());
    assertTrue(result.staleCompareAndMutateRejected());
    assertEquals(2, result.scannedRows());
    // A second run in the same table must neither scan nor mutate the first run's rows.
    assertEquals(2, BackendProbe.execute(serviceConnection, tableName, runId + "b").scannedRows());

    HBaseBackendIdentityProvider independentProvider = newProvider(1);
    AsyncConnection independentConnection = openConnection(independentProvider);
    BackendProbe.verifyPersistedValue(independentConnection, tableName, runId);
    independentProvider.close();
    assertTrue(independentConnection.isClosed());

    serviceProvider.close();
    assertTrue(serviceConnection.isClosed());
    assertTrue(bobConnection.isClosed());
    HBaseBackendIdentityProvider restartedProvider = newProvider(1);
    AsyncConnection restartedConnection = openConnection(restartedProvider);
    BackendProbe.verifyPersistedValue(restartedConnection, tableName, runId);
    BackendProbe.cleanup(restartedConnection, tableName, runId + "b");
    // Removing one run's exact rows preserves the first run's value.
    BackendProbe.verifyPersistedValue(restartedConnection, tableName, runId);
    restartedProvider.close();
    assertTrue(restartedConnection.isClosed());

    System.out.printf(
        "hbase-probe profile=public-local authentication=simple authorization=false"
            + " hbase.version=%s hbase.jvm=%s client.jvm=%s operations=put,get,cas,scan,delete"
            + " independentRead=true reconnectRead=true runIsolation=true tableDurability=SYNC_WAL"
            + " mutationDurability=SYNC_WAL connectionOwner=unified-provider closed=true%n",
        cluster.hbaseVersion(), cluster.jvmVersion(), Runtime.version());
  }

  private void verifyRequestScopedScannerCleanup(
      HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity,
      AsyncConnection connection)
      throws Exception {
    var scope = io.etcdhbase.core.request.RequestScope.withTimeout(Duration.ofSeconds(10));
    var request = new io.etcdhbase.hbase.connection.HBaseRequestContext(scope);
    assertSame(connection, await(request.borrowConnection(provider, identity)));
    var table = connection.getTable(tableName);
    var scanner =
        org.mockito.Mockito.spy(
            table.getScanner(new org.apache.hadoop.hbase.client.Scan().setCaching(1).setLimit(2)));
    try {
      request.ownScanner(scanner);
      var row = scanner.next();
      assertFalse(row.isEmpty());
      assertFalse(
          await(
                  request.execute(
                      () -> table.get(new org.apache.hadoop.hbase.client.Get(row.getRow()))))
              .isEmpty());
      scope.cancel();
      scope.close();
      org.mockito.Mockito.verify(scanner, org.mockito.Mockito.times(1)).close();
      assertFalse(connection.isClosed());
      assertFalse(await(table.get(new org.apache.hadoop.hbase.client.Get(row.getRow()))).isEmpty());
      System.out.println(
          "j06-hbase realScannerClosed=true sharedConnectionPreserved=true readAfterCancel=true");
    } finally {
      scope.close();
    }
  }

  private AsyncConnection openConnection() throws Exception {
    return openConnection(newProvider(1));
  }

  private AsyncConnection openConnection(HBaseBackendIdentityProvider provider) throws Exception {
    AuthorizedBackendIdentity identity = authenticate(provider, "m0-local-probe");
    return await(provider.connectionFor(identity).toCompletableFuture());
  }

  private AuthorizedBackendIdentity authenticate(
      HBaseBackendIdentityProvider provider, String principal) throws Exception {
    try (var credentials = new BackendCredentials(principal, new char[0])) {
      return await(
          provider
              .authenticate(credentials, new AuthenticationScope(TABLE_GROUP_ID, PROFILE_ID))
              .toCompletableFuture());
    }
  }

  private HBaseBackendIdentityProvider newProvider(int maximumContexts) {
    // SIMPLE has no ownership proof or ACL enforcement. This actual, non-mutating RPC verifies
    // development reachability before namespace/table creation, never target authorization.
    var provider =
        new HBaseBackendIdentityProvider(
            "local-unified",
            new AuthenticationScope(TABLE_GROUP_ID, PROFILE_ID),
            clientConfiguration,
            HBaseAuthenticators.simpleDevelopment(),
            (connection, scope) ->
                connection.getAdmin().listNamespaces().thenApply(ignored -> null),
            maximumContexts,
            maximumContexts,
            Duration.ofSeconds(30));
    providers.add(provider);
    return provider;
  }

  private static <T> T await(java.util.concurrent.CompletableFuture<T> future) throws Exception {
    return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
