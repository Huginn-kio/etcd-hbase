package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;
import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/** Exercises real server routing; it does not measure production capacity or latency. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class SaltDistributionIT {
  private static final int REGION_COUNT = 16;
  private static final int SERVER_COUNT = 3;
  private static final int BATCH_SIZE = 128;
  private static final byte[] FAMILY = Bytes.toBytes("d");
  private static final byte[] QUALIFIER = Bytes.toBytes("salt-probe");
  private static final byte[] VALUE = Bytes.toBytes("j05-salt-record");
  private final HashSaltStrategy salt = new HashSaltStrategy();
  private TemporaryHBaseClusterProcess cluster;
  private HBaseBackendIdentityProvider provider;
  private AsyncConnection connection;
  private List<ServerName> servers;
  private TableName saltedTable;
  private TableName unsaltedTable;
  private Map<String, ServerName> saltedPlacement;
  private Map<String, ServerName> unsaltedPlacement;

  @BeforeAll
  void startThreeServersAndAuthenticateAnExplicitDevelopmentIdentity() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start(SERVER_COUNT);
    var plan = initializationPlan(cluster.clientConfiguration());
    var scope = new AuthenticationScope(plan.tableGroupId(), plan.hbaseProfile().id());
    provider =
        new HBaseBackendIdentityProvider(
            DEVELOPMENT_NO_AUTH_PROVIDER_ID,
            scope,
            cluster.clientConfiguration(),
            HBaseAuthenticators.simpleDevelopment(),
            (backend, ignored) -> backend.getAdmin().listNamespaces().thenApply(names -> null),
            1,
            1,
            OPERATION_TIMEOUT);
    try (var credentials = new BackendCredentials("j05-salt-operator", new char[0])) {
      var identity =
          await(provider.authenticate(credentials, scope).toCompletableFuture(), OPERATION_TIMEOUT);
      connection = await(provider.connectionFor(identity).toCompletableFuture(), OPERATION_TIMEOUT);
      servers =
          await(connection.getAdmin().getRegionServers(), OPERATION_TIMEOUT).stream()
              .sorted()
              .toList();
      assertEquals(SERVER_COUNT, servers.size());
      await(connection.getAdmin().balancerSwitch(false, true), OPERATION_TIMEOUT);
      provisionTables(plan, identity);
    }
    saltedPlacement = placeRegions(saltedTable);
    unsaltedPlacement = placeRegions(unsaltedTable);
  }

  private static TableGroupInitializationPlan initializationPlan(Configuration backend)
      throws Exception {
    var tree =
        InitializationFixtures.example(
            "native-cell-versions.json",
            "m0_j05_salt_" + UUID.randomUUID().toString().replace("-", ""));
    var profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("quorum", backend.get("hbase.zookeeper.quorum"));
    profile.put("client_port", backend.getInt("hbase.zookeeper.property.clientPort", 0));
    return TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
  }

  private void provisionTables(
      TableGroupInitializationPlan plan, AuthorizedBackendIdentity identity) throws Exception {
    TableGroupInitializer.initialize(plan, "sha256:" + plan.fingerprint(), identity, provider);
    saltedTable = TableName.valueOf(plan.namespace(), "kv");
    unsaltedTable = TableName.valueOf(plan.namespace(), "unsalted_control");
    var schema = await(connection.getAdmin().getDescriptor(saltedTable), OPERATION_TIMEOUT);
    var control =
        TableDescriptorBuilder.newBuilder(unsaltedTable)
            .setColumnFamilies(Arrays.asList(schema.getColumnFamilies()))
            .setDurability(schema.getDurability())
            .build();
    // The same boundaries and server placement isolate the effect of adding the salt prefix.
    await(
        connection.getAdmin().createTable(control, salt.splitKeys().toArray(byte[][]::new)),
        OPERATION_TIMEOUT);
  }

  private Map<String, ServerName> placeRegions(TableName table) throws Exception {
    var regions =
        await(connection.getAdmin().getRegions(table), OPERATION_TIMEOUT).stream()
            .sorted(Comparator.comparing(RegionInfo::getStartKey, Arrays::compareUnsigned))
            .toList();
    assertEquals(REGION_COUNT, regions.size());
    Map<String, ServerName> expected = new HashMap<>();
    List<CompletableFuture<Void>> moves = new ArrayList<>();
    for (int regionIndex = 0; regionIndex < regions.size(); regionIndex++) {
      var region = regions.get(regionIndex);
      var server = servers.get(regionIndex % servers.size());
      expected.put(region.getEncodedName(), server);
      moves.add(connection.getAdmin().move(region.getEncodedNameAsBytes(), server));
    }
    await(CompletableFuture.allOf(moves.toArray(CompletableFuture[]::new)), OPERATION_TIMEOUT);
    long deadline = System.nanoTime() + OPERATION_TIMEOUT.toNanos();
    Map<String, ServerName> observed;
    do {
      observed = new HashMap<>();
      for (var server : servers) {
        for (var region : await(connection.getAdmin().getRegions(server), OPERATION_TIMEOUT)) {
          if (region.getTable().equals(table)) {
            observed.put(region.getEncodedName(), server);
          }
        }
      }
      if (expected.equals(observed)) {
        return Map.copyOf(expected);
      }
      // move acceptance can precede the Region becoming online; wait on that actual state.
      TimeUnit.MILLISECONDS.sleep(50);
    } while (System.nanoTime() < deadline);
    assertEquals(expected, observed, "region placement did not become ready before the deadline");
    return Map.copyOf(expected);
  }

  private Map<String, RegionWrites> readWrites(TableName table) throws Exception {
    Map<String, RegionWrites> counts = new HashMap<>();
    for (var server : servers) {
      for (var metrics :
          await(connection.getAdmin().getRegionMetrics(server, table), OPERATION_TIMEOUT)) {
        var previous =
            counts.put(
                RegionInfo.encodeRegionName(metrics.getRegionName()),
                new RegionWrites(server, metrics.getWriteRequestCount()));
        assertNull(previous, "a region must not appear online on two servers");
      }
    }
    assertEquals(REGION_COUNT, counts.size());
    return Map.copyOf(counts);
  }

  @AfterAll
  void closeTheOwnedIdentityBeforeStoppingItsDisposableServers() throws Exception {
    try {
      if (provider != null) {
        provider.close();
      }
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  @Test
  void writesAndReadsOneBatchAcrossRealRegionsAndServers() throws Exception {
    var identifiers =
        IntStream.range(0, BATCH_SIZE).mapToObj(index -> Bytes.toBytes((long) index)).toList();
    var saltedRows = identifiers.stream().map(this::saltedRow).toList();
    var beforeSalt = readWrites(saltedTable);
    var beforeControl = readWrites(unsaltedTable);
    writeRows(saltedTable, saltedRows);
    writeRows(unsaltedTable, identifiers);
    assertDistribution(
        distribution(beforeSalt, readWrites(saltedTable), saltedPlacement),
        BATCH_SIZE,
        REGION_COUNT,
        SERVER_COUNT);
    assertDistribution(
        distribution(beforeControl, readWrites(unsaltedTable), unsaltedPlacement),
        BATCH_SIZE,
        1,
        1);
    assertReadBack(saltedTable, saltedRows);
    assertReadBack(unsaltedTable, identifiers);
  }

  private byte[] saltedRow(byte[] identifier) {
    return Bytes.add(salt.salt(identifier), identifier);
  }

  private void writeRows(TableName table, List<byte[]> rows) throws Exception {
    var mutations =
        rows.stream().map(row -> new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).toList();
    await(
        CompletableFuture.allOf(
            connection.getTable(table).put(mutations).toArray(CompletableFuture[]::new)),
        OPERATION_TIMEOUT);
  }

  private void assertReadBack(TableName table, List<byte[]> rows) throws Exception {
    var observed =
        await(
            connection.getTable(table).getAll(rows.stream().map(Get::new).toList()),
            OPERATION_TIMEOUT);
    assertEquals(rows.size(), observed.size());
    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      assertArrayEquals(rows.get(rowIndex), observed.get(rowIndex).getRow());
      assertArrayEquals(VALUE, observed.get(rowIndex).getValue(FAMILY, QUALIFIER));
    }
  }

  private Distribution distribution(
      Map<String, RegionWrites> before,
      Map<String, RegionWrites> after,
      Map<String, ServerName> placement) {
    assertEquals(placement.keySet(), before.keySet());
    assertEquals(before.keySet(), after.keySet());
    List<Long> byRegion = new ArrayList<>();
    Map<ServerName, Long> byServer = new HashMap<>();
    for (var entry : placement.entrySet()) {
      var previous = before.get(entry.getKey());
      var current = after.get(entry.getKey());
      assertEquals(entry.getValue(), previous.server());
      assertEquals(entry.getValue(), current.server(), "region placement changed during sampling");
      long delta = current.count() - previous.count();
      assertTrue(delta >= 0, "a region counter must not reset during sampling");
      byRegion.add(delta);
      byServer.merge(current.server(), delta, Long::sum);
    }
    return new Distribution(
        byRegion.stream().sorted().toList(),
        servers.stream().map(server -> byServer.getOrDefault(server, 0L)).toList());
  }

  private static void assertDistribution(
      Distribution observed, long writes, long activeRegions, long activeServers) {
    assertEquals(writes, observed.regions().stream().mapToLong(Long::longValue).sum());
    assertEquals(writes, observed.servers().stream().mapToLong(Long::longValue).sum());
    assertEquals(activeRegions, observed.regions().stream().filter(count -> count > 0).count());
    assertEquals(activeServers, observed.servers().stream().filter(count -> count > 0).count());
  }

  private record Distribution(List<Long> regions, List<Long> servers) {}

  private record RegionWrites(ServerName server, long count) {}
}
