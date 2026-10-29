package io.etcdhbase.admin;

import static io.etcdhbase.admin.InitializationFixtures.plan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class TableGroupInitializerTest {
  @Test
  void resumesMatchingPartialCreationAndThenBecomesANoOp() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_partial");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;

    TableGroupInitializer.PartialInitializationException failure =
        assertThrows(
            TableGroupInitializer.PartialInitializationException.class,
            () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(plan.namespace(), failure.namespace());
    assertEquals(1, failure.createdTables().size());
    assertEquals(1, admin.descriptors.size());
    assertTrue(failure.getMessage().contains("rerun the same confirmed plan"));

    admin.failOnCreateNumber = 0;
    TableGroupInitializer.InitializationResult resumed =
        TableGroupInitializer.initialize(plan, admin);
    assertEquals(plan.tables().size() - 1, resumed.createdTables().size());
    assertFalse(resumed.alreadyProvisioned());
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);
    assertTrue(repeated.alreadyProvisioned());
    assertTrue(repeated.createdTables().isEmpty());
    assertEquals(plan.tables().size(), admin.descriptors.size());
  }

  @Test
  void refusesConflictingDescriptorsAndUnrelatedTablesWithoutModifyingThem() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_conflict");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName firstTable = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.descriptors.put(
        firstTable,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(firstTable))
            .setValue("etcdhbase.m0.schema-version", "999")
            .build());

    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableName unrelated = TableName.valueOf(plan.namespace(), "business-data");
    admin.descriptors.put(unrelated, TableDescriptorBuilder.newBuilder(unrelated).build());
    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertTrue(admin.descriptors.containsKey(unrelated));
  }

  @Test
  void acceptsBackendAddedDescriptorDefaultsWhenPlannedSchemaStillMatches() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_backend_default");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName firstTable = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.descriptors.put(
        firstTable,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(firstTable))
            .setValue("hbase.store.file-tracker.impl", "DEFAULT")
            .build());

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);

    assertTrue(repeated.alreadyProvisioned());
    assertTrue(repeated.createdTables().isEmpty());
  }

  @Test
  void refusesToExtendAPartialGroupThatAlreadyContainsRows() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_nonempty");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;
    assertThrows(
        TableGroupInitializer.PartialInitializationException.class,
        () -> TableGroupInitializer.initialize(plan, admin));
    TableName existing = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.nonEmptyTables.add(existing);
    int existingTableCount = admin.descriptors.size();

    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(existingTableCount, admin.descriptors.size());
  }

  @Test
  void fullGroupRetryIgnoresOperationalRegionSplitsAndPreservesData() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_split_full");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName tableName = TableName.valueOf(plan.namespace(), "system");
    int createdTableCount = admin.createNumber;
    admin.splitRegion(tableName, HexFormat.of().parseHex("000080"));
    admin.nonEmptyTables.add(tableName);

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);

    assertTrue(repeated.alreadyProvisioned());
    assertTrue(repeated.createdTables().isEmpty());
    assertEquals(createdTableCount, admin.createNumber);
    assertTrue(admin.nonEmptyTables.contains(tableName));
  }

  @Test
  void partialGroupWithChangedRegionBoundariesCannotResume() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.json", "m0_init_split_partial");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;
    assertThrows(
        TableGroupInitializer.PartialInitializationException.class,
        () -> TableGroupInitializer.initialize(plan, admin));
    TableName existing = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.splitRegion(existing, HexFormat.of().parseHex("000080"));
    int existingTableCount = admin.descriptors.size();
    int createCount = admin.createNumber;

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));

    assertTrue(failure.getMessage().contains("different region split boundaries"));
    assertEquals(existingTableCount, admin.descriptors.size());
    assertEquals(createCount, admin.createNumber);
  }

  @Test
  void mapsBothHistoryCandidatesIntoDistinctExplicitTableGroups() throws Exception {
    TableGroupInitializationPlan separate = plan("history-table.json", "m0_init_separate");
    TableGroupInitializationPlan nativeVersions =
        plan("native-cell-versions.json", "m0_init_native");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();

    TableGroupInitializer.initialize(separate, admin);
    TableGroupInitializer.initialize(nativeVersions, admin);

    assertEquals(5, admin.descriptors.size());
    assertEquals(
        64,
        admin
            .descriptors
            .get(TableName.valueOf("m0_init_native:kv"))
            .getColumnFamily(Bytes.toBytes("d"))
            .getMaxVersions());
  }

  @Test
  void initializesConfiguredSiblingGroupsInTheSameNamespaceWithoutModifyingThem() throws Exception {
    InitializationConfiguration configuration = configurationWithSiblingGroup("shared_namespace");
    TableGroupInitializationPlan local = TableGroupPlanFactory.create(configuration, "local", null);
    TableGroupInitializationPlan sibling =
        TableGroupPlanFactory.create(configuration, "sibling", null);
    TableGroupInitializationPlan singleGroup = plan("history-table.json", "shared_namespace");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    Set<String> expectedConfiguredTables =
        Stream.concat(
                local.tables().stream().map(TableSchemaCandidate::tableName),
                sibling.tables().stream().map(TableSchemaCandidate::tableName))
            .collect(Collectors.toUnmodifiableSet());

    assertEquals(expectedConfiguredTables, Set.copyOf(local.configuredNamespaceTableNames()));
    assertEquals(expectedConfiguredTables, Set.copyOf(sibling.configuredNamespaceTableNames()));
    assertNotEquals(singleGroup.fingerprint(), local.fingerprint());

    TableGroupInitializer.InitializationResult localResult =
        TableGroupInitializer.initialize(local, admin);
    Map<TableName, TableDescriptor> localDescriptors = Map.copyOf(admin.descriptors);
    TableGroupInitializer.InitializationResult siblingResult =
        TableGroupInitializer.initialize(sibling, admin);

    assertFalse(localResult.alreadyProvisioned());
    assertFalse(siblingResult.alreadyProvisioned());
    assertEquals(local.tables().size(), localDescriptors.size());
    assertEquals(local.tables().size() + sibling.tables().size(), admin.descriptors.size());
    for (Map.Entry<TableName, TableDescriptor> entry : localDescriptors.entrySet()) {
      assertSame(entry.getValue(), admin.descriptors.get(entry.getKey()));
      assertEquals("local", entry.getValue().getValue("etcdhbase.m0.table-group-id"));
    }
    for (TableSchemaCandidate table : sibling.tables()) {
      assertEquals(
          "sibling",
          admin
              .descriptors
              .get(TableName.valueOf(sibling.namespace(), table.tableName()))
              .getValue("etcdhbase.m0.table-group-id"));
    }
    assertTrue(TableGroupInitializer.initialize(local, admin).alreadyProvisioned());
    assertTrue(TableGroupInitializer.initialize(sibling, admin).alreadyProvisioned());
  }

  @Test
  void resumesWhenTheServerCommittedATableButTheCallerObservedAFailure() throws Exception {
    var plan = plan("history-table.json", "m0_unknown_commit");
    var admin = new InMemoryTableGroupAdmin();
    admin.failAfterCreateNumber = 1;
    var failure =
        assertThrows(
            TableGroupInitializer.PartialInitializationException.class,
            () -> TableGroupInitializer.initialize(plan, admin));
    assertTrue(failure.createdTables().isEmpty());
    assertEquals(1, admin.descriptors.size());
    admin.failAfterCreateNumber = 0;
    assertEquals(2, TableGroupInitializer.initialize(plan, admin).createdTables().size());
    assertTrue(TableGroupInitializer.initialize(plan, admin).alreadyProvisioned());
  }

  @Test
  void rechecksAConcurrentMatchingCreationInsteadOfOverwritingIt() throws Exception {
    var plan = plan("history-table.json", "m0_race");
    var admin = new InMemoryTableGroupAdmin();
    admin.concurrentNamespaceCreation = true;
    admin.concurrentOnCreateNumber = 1;
    var result = TableGroupInitializer.initialize(plan, admin);
    assertFalse(result.namespaceCreated());
    assertEquals(2, result.createdTables().size());
    assertEquals(3, admin.descriptors.size());
  }

  @Test
  void refusesAConcurrentForeignTableAndDoesNotCreateTheRemainingTables() throws Exception {
    var plan = plan("history-table.json", "m0_foreign_race");
    var admin = new InMemoryTableGroupAdmin();
    admin.concurrentOnCreateNumber = 1;
    admin.foreignConcurrentCreation = true;
    var failure =
        assertThrows(
            TableGroupInitializer.PartialInitializationException.class,
            () -> TableGroupInitializer.initialize(plan, admin));
    assertTrue(failure.getCause() instanceof IllegalArgumentException);
    assertEquals(1, admin.createNumber);
    assertEquals(1, admin.descriptors.size());
  }

  @Test
  void refusesForeignOwnershipAndAnotherClusterEvenWhenColumnFamiliesMatch() throws Exception {
    var plan = plan("history-table.json", "m0_ownership");
    var admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    admin.clusterId = "other-cluster";
    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    admin.clusterId = "fixture-cluster";
    var table = TableName.valueOf(plan.namespace(), "system");
    admin.descriptors.put(
        table,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(table))
            .setValue("etcdhbase.m0.plan-fingerprint", "foreign")
            .build());
    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(3, admin.createNumber);
  }

  @Test
  void checksConfirmationAndIdentityScopeBeforeBorrowingAnyConnection() throws Exception {
    var plan = plan("history-table.json", "m0_confirmation");
    var identity =
        new AuthorizedBackendIdentity(
            plan.identityProviderId(),
            "operator",
            "opaque",
            new AuthenticationScope("local", "local-hbase"),
            Set.of("local"),
            0);
    HBaseConnectionFactory forbidden =
        new HBaseConnectionFactory() {
          @Override
          public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity ignored) {
            throw new AssertionError("must reject before borrowing a connection");
          }

          @Override
          public void close() {
            throw new AssertionError("the caller owns the factory");
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableGroupInitializer.initialize(
                plan, "sha256:" + "0".repeat(64), identity, forbidden));
    var otherGroup =
        new AuthorizedBackendIdentity(
            plan.identityProviderId(),
            "operator",
            "opaque",
            new AuthenticationScope("sibling", "local-hbase"),
            Set.of("sibling"),
            0);
    assertEquals(
        BackendIdentityException.Reason.INSUFFICIENT_PERMISSION,
        assertThrows(
                BackendIdentityException.class,
                () ->
                    TableGroupInitializer.initialize(
                        plan, "sha256:" + plan.fingerprint(), otherGroup, forbidden))
            .reason());
  }

  private static InitializationConfiguration configurationWithSiblingGroup(String namespace)
      throws Exception {
    return InitializationFixtures.load(InitializationFixtures.withSibling(namespace));
  }

  private static final class InMemoryTableGroupAdmin implements TableGroupAdmin {
    private final Set<String> namespaces = new HashSet<>();
    private final Map<TableName, TableDescriptor> descriptors = new HashMap<>();
    private final Map<TableName, List<RegionInfo>> regions = new HashMap<>();
    private final Set<TableName> nonEmptyTables = new HashSet<>();
    private int createNumber;
    private String clusterId = "fixture-cluster";
    private int failOnCreateNumber;
    private int failAfterCreateNumber;
    private int concurrentOnCreateNumber;
    private boolean concurrentNamespaceCreation;
    private boolean foreignConcurrentCreation;

    @Override
    public String clusterId() {
      return clusterId;
    }

    @Override
    public List<String> listNamespaces() {
      return List.copyOf(namespaces);
    }

    @Override
    public void createNamespace(String namespace) throws Exception {
      namespaces.add(namespace);
      if (concurrentNamespaceCreation) {
        throw new NamespaceExistException(namespace);
      }
    }

    @Override
    public List<TableName> listTableNames(String namespace) {
      return descriptors.keySet().stream()
          .filter(tableName -> tableName.getNamespaceAsString().equals(namespace))
          .sorted()
          .toList();
    }

    @Override
    public TableDescriptor getDescriptor(TableName tableName) {
      return descriptors.get(tableName);
    }

    @Override
    public List<RegionInfo> getRegions(TableName tableName) {
      return regions.get(tableName);
    }

    @Override
    public boolean isEmpty(TableName tableName) {
      return !nonEmptyTables.contains(tableName);
    }

    @Override
    public void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception {
      createNumber++;
      if (createNumber == failOnCreateNumber) {
        throw new Exception("simulated create failure");
      }
      TableName tableName = descriptor.getTableName();
      if (descriptors.containsKey(tableName)) {
        throw new org.apache.hadoop.hbase.TableExistsException(tableName);
      }
      descriptors.put(tableName, descriptor);
      namespaces.add(tableName.getNamespaceAsString());
      List<byte[]> starts = new ArrayList<>();
      starts.add(new byte[0]);
      starts.addAll(Arrays.asList(splitKeys));
      starts.sort(Bytes.BYTES_COMPARATOR);
      List<RegionInfo> tableRegions = new ArrayList<>();
      for (int index = 0; index < starts.size(); index++) {
        byte[] endKey = index + 1 == starts.size() ? new byte[0] : starts.get(index + 1);
        tableRegions.add(
            RegionInfoBuilder.newBuilder(tableName)
                .setStartKey(starts.get(index))
                .setEndKey(endKey)
                .build());
      }
      regions.put(tableName, List.copyOf(tableRegions));
      if (createNumber == failAfterCreateNumber) {
        throw new IOException("fixture response was lost after create committed");
      }
      if (createNumber == concurrentOnCreateNumber) {
        if (foreignConcurrentCreation) {
          descriptors.put(
              tableName,
              TableDescriptorBuilder.newBuilder(descriptor)
                  .setValue("etcdhbase.m0.table-group-id", "foreign")
                  .build());
        }
        throw new org.apache.hadoop.hbase.TableExistsException(tableName);
      }
    }

    private void splitRegion(TableName tableName, byte[] splitPoint) {
      List<RegionInfo> tableRegions = new ArrayList<>(regions.get(tableName));
      for (int index = 0; index < tableRegions.size(); index++) {
        RegionInfo parent = tableRegions.get(index);
        byte[] startKey = parent.getStartKey();
        byte[] endKey = parent.getEndKey();
        if (Bytes.compareTo(startKey, splitPoint) < 0
            && (endKey.length == 0 || Bytes.compareTo(splitPoint, endKey) < 0)) {
          tableRegions.remove(index);
          tableRegions.add(
              RegionInfoBuilder.newBuilder(tableName)
                  .setStartKey(startKey)
                  .setEndKey(splitPoint)
                  .build());
          tableRegions.add(
              RegionInfoBuilder.newBuilder(tableName)
                  .setStartKey(splitPoint)
                  .setEndKey(endKey)
                  .build());
          tableRegions.sort(
              (left, right) -> Bytes.compareTo(left.getStartKey(), right.getStartKey()));
          regions.put(tableName, List.copyOf(tableRegions));
          return;
        }
      }
      throw new IllegalArgumentException("split point is not inside an existing region");
    }
  }
}
