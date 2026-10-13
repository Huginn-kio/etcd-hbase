package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.HBaseProfile;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class TableGroupInitializerTest {
  @Test
  void resumesMatchingPartialCreationAndThenBecomesANoOp() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_partial");
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
    assertFalse(resumed.alreadyInitialized());
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);
    assertTrue(repeated.alreadyInitialized());
    assertTrue(repeated.createdTables().isEmpty());
    assertEquals(plan.tables().size(), admin.descriptors.size());
  }

  @Test
  void refusesConflictingDescriptorsAndUnrelatedTablesWithoutModifyingThem() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_conflict");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName firstTable = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.descriptors.put(
        firstTable,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(firstTable))
            .setValue("etcdhbase.m0.schema-version", "999")
            .build());

    assertThrows(IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableName unrelated = TableName.valueOf(plan.namespace(), "business-data");
    admin.descriptors.put(unrelated, TableDescriptorBuilder.newBuilder(unrelated).build());
    assertThrows(IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertTrue(admin.descriptors.containsKey(unrelated));
  }

  @Test
  void acceptsBackendAddedDescriptorDefaultsWhenPlannedSchemaStillMatches() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_backend_default");
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

    assertTrue(repeated.alreadyInitialized());
    assertTrue(repeated.createdTables().isEmpty());
  }

  @Test
  void refusesToExtendAPartialGroupThatAlreadyContainsRows() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_nonempty");
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
  void mapsBothHistoryCandidatesIntoDistinctExplicitTableGroups() throws Exception {
    TableGroupInitializationPlan separate = plan("history-table.yaml", "m0_init_separate");
    TableGroupInitializationPlan nativeVersions = plan("native-cell-versions.yaml", "m0_init_native");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();

    TableGroupInitializer.initialize(separate, admin);
    TableGroupInitializer.initialize(nativeVersions, admin);

    assertEquals(5, admin.descriptors.size());
    assertEquals(64, admin.descriptors.get(TableName.valueOf("m0_init_native:kv"))
        .getColumnFamily(Bytes.toBytes("d"))
        .getMaxVersions());
  }

  private static TableGroupInitializationPlan plan(String resourceName, String namespace)
      throws Exception {
    RuntimeConfiguration configuration;
    try (InputStream input =
        TableGroupInitializerTest.class.getResourceAsStream("/m0-examples/" + resourceName)) {
      if (input == null) {
        throw new IllegalStateException("missing M0 configuration fixture");
      }
      configuration =
          new RuntimeConfigurationLoader(
                  Set.of(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID),
                  Set.of(HashSaltStrategy.STRATEGY_ID))
              .load(input.readAllBytes(), Map.of());
    }
    HBaseProfile profile = configuration.hbaseProfiles().getFirst();
    RuntimeConfiguration isolated =
        new RuntimeConfiguration(
            configuration.schemaVersion(),
            configuration.listeners(),
            List.of(
                new HBaseProfile(
                    profile.id(),
                    profile.quorum(),
                    profile.clientPort(),
                    namespace,
                    profile.authenticationMode(),
                    profile.credentialReference())),
            configuration.saltProfiles(),
            configuration.tableGroups(),
            configuration.resources());
    return TableGroupPlanFactory.create(isolated, "local", null);
  }

  private static final class InMemoryTableGroupAdmin implements TableGroupAdmin {
    private final Set<String> namespaces = new HashSet<>();
    private final Map<TableName, TableDescriptor> descriptors = new HashMap<>();
    private final Map<TableName, List<RegionInfo>> regions = new HashMap<>();
    private final Set<TableName> nonEmptyTables = new HashSet<>();
    private int createNumber;
    private int failOnCreateNumber;

    @Override
    public List<String> listNamespaces() {
      return List.copyOf(namespaces);
    }

    @Override
    public void createNamespace(String namespace) {
      namespaces.add(namespace);
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
    }
  }
}
