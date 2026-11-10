package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
import org.junit.jupiter.api.Test;

class M3FreshTableCreatorTest {
  @Test
  void freshTablesContainTheSelectedStorageFormatForBothLayouts() throws Exception {
    for (String file : List.of("history-table.json", "native-cell-versions.json")) {
      var plan = M3TableGroupPlan.from(M3TableGroupPlanTest.plan(file, true));
      var admin = new MemoryAdmin();
      var created = M3FreshTableCreator.create(plan, admin);
      assertEquals(plan.source().tables().size(), created.tables().size());
      plan.system().validate(admin.descriptors.get(plan.system().table()));
      plan.primary().validate(admin.descriptors.get(plan.primary().table()));
      plan.current().ifPresent(binding -> binding.validate(admin.descriptors.get(binding.table())));
      assertEquals(plan.source().fingerprint(), created.planFingerprint());
    }
  }

  @Test
  void rejectsAnyPreexistingTargetTableWithoutWritingOrAdoptingIt() throws Exception {
    var plan = M3TableGroupPlan.from(M3TableGroupPlanTest.plan("history-table.json", true));
    var admin = new MemoryAdmin();
    M3FreshTableCreator.create(plan, admin);
    int creates = admin.creates;
    assertThrows(IllegalArgumentException.class, () -> M3FreshTableCreator.create(plan, admin));
    assertEquals(creates, admin.creates);
  }

  @Test
  void lostCreateAcknowledgementStopsBeforeAnyFurtherTableIsCreated() throws Exception {
    var plan = M3TableGroupPlan.from(M3TableGroupPlanTest.plan("history-table.json", true));
    var admin = new MemoryAdmin();
    admin.failAfterFirstCreate = true;
    var failure = assertThrows(M3FreshTableCreator.PartialCreationException.class,
        () -> M3FreshTableCreator.create(plan, admin));
    assertTrue(failure.observedCreated().isEmpty());
    assertEquals(1, admin.creates);
    assertEquals(1, admin.descriptors.size(), "the first server-side create may have succeeded");
    assertThrows(IllegalArgumentException.class, () -> M3FreshTableCreator.create(plan, admin));
    assertEquals(1, admin.creates);
  }

  private static final class MemoryAdmin implements TableGroupAdmin {
    private final Set<String> namespaces = new HashSet<>();
    private final Map<TableName, TableDescriptor> descriptors = new HashMap<>();
    private final Map<TableName, List<RegionInfo>> regions = new HashMap<>();
    private int creates;
    private boolean failAfterFirstCreate;

    @Override public String clusterId() { return "fixture-cluster"; }
    @Override public List<String> listNamespaces() { return List.copyOf(namespaces); }
    @Override public void createNamespace(String namespace) { namespaces.add(namespace); }
    @Override public List<TableName> listTableNames(String namespace) {
      return descriptors.keySet().stream()
          .filter(table -> table.getNamespaceAsString().equals(namespace)).sorted().toList();
    }
    @Override public TableDescriptor getDescriptor(TableName table) { return descriptors.get(table); }
    @Override public List<RegionInfo> getRegions(TableName table) { return regions.get(table); }
    @Override public boolean isEmpty(TableName table) { return true; }
    @Override public void createTable(TableDescriptor descriptor, byte[][] splits) throws Exception {
      creates++;
      var table = descriptor.getTableName();
      if (descriptors.putIfAbsent(table, descriptor) != null) {
        throw new org.apache.hadoop.hbase.TableExistsException(table);
      }
      var starts = new ArrayList<byte[]>();
      starts.add(new byte[0]);
      starts.addAll(Arrays.asList(splits));
      var created = new ArrayList<RegionInfo>();
      for (int index = 0; index < starts.size(); index++) {
        byte[] end = index + 1 == starts.size() ? new byte[0] : starts.get(index + 1);
        created.add(RegionInfoBuilder.newBuilder(table).setStartKey(starts.get(index)).setEndKey(end).build());
      }
      regions.put(table, List.copyOf(created));
      if (failAfterFirstCreate) throw new IOException("fixture lost create acknowledgement");
    }
  }
}
