package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import org.junit.jupiter.api.Test;

class M3TableGroupPlanTest {
  @Test
  void mapsBothConfiguredLayoutsToTheirActualHBaseBindings() throws Exception {
    var separate = M3TableGroupPlan.from(plan("history-table.json", true));
    assertEquals("m3_plan:system", separate.system().table().getNameAsString());
    assertEquals("m3_plan:history", separate.primary().table().getNameAsString());
    assertEquals(StorageTableBinding.Layout.HISTORY, separate.primary().layout());
    assertEquals("m3_plan:latest", separate.current().orElseThrow().table().getNameAsString());
    assertEquals(StorageTableBinding.Layout.CURRENT, separate.current().orElseThrow().layout());
    assertEquals(separate.system().saltProfile(), separate.primary().codec().saltProfile());

    var nativeLayout = M3TableGroupPlan.from(plan("native-cell-versions.json", true));
    assertEquals("m3_plan:kv", nativeLayout.primary().table().getNameAsString());
    assertEquals(StorageTableBinding.Layout.NATIVE, nativeLayout.primary().layout());
    assertTrue(nativeLayout.current().isEmpty());
    assertEquals(16, nativeLayout.primary().codec().saltProfile().bucketCount());
  }

  @Test
  void refusesAnOlderSystemTableWithoutTheRequiredResultCapability() throws Exception {
    var oldPlan = plan("history-table.json", false);
    assertFalse(oldPlan.tables().getFirst().recordCategories().contains(
        io.etcdhbase.admin.config.InitializationConfiguration.RecordCategory.RESULT));
    assertThrows(IllegalArgumentException.class, () -> M3TableGroupPlan.from(oldPlan));
  }

  @Test
  void refusesADataTableWithExpiringRevisionHistory() throws Exception {
    ObjectNode tree = InitializationFixtures.example("native-cell-versions.json", "m3_plan");
    ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
    ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
    var candidates = group.withArray("history_candidates");
    ObjectNode nativeTable = (ObjectNode) candidates.get(1).withArray("tables").get(0);
    ((ObjectNode) nativeTable.withArray("column_families").get(0)).put("ttl_seconds", 60);
    var unsafe = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    assertThrows(IllegalArgumentException.class, () -> M3TableGroupPlan.from(unsafe));
  }

  static TableGroupInitializationPlan plan(String filename, boolean results) throws Exception {
    ObjectNode tree = InitializationFixtures.example(filename, "m3_plan");
    if (results) {
      ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
      ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
    }
    return TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
  }
}
