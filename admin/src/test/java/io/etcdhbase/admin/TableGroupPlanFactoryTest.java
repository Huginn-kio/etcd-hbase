package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TableGroupPlanFactoryTest {
  @Test
  void selectsBothCandidatesAndProducesARepeatableFingerprintWithoutOpeningHBase()
      throws Exception {
    var separate = InitializationFixtures.plan("history-table.json", "m0_plan");
    var nativeVersions = InitializationFixtures.plan("native-cell-versions.json", "m0_plan");
    assertEquals(
        List.of("system", "latest", "history"),
        separate.tables().stream().map(TableSchemaCandidate::tableName).toList());
    assertEquals(
        List.of("system", "kv"),
        nativeVersions.tables().stream().map(TableSchemaCandidate::tableName).toList());
    assertEquals(15, separate.saltProfile().splitKeysHex().size());
    assertEquals(64, separate.fingerprint().length());
    assertEquals(
        separate.fingerprint(),
        InitializationFixtures.plan("history-table.json", "m0_plan").fingerprint());
    assertNotEquals(separate.fingerprint(), nativeVersions.fingerprint());
  }

  @Test
  void requiresAnExplicitDecisionForMultipleGroupsOrAnUndecidedLayout() throws Exception {
    var siblings = InitializationFixtures.load(InitializationFixtures.withSibling("m0_plan"));
    assertThrows(
        IllegalArgumentException.class, () -> TableGroupPlanFactory.create(siblings, null, null));
    var tree = InitializationFixtures.example("history-table.json", "m0_plan");
    ((ObjectNode) tree.withArray("table_groups").get(0)).put("history_layout", "UNDECIDED");
    var undecided = InitializationFixtures.load(tree);
    assertThrows(
        IllegalArgumentException.class,
        () -> TableGroupPlanFactory.create(undecided, "local", null));
    assertEquals(
        HistoryLayout.NATIVE_CELL_VERSIONS,
        TableGroupPlanFactory.create(undecided, "local", HistoryLayout.NATIVE_CELL_VERSIONS)
            .historyLayout());
    var concrete =
        InitializationFixtures.load(
            InitializationFixtures.example("history-table.json", "m0_plan"));
    assertThrows(
        IllegalArgumentException.class,
        () -> TableGroupPlanFactory.create(concrete, "local", HistoryLayout.NATIVE_CELL_VERSIONS));
  }

  @Test
  void fingerprintsSchemaAndTargetChangesAndRedactsConnectionAndCredentialReferences()
      throws Exception {
    var tree = InitializationFixtures.example("history-table.json", "m0_plan");
    var before = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    ((ObjectNode) tree.at("/table_groups/0/system_table/column_families/0")).put("max_versions", 2);
    var after = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    assertNotEquals(before.fingerprint(), after.fingerprint());
    var profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("authentication_mode", "CREDENTIAL_REFERENCE");
    profile.put("credential_reference", "secret://test/private-reference");
    profile.put("quorum", "private-test.invalid");
    ((ObjectNode) tree.withArray("table_groups").get(0)).put("identity_provider_id", "custom-sasl");
    var secure = TableGroupPlanFactory.create(InitializationFixtures.load(tree), "local", null);
    assertNotEquals(after.fingerprint(), secure.fingerprint());
    assertFalse(secure.toString().contains("private-test.invalid"));
    assertFalse(secure.toString().contains("private-reference"));
  }

  @Test
  void recordsConfiguredSiblingScopeWithoutIncludingTheUnselectedOwnLayout() throws Exception {
    var configuration = InitializationFixtures.load(InitializationFixtures.withSibling("m0_plan"));
    var plan = TableGroupPlanFactory.create(configuration, "local", null);
    assertEquals(
        Set.of(
            "system", "latest", "history", "sibling_system", "sibling_latest", "sibling_history"),
        Set.copyOf(plan.configuredNamespaceTableNames()));
    assertFalse(plan.configuredNamespaceTableNames().contains("kv"));
    assertThrows(UnsupportedOperationException.class, () -> plan.tables().clear());
  }
}
