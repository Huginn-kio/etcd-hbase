package io.etcdhbase.admin.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class InitializationConfigurationLoaderTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final InitializationConfigurationLoader loader = new InitializationConfigurationLoader();

  @Test
  void readsBothPublishedCandidatesRetainsTheAlternativeLayoutAndOwnsNoInput() throws Exception {
    for (String name : List.of("history-table.json", "native-cell-versions.json")) {
      var closed = new AtomicBoolean();
      var input =
          new ByteArrayInputStream(example(name)) {
            @Override
            public void close() {
              closed.set(true);
            }
          };
      var configuration = loader.load(input);
      assertFalse(closed.get());
      assertEquals(1, configuration.schemaVersion());
      var group = configuration.tableGroups().getFirst();
      assertEquals(
          name.equals("history-table.json")
              ? HistoryLayout.SEPARATE_HISTORY_TABLE
              : HistoryLayout.NATIVE_CELL_VERSIONS,
          group.historyLayout());
      assertEquals(2, group.historyCandidates().size());
      assertEquals(15, configuration.saltProfiles().getFirst().splitKeysHex().size());
      assertThrows(UnsupportedOperationException.class, configuration.tableGroups()::clear);
      assertThrows(
          UnsupportedOperationException.class, group.systemTable().columnFamilies()::clear);
    }
  }

  @Test
  void credentialReferencesSelectAnOpenProviderIdRatherThanAClosedDigestMode() throws Exception {
    ObjectNode tree = exampleTree();
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("authentication_mode", "CREDENTIAL_REFERENCE");
    profile.put("credential_reference", "secret://test/operator-auth");
    profile.put("quorum", "hbase.invalid");
    ((ObjectNode) tree.withArray("table_groups").get(0)).put("identity_provider_id", "custom-sasl");
    ((ObjectNode) tree.withArray("table_groups").get(0)).put("history_layout", "UNDECIDED");
    var configuration = load(tree);
    assertEquals("custom-sasl", configuration.tableGroups().getFirst().identityProviderId());
    assertEquals(HistoryLayout.UNDECIDED, configuration.tableGroups().getFirst().historyLayout());
    assertEquals(
        "secret://test/operator-auth",
        configuration.hbaseProfiles().getFirst().credentialReference());
  }

  @Test
  void rejectsMissingUnknownNullDuplicateCoercedAndMalformedJsonWithoutEchoingValues()
      throws Exception {
    String valid = new String(example("history-table.json"), StandardCharsets.UTF_8);
    String marker = "must-not-echo-this-private-value";
    for (String text :
        List.of(
            valid.replaceFirst("\\{", "{\"unknown_secret\":\"" + marker + "\","),
            valid.replaceFirst(
                "\"schema_version\"\\s*:\\s*1", "\"schema_version\":1,\"schema_version\":1"),
            valid.replaceFirst("\"schema_version\"\\s*:\\s*1", "\"schema_version\":\"1\""),
            valid.replaceFirst("\"schema_version\"\\s*:\\s*1", "\"schema_version\":1.0"),
            valid.replaceFirst("\"schema_version\"\\s*:\\s*1", "\"schema_version\":null"),
            valid.replaceFirst("\"schema_version\"\\s*:\\s*1,", ""),
            valid + " {}",
            "[]")) {
      var failure =
          assertThrows(
              IllegalArgumentException.class, () -> load(text.getBytes(StandardCharsets.UTF_8)));
      assertFalse(failure.getMessage().contains(marker));
      assertNull(failure.getCause(), "parser diagnostics must not carry raw configuration");
    }
    assertThrows(IllegalArgumentException.class, () -> load(new byte[] {(byte) 0xc3, (byte) 0x28}));
    assertThrows(
        IllegalArgumentException.class,
        () -> load(new byte[InitializationConfigurationLoader.MAX_CONFIG_BYTES + 1]));
    assertThrows(IllegalArgumentException.class, () -> load(new byte[0]));
  }

  @Test
  void rejectsInvalidSchemaSplitsVersionsTtlCategoriesRolesAndDevelopmentCredentials()
      throws Exception {
    List<Consumer<ObjectNode>> invalid =
        List.of(
            tree -> tree.put("schema_version", 2),
            tree ->
                ((ObjectNode) tree.withArray("salt_profiles").get(0)).put("encoding_version", 2),
            tree ->
                ((ObjectNode) tree.withArray("salt_profiles").get(0))
                    .withArray("split_keys_hex")
                    .set(0, JSON.getNodeFactory().textNode("0000")),
            tree ->
                ((ObjectNode) tree.at("/table_groups/0/system_table/column_families/0"))
                    .put("min_versions", 2),
            tree ->
                ((ObjectNode) tree.at("/table_groups/0/system_table/column_families/0"))
                    .put("ttl_seconds", 0),
            tree ->
                ((ObjectNode) tree.at("/table_groups/0/system_table/column_families/0"))
                    .put("ttl_seconds", Integer.MAX_VALUE),
            tree ->
                ((ObjectNode) tree.at("/table_groups/0/system_table"))
                    .withArray("record_categories")
                    .remove(0),
            tree ->
                ((ObjectNode) tree.at("/table_groups/0/system_table/identity_permissions/0"))
                    .withArray("permissions")
                    .add("READ"),
            tree ->
                ((ObjectNode) tree.at("/hbase_profiles/0"))
                    .put("credential_reference", "secret://test/unexpected"),
            tree -> ((ObjectNode) tree.at("/hbase_profiles/0")).put("quorum", "hbase.invalid"),
            tree -> ((ObjectNode) tree.at("/hbase_profiles/0")).put("namespace", "hbase"),
            tree -> ((ObjectNode) tree.at("/hbase_profiles/0")).put("client_port", 0));
    for (var mutation : invalid) {
      var tree = exampleTree();
      mutation.accept(tree);
      assertThrows(IllegalArgumentException.class, () -> load(tree));
    }
  }

  @Test
  void detectsSiblingTableOverlapAcrossEquivalentLoopbackProfiles() throws Exception {
    ObjectNode tree = exampleTree();
    ObjectNode profile = tree.withArray("hbase_profiles").get(0).deepCopy();
    profile.put("id", "alias");
    profile.put("quorum", "localhost");
    tree.withArray("hbase_profiles").add(profile);
    ObjectNode group = tree.withArray("table_groups").get(0).deepCopy();
    group.put("id", "sibling");
    group.put("hbase_profile_id", "alias");
    tree.withArray("table_groups").add(group);
    assertThrows(IllegalArgumentException.class, () -> load(tree));
    profile.put("namespace", "another_namespace");
    assertEquals(2, load(tree).tableGroups().size());
  }

  private InitializationConfiguration load(ObjectNode tree) throws Exception {
    return load(JSON.writeValueAsBytes(tree));
  }

  private InitializationConfiguration load(byte[] bytes) {
    return loader.load(new ByteArrayInputStream(bytes));
  }

  private static ObjectNode exampleTree() throws Exception {
    return (ObjectNode) JSON.readTree(example("history-table.json"));
  }

  private static byte[] example(String name) throws Exception {
    return Files.readAllBytes(Path.of("..", "config", "examples", "j05", name));
  }
}
