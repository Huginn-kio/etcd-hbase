package io.etcdhbase.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class InitializationFixtures {
  private static final ObjectMapper JSON = new ObjectMapper();

  private InitializationFixtures() {}

  static ObjectNode example(String name, String namespace) throws IOException {
    ObjectNode tree =
        (ObjectNode)
            JSON.readTree(Files.readAllBytes(Path.of("..", "config", "examples", "j05", name)));
    ((ObjectNode) tree.withArray("hbase_profiles").get(0)).put("namespace", namespace);
    return tree;
  }

  static InitializationConfiguration load(ObjectNode tree) throws IOException {
    return new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(JSON.writeValueAsBytes(tree)));
  }

  static TableGroupInitializationPlan plan(String name, String namespace) throws IOException {
    return TableGroupPlanFactory.create(load(example(name, namespace)), "local", null);
  }

  static ObjectNode withSibling(String namespace) throws IOException {
    ObjectNode tree = example("history-table.json", namespace);
    ObjectNode sibling = tree.withArray("table_groups").get(0).deepCopy();
    sibling.put("id", "sibling");
    ObjectNode system = (ObjectNode) sibling.get("system_table");
    system.put("table_name", "sibling_system");
    for (var candidate : sibling.withArray("history_candidates")) {
      for (var table : candidate.withArray("tables")) {
        ((ObjectNode) table).put("table_name", "sibling_" + table.get("table_name").asText());
      }
    }
    tree.withArray("table_groups").add(sibling);
    return tree;
  }
}
