package io.etcdhbase.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/** Explicit runtime overrides; build-tool variables never become runtime settings. */
final class ConfigurationOverrides {
  private static final Set<String> BUILD_VARIABLES =
      Set.of("ETCD_HBASE_JDK21_HOME", "ETCD_HBASE_MAVEN_REPOSITORY");
  private static final Map<String, String> LISTENER_FIELDS =
      Map.of(
          "ETCD_HBASE_LISTENER_BIND_HOST", "bind_host",
          "ETCD_HBASE_LISTENER_PORT", "port",
          "ETCD_HBASE_LISTENER_TABLE_GROUP_ID", "table_group_id");
  private static final Map<String, String> RESOURCE_FIELDS =
      Map.of(
          "ETCD_HBASE_MAX_CONFIG_BYTES", "max_config_bytes",
          "ETCD_HBASE_MAX_CONCURRENT_REQUESTS", "max_concurrent_requests",
          "ETCD_HBASE_REQUEST_DEADLINE_MS", "request_deadline_ms",
          "ETCD_HBASE_MAX_RPC_BYTES", "max_rpc_bytes",
          "ETCD_HBASE_MAX_REQUEST_BYTES", "max_request_bytes",
          "ETCD_HBASE_MAX_SCANNERS_PER_TABLE", "max_scanners_per_table",
          "ETCD_HBASE_MAX_QUEUED_REQUESTS", "max_queued_requests");

  private ConfigurationOverrides() {}

  static long apply(JsonNode root, Map<String, String> environment) {
    long bytes = 0;
    for (var entry : environment.entrySet()) {
      String name = entry.getKey();
      if (!name.startsWith("ETCD_HBASE_") || BUILD_VARIABLES.contains(name)) continue;
      String field = LISTENER_FIELDS.get(name);
      JsonNode target = field == null ? root.path("resources") : root.path("listeners").path(0);
      if (field == null) field = RESOURCE_FIELDS.get(name);
      if (field == null || !(target instanceof ObjectNode object)) {
        throw new IllegalArgumentException("unknown runtime override or missing target object");
      }
      String value = entry.getValue();
      if (value == null || value.length() > 65_536) {
        throw new IllegalArgumentException("runtime override exceeds its value budget");
      }
      bytes += name.length() + value.getBytes(StandardCharsets.UTF_8).length;
      if (field.equals("bind_host") || field.equals("table_group_id")) {
        object.put(field, value);
      } else {
        object.put(field, integer(value));
      }
    }
    return bytes;
  }

  private static int integer(String value) {
    if (!value.matches("[0-9]{1,10}")) {
      throw new IllegalArgumentException("runtime override must be a nonnegative decimal integer");
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException failure) {
      throw new IllegalArgumentException("runtime override integer is out of range");
    }
  }
}
