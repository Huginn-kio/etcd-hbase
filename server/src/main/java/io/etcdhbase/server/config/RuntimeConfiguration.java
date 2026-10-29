package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.TableGroup;
import java.util.List;

/** Service configuration reuses the exact initialization models and validation rules. */
public record RuntimeConfiguration(
    int schemaVersion,
    List<Listener> listeners,
    List<HBaseProfile> hbaseProfiles,
    List<SaltProfile> saltProfiles,
    List<TableGroup> tableGroups,
    ResourceBudgets resources) {
  public RuntimeConfiguration {
    listeners = List.copyOf(listeners);
    hbaseProfiles = List.copyOf(hbaseProfiles);
    saltProfiles = List.copyOf(saltProfiles);
    tableGroups = List.copyOf(tableGroups);
  }

  public InitializationConfiguration initialization() {
    return new InitializationConfiguration(schemaVersion, hbaseProfiles, saltProfiles, tableGroups);
  }

  public record Listener(String id, String bindHost, int port, String tableGroupId) {}

  /** Queueing is disabled until a cancellable queue exists; scanner limits belong to adapters. */
  public record ResourceBudgets(
      int maxConfigBytes,
      int maxConcurrentRequests,
      int requestDeadlineMs,
      int maxRpcBytes,
      int maxRequestBytes,
      int maxScannersPerTable,
      int maxQueuedRequests) {}
}
