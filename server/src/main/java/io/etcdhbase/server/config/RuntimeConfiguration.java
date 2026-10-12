package io.etcdhbase.server.config;

import java.util.List;

/** Immutable server configuration model validated by {@link RuntimeConfigurationLoader}. */
public record RuntimeConfiguration(
    int schemaVersion,
    List<Listener> listeners,
    List<HBaseProfile> hbaseProfiles,
    List<SaltProfile> saltProfiles,
    List<TableGroup> tableGroups,
    ResourceBudgets resources) {

  public RuntimeConfiguration {
    listeners = immutableOrEmpty(listeners);
    hbaseProfiles = immutableOrEmpty(hbaseProfiles);
    saltProfiles = immutableOrEmpty(saltProfiles);
    tableGroups = immutableOrEmpty(tableGroups);
  }

  private static <T> List<T> immutableOrEmpty(List<T> values) {
    return values == null ? List.of() : List.copyOf(values);
  }

  public record Listener(String id, String bindHost, int port, String tableGroupId) {}

  public record HBaseProfile(
      String id,
      String quorum,
      int clientPort,
      String namespace,
      AuthenticationMode authenticationMode,
      String credentialReference) {}

  public record SaltProfile(
      String id,
      String strategyId,
      int encodingVersion,
      int bucketCount,
      int saltWidthBytes,
      List<String> splitKeysHex) {
    public SaltProfile {
      splitKeysHex = immutableOrEmpty(splitKeysHex);
    }
  }

  public record TableGroup(
      String id,
      int schemaVersion,
      String hbaseProfileId,
      String saltProfileId,
      String identityProviderId,
      HistoryLayout historyLayout) {}

  public record ResourceBudgets(
      int maxConfigBytes,
      int maxConcurrentRequests,
      int maxScannersPerTable,
      long maxRequestBytes) {}

  public enum AuthenticationMode {
    DEV_NO_AUTH,
    CREDENTIAL_REFERENCE
  }

  public enum HistoryLayout {
    UNDECIDED,
    SEPARATE_HISTORY_TABLE,
    NATIVE_CELL_VERSIONS
  }
}
