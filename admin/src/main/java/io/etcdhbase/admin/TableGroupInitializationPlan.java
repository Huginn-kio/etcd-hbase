package io.etcdhbase.admin;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.etcdhbase.admin.config.HBaseBackendScope;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.TableGroup;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Immutable prototype plan, created only from validated configuration by the plan factory. */
public final class TableGroupInitializationPlan {
  private static final ObjectMapper CANONICAL_JSON =
      JsonMapper.builder()
          .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .build();

  private final TableGroup group;
  private final HBaseProfile hbaseProfile;
  private final SaltProfile saltProfile;
  private final HistoryLayout historyLayout;
  private final List<TableSchemaCandidate> tables;
  private final List<String> configuredNamespaceTableNames;
  private final String fingerprint;

  TableGroupInitializationPlan(
      TableGroup group,
      HBaseProfile hbaseProfile,
      SaltProfile saltProfile,
      HistoryLayout historyLayout,
      List<TableSchemaCandidate> tables,
      List<String> namespaceTables) {
    this.group = group;
    this.hbaseProfile = hbaseProfile;
    this.saltProfile = saltProfile;
    this.historyLayout = historyLayout;
    this.tables = List.copyOf(tables);
    this.configuredNamespaceTableNames = List.copyOf(namespaceTables);
    this.fingerprint = fingerprintConfiguration();
  }

  public String tableGroupId() {
    return group.id();
  }

  public String identityProviderId() {
    return group.identityProviderId();
  }

  public int schemaVersion() {
    return group.schemaVersion();
  }

  public String namespace() {
    return hbaseProfile.namespace();
  }

  public HBaseProfile hbaseProfile() {
    return hbaseProfile;
  }

  public SaltProfile saltProfile() {
    return saltProfile;
  }

  public HistoryLayout historyLayout() {
    return historyLayout;
  }

  public List<TableSchemaCandidate> tables() {
    return tables;
  }

  public List<String> configuredNamespaceTableNames() {
    return configuredNamespaceTableNames;
  }

  public String fingerprint() {
    return fingerprint;
  }

  @Override
  public String toString() {
    return "TableGroupInitializationPlan[group="
        + tableGroupId()
        + ", fingerprint=sha256:"
        + fingerprint
        + "]";
  }

  private String fingerprintConfiguration() {
    // Typed canonical JSON gives every sequence and field an unambiguous boundary. References
    // participate in confirmation; resolved credential values are never loaded or serialized.
    Map<String, Object> fields =
        Map.of(
            "format_version",
            1,
            "group_id",
            tableGroupId(),
            "schema_version",
            schemaVersion(),
            "identity_provider_id",
            identityProviderId(),
            "backend",
            hbaseProfile,
            "physical_scope",
            HBaseBackendScope.of(hbaseProfile),
            "salt",
            saltProfile,
            "history_layout",
            historyLayout,
            "namespace_tables",
            configuredNamespaceTableNames,
            "tables",
            tables);
    try {
      byte[] canonical = CANONICAL_JSON.writeValueAsBytes(fields);
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
    } catch (IOException | NoSuchAlgorithmException failure) {
      throw new IllegalStateException(
          "cannot fingerprint the validated initialization plan", failure);
    }
  }
}
