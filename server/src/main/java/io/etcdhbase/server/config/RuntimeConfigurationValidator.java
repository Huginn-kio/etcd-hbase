package io.etcdhbase.server.config;

import io.etcdhbase.server.config.RuntimeConfiguration.AuthenticationMode;
import io.etcdhbase.server.config.RuntimeConfiguration.ColumnFamilySchema;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayoutCandidate;
import io.etcdhbase.server.config.RuntimeConfiguration.IdentityPermission;
import io.etcdhbase.server.config.RuntimeConfiguration.KeepDeletedCells;
import io.etcdhbase.server.config.RuntimeConfiguration.RecordCategory;
import io.etcdhbase.server.config.RuntimeConfiguration.TableSchemaCandidate;
import io.etcdhbase.server.config.RuntimeConfiguration.TablePermission;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Applies the cross-field and resource checks required before a configuration can be used. */
final class RuntimeConfigurationValidator {
  private static final int MAX_CONCURRENT_REQUESTS = 100_000;
  private static final int MAX_SCANNERS_PER_TABLE = 1_024;
  private static final int MAX_REQUEST_DEADLINE_MILLIS = 600_000;
  private static final int MAX_QUEUED_REQUESTS = 100_000;
  private static final long MAX_RPC_BYTES = 64L * 1024 * 1024;
  private static final long MAX_REQUEST_BYTES = 64L * 1024 * 1024;
  private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9._-]{0,63}");
  private static final Pattern ENV_REFERENCE_PATTERN =
      Pattern.compile("env://[A-Za-z_][A-Za-z0-9_]*");
  private static final Pattern SECRET_REFERENCE_PATTERN =
      Pattern.compile("secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,127}");
  private static final Pattern HEX_PATTERN = Pattern.compile("[0-9A-Fa-f]+");
  private static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}");
  private static final Pattern COLUMN_FAMILY_NAME_PATTERN =
      Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]*");

  private final Set<String> supportedProviderIds;
  private final Set<String> supportedSaltStrategyIds;

  RuntimeConfigurationValidator(Set<String> supportedProviderIds, Set<String> supportedSaltStrategyIds) {
    this.supportedProviderIds = immutableNonBlankSet(supportedProviderIds, "provider IDs");
    this.supportedSaltStrategyIds = immutableNonBlankSet(supportedSaltStrategyIds, "salt strategy IDs");
  }

  private static Set<String> immutableNonBlankSet(Set<String> values, String field) {
    if (values == null) {
      throw new IllegalArgumentException(field + " are required");
    }
    Set<String> copy = new HashSet<>();
    for (String value : values) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException(field + " must not contain blank values");
      }
      copy.add(value);
    }
    return Set.copyOf(copy);
  }

  void validate(RuntimeConfiguration configuration, int actualBytes) {
    if (configuration == null) {
      throw invalid("configuration", "is required");
    }
    if (configuration.schemaVersion() != 1) {
      throw invalid("schema_version", "must be 1");
    }
    if (configuration.listeners().isEmpty()) {
      throw invalid("listeners", "must contain at least one listener");
    }
    if (configuration.hbaseProfiles().isEmpty()) {
      throw invalid("hbase_profiles", "must contain at least one profile");
    }
    if (configuration.saltProfiles().isEmpty()) {
      throw invalid("salt_profiles", "must contain at least one profile");
    }
    if (configuration.tableGroups().isEmpty()) {
      throw invalid("table_groups", "must contain at least one table group");
    }

    Map<String, RuntimeConfiguration.HBaseProfile> hbaseProfiles =
        indexHBaseProfiles(configuration.hbaseProfiles());
    Map<String, RuntimeConfiguration.SaltProfile> saltProfiles =
        indexSaltProfiles(configuration.saltProfiles());
    Map<String, RuntimeConfiguration.TableGroup> tableGroups =
        indexTableGroups(configuration.tableGroups(), hbaseProfiles, saltProfiles);
    validateListeners(configuration.listeners(), tableGroups, hbaseProfiles);
    validateResources(configuration.resources(), actualBytes);
  }

  private Map<String, RuntimeConfiguration.HBaseProfile> indexHBaseProfiles(
      List<RuntimeConfiguration.HBaseProfile> profiles) {
    Map<String, RuntimeConfiguration.HBaseProfile> byId = new HashMap<>();
    for (int index = 0; index < profiles.size(); index++) {
      RuntimeConfiguration.HBaseProfile profile = profiles.get(index);
      String path = "hbase_profiles[" + index + "]";
      if (profile == null) {
        throw invalid(path, "is required");
      }
      validateId(profile.id(), path + ".id");
      if (byId.putIfAbsent(profile.id(), profile) != null) {
        throw invalid(path + ".id", "must be unique");
      }
      requireText(profile.quorum(), path + ".quorum");
      if (profile.quorum().length() > 1024 || containsWhitespaceOrControl(profile.quorum())) {
        throw invalid(path + ".quorum", "must be a bounded host list without whitespace");
      }
      requirePort(profile.clientPort(), path + ".client_port");
      requireText(profile.namespace(), path + ".namespace");
      if (profile.namespace().length() > 255 || containsWhitespaceOrControl(profile.namespace())) {
        throw invalid(path + ".namespace", "must be a bounded namespace name");
      }
      validateAuthentication(profile, path);
    }
    return byId;
  }

  private Map<String, RuntimeConfiguration.SaltProfile> indexSaltProfiles(
      List<RuntimeConfiguration.SaltProfile> profiles) {
    Map<String, RuntimeConfiguration.SaltProfile> byId = new HashMap<>();
    for (int index = 0; index < profiles.size(); index++) {
      RuntimeConfiguration.SaltProfile profile = profiles.get(index);
      String path = "salt_profiles[" + index + "]";
      if (profile == null) {
        throw invalid(path, "is required");
      }
      validateId(profile.id(), path + ".id");
      if (byId.putIfAbsent(profile.id(), profile) != null) {
        throw invalid(path + ".id", "must be unique");
      }
      requireSupportedId(profile.strategyId(), supportedSaltStrategyIds, path + ".strategy_id");
      if (profile.encodingVersion() < 1) {
        throw invalid(path + ".encoding_version", "must be positive");
      }
      if (profile.bucketCount() < 1) {
        throw invalid(path + ".bucket_count", "must be positive");
      }
      try {
        new SaltProfile(
            profile.strategyId(),
            profile.encodingVersion(),
            profile.bucketCount(),
            profile.saltWidthBytes());
      } catch (IllegalArgumentException exception) {
        throw invalid(path + ".salt_width_bytes", "cannot encode the configured buckets");
      }
      validateSplitKeys(profile, path);
    }
    return byId;
  }

  private static void validateSplitKeys(RuntimeConfiguration.SaltProfile profile, String path) {
    List<byte[]> splitKeys = new ArrayList<>(profile.splitKeysHex().size());
    byte[] previous = null;
    for (int index = 0; index < profile.splitKeysHex().size(); index++) {
      String encoded = profile.splitKeysHex().get(index);
      String splitPath = path + ".split_keys_hex[" + index + "]";
      if (encoded == null
          || encoded.length() != profile.saltWidthBytes() * 2
          || !HEX_PATTERN.matcher(encoded).matches()) {
        throw invalid(splitPath, "must be fixed-width hexadecimal");
      }
      byte[] bytes = decodeHex(encoded);
      if (previous != null && compareUnsigned(previous, bytes) >= 0) {
        throw invalid(splitPath, "must be strictly increasing");
      }
      splitKeys.add(bytes);
      previous = bytes;
    }

    if (HashSaltStrategy.STRATEGY_ID.equals(profile.strategyId())) {
      if (profile.encodingVersion() != 1) {
        throw invalid(path + ".encoding_version", "crc32-bucket-v1 requires version 1");
      }
      if ((long) profile.bucketCount() - 1 != splitKeys.size()) {
        throw invalid(path + ".split_keys_hex", "must contain one boundary for each nonzero bucket");
      }
      SaltProfile parameters =
          new SaltProfile(
              profile.strategyId(),
              profile.encodingVersion(),
              profile.bucketCount(),
              profile.saltWidthBytes());
      for (int bucket = 1; bucket < profile.bucketCount(); bucket++) {
        if (!Arrays.equals(splitKeys.get(bucket - 1), parameters.encodeBucket(bucket))) {
          throw invalid(path + ".split_keys_hex[" + (bucket - 1) + "]", "does not match the bucket encoding");
        }
      }
    }
  }

  private Map<String, RuntimeConfiguration.TableGroup> indexTableGroups(
      List<RuntimeConfiguration.TableGroup> groups,
      Map<String, RuntimeConfiguration.HBaseProfile> hbaseProfiles,
      Map<String, RuntimeConfiguration.SaltProfile> saltProfiles) {
    Map<String, RuntimeConfiguration.TableGroup> byId = new HashMap<>();
    Map<String, Set<String>> tableNamesByHBaseProfile = new HashMap<>();
    for (int index = 0; index < groups.size(); index++) {
      RuntimeConfiguration.TableGroup group = groups.get(index);
      String path = "table_groups[" + index + "]";
      if (group == null) {
        throw invalid(path, "is required");
      }
      validateId(group.id(), path + ".id");
      if (byId.putIfAbsent(group.id(), group) != null) {
        throw invalid(path + ".id", "must be unique");
      }
      if (group.schemaVersion() != 1) {
        throw invalid(path + ".schema_version", "must be 1");
      }
      if (!hbaseProfiles.containsKey(group.hbaseProfileId())) {
        throw invalid(path + ".hbase_profile_id", "references an unknown profile");
      }
      if (!saltProfiles.containsKey(group.saltProfileId())) {
        throw invalid(path + ".salt_profile_id", "references an unknown profile");
      }
      requireSupportedId(
          group.identityProviderId(), supportedProviderIds, path + ".identity_provider_id");
      if (group.historyLayout() == null) {
        throw invalid(path + ".history_layout", "is required");
      }
      validateTableGroupSchemas(group, saltProfiles.get(group.saltProfileId()), path);
      Set<String> profileTableNames =
          tableNamesByHBaseProfile.computeIfAbsent(group.hbaseProfileId(), ignored -> new HashSet<>());
      for (String tableName : tableNames(group)) {
        if (!profileTableNames.add(tableName)) {
          throw invalid(path + ".tables", "conflicts with another table group using the same HBase profile");
        }
      }
    }
    return byId;
  }

  private static List<String> tableNames(RuntimeConfiguration.TableGroup group) {
    List<String> tableNames = new ArrayList<>();
    Set<String> uniqueNames = new HashSet<>();
    addTableName(group.systemTable(), tableNames, uniqueNames);
    for (HistoryLayoutCandidate candidate : group.historyCandidates()) {
      for (TableSchemaCandidate table : candidate.tables()) {
        addTableName(table, tableNames, uniqueNames);
      }
    }
    return tableNames;
  }

  private static void addTableName(
      TableSchemaCandidate table, List<String> orderedNames, Set<String> uniqueNames) {
    if (uniqueNames.add(table.tableName())) {
      orderedNames.add(table.tableName());
    }
  }

  private static void validateTableGroupSchemas(
      RuntimeConfiguration.TableGroup group,
      RuntimeConfiguration.SaltProfile saltProfile,
      String path) {
    validateTableSchema(group.systemTable(), saltProfile, path + ".system_table");
    Set<RecordCategory> systemCategories = new HashSet<>(group.systemTable().recordCategories());
    if (!systemCategories.equals(
        Set.of(
            RecordCategory.DECISION,
            RecordCategory.EVENT,
            RecordCategory.LEASE,
            RecordCategory.AUTH,
            RecordCategory.CONTROL))) {
      throw invalid(path + ".system_table.record_categories", "must cover the system record categories");
    }
    if (group.historyCandidates().size() != 2) {
      throw invalid(path + ".history_candidates", "must describe both history layouts");
    }

    Set<HistoryLayout> layouts = new HashSet<>();
    for (int index = 0; index < group.historyCandidates().size(); index++) {
      HistoryLayoutCandidate candidate = group.historyCandidates().get(index);
      String candidatePath = path + ".history_candidates[" + index + "]";
      if (candidate == null || candidate.historyLayout() == null) {
        throw invalid(candidatePath + ".history_layout", "is required");
      }
      HistoryLayout layout = candidate.historyLayout();
      if (layout == HistoryLayout.UNDECIDED) {
        throw invalid(candidatePath + ".history_layout", "must name a concrete candidate");
      }
      if (!layouts.add(layout)) {
        throw invalid(candidatePath + ".history_layout", "must be unique");
      }

      Set<String> tableNames = new HashSet<>();
      tableNames.add(group.systemTable().tableName());
      Set<RecordCategory> assignedCategories = new HashSet<>(systemCategories);
      for (int tableIndex = 0; tableIndex < candidate.tables().size(); tableIndex++) {
        TableSchemaCandidate table = candidate.tables().get(tableIndex);
        String tablePath = candidatePath + ".tables[" + tableIndex + "]";
        validateTableSchema(table, saltProfile, tablePath);
        if (!tableNames.add(table.tableName())) {
          throw invalid(tablePath + ".table_name", "must be unique within the table group");
        }
        for (RecordCategory category : table.recordCategories()) {
          if (!assignedCategories.add(category)) {
            throw invalid(tablePath + ".record_categories", "must not assign one category to multiple tables");
          }
        }
      }

      if (layout == HistoryLayout.SEPARATE_HISTORY_TABLE) {
        if (candidate.tables().size() != 2
            || !candidate.tables().stream().anyMatch(
                table -> table.recordCategories().equals(List.of(RecordCategory.CURRENT)))
            || !candidate.tables().stream().anyMatch(
                table -> table.recordCategories().equals(List.of(RecordCategory.HISTORY)))) {
          throw invalid(candidatePath + ".tables", "must separate CURRENT and HISTORY into two tables");
        }
      } else if (layout == HistoryLayout.NATIVE_CELL_VERSIONS) {
        if (candidate.tables().size() != 1
            || !new HashSet<>(candidate.tables().getFirst().recordCategories())
                .equals(Set.of(RecordCategory.CURRENT, RecordCategory.HISTORY))) {
          throw invalid(candidatePath + ".tables", "must store CURRENT and HISTORY in one table");
        }
        validateNativeVersionCandidate(candidate.tables().getFirst(), candidatePath + ".tables[0]");
      }
    }

    if (!layouts.contains(HistoryLayout.SEPARATE_HISTORY_TABLE)
        || !layouts.contains(HistoryLayout.NATIVE_CELL_VERSIONS)) {
      throw invalid(path + ".history_candidates", "must include both supported history layouts");
    }
    if (group.historyLayout() != HistoryLayout.UNDECIDED && !layouts.contains(group.historyLayout())) {
      throw invalid(path + ".history_layout", "does not name a configured candidate");
    }
  }

  private static void validateTableSchema(
      TableSchemaCandidate schema,
      RuntimeConfiguration.SaltProfile saltProfile,
      String path) {
    if (schema == null) {
      throw invalid(path, "is required");
    }
    if (schema.tableName() == null
        || !TABLE_NAME_PATTERN.matcher(schema.tableName()).matches()
        || schema.tableName().equals("zookeeper")) {
      throw invalid(path + ".table_name", "must be a bounded unqualified table name");
    }
    if (schema.schemaVersion() != 1) {
      throw invalid(path + ".schema_version", "must be 1");
    }
    if (schema.profileVersion() != saltProfile.encodingVersion()) {
      throw invalid(path + ".profile_version", "must match the table group's salt profile version");
    }
    if (!sameHexKeys(schema.splitKeysHex(), saltProfile.splitKeysHex())) {
      throw invalid(path + ".split_keys_hex", "must match the table group's salt profile");
    }
    if (schema.columnFamilies().isEmpty()) {
      throw invalid(path + ".column_families", "must contain at least one family");
    }
    if (schema.recordCategories().isEmpty()) {
      throw invalid(path + ".record_categories", "must contain at least one category");
    }
    if (new HashSet<>(schema.recordCategories()).size() != schema.recordCategories().size()) {
      throw invalid(path + ".record_categories", "must be unique");
    }
    validateIdentityPermissions(schema.identityPermissions(), path + ".identity_permissions");

    Set<String> familyNames = new HashSet<>();
    for (int index = 0; index < schema.columnFamilies().size(); index++) {
      ColumnFamilySchema family = schema.columnFamilies().get(index);
      String familyPath = path + ".column_families[" + index + "]";
      if (family == null) {
        throw invalid(familyPath, "is required");
      }
      if (family.name() == null || !COLUMN_FAMILY_NAME_PATTERN.matcher(family.name()).matches()) {
        throw invalid(familyPath + ".name", "must be a supported column-family name");
      }
      if (family.name().equals("recovered.edits")) {
        throw invalid(familyPath + ".name", "is reserved by HBase");
      }
      if (!familyNames.add(family.name())) {
        throw invalid(familyPath + ".name", "must be unique within the table");
      }
      if (family.maxVersions() < 1) {
        throw invalid(familyPath + ".max_versions", "must be positive");
      }
      if (family.minVersions() < 0 || family.minVersions() >= family.maxVersions()) {
        throw invalid(familyPath + ".min_versions", "must be nonnegative and less than max_versions");
      }
      if (family.ttlSeconds() != -1 && family.ttlSeconds() < 1) {
        throw invalid(familyPath + ".ttl_seconds", "must be -1 (forever) or positive seconds");
      }
      if (family.minVersions() > 0 && family.ttlSeconds() == -1) {
        throw invalid(familyPath + ".min_versions", "requires a finite ttl_seconds");
      }
      if (family.keepDeletedCells() == null) {
        throw invalid(familyPath + ".keep_deleted_cells", "is required");
      }
      if (family.keepDeletedCells() == KeepDeletedCells.TTL && family.ttlSeconds() == -1) {
        throw invalid(familyPath + ".keep_deleted_cells", "TTL retention requires a finite ttl_seconds");
      }
    }
  }

  private static void validateIdentityPermissions(List<IdentityPermission> permissions, String path) {
    if (permissions.isEmpty()) {
      throw invalid(path, "must declare at least one logical identity role");
    }
    Set<String> identityRefs = new HashSet<>();
    for (int index = 0; index < permissions.size(); index++) {
      IdentityPermission permission = permissions.get(index);
      String permissionPath = path + "[" + index + "]";
      if (permission == null) {
        throw invalid(permissionPath, "is required");
      }
      validateId(permission.identityRef(), permissionPath + ".identity_ref");
      if (!identityRefs.add(permission.identityRef())) {
        throw invalid(permissionPath + ".identity_ref", "must be unique within the table");
      }
      if (permission.permissions().isEmpty()) {
        throw invalid(permissionPath + ".permissions", "must contain at least one permission");
      }
      if (new HashSet<>(permission.permissions()).size() != permission.permissions().size()) {
        throw invalid(permissionPath + ".permissions", "must be unique");
      }
    }
  }

  private static boolean sameHexKeys(List<String> actual, List<String> expected) {
    if (actual.size() != expected.size()) {
      return false;
    }
    for (int index = 0; index < actual.size(); index++) {
      if (actual.get(index) == null || !actual.get(index).equalsIgnoreCase(expected.get(index))) {
        return false;
      }
    }
    return true;
  }

  private static void validateNativeVersionCandidate(TableSchemaCandidate schema, String path) {
    for (int index = 0; index < schema.columnFamilies().size(); index++) {
      ColumnFamilySchema family = schema.columnFamilies().get(index);
      String familyPath = path + ".column_families[" + index + "]";
      if (family.maxVersions() < 2) {
        throw invalid(familyPath + ".max_versions", "must retain multiple cell versions for this candidate");
      }
      if (family.ttlSeconds() != -1) {
        throw invalid(familyPath + ".ttl_seconds", "must be -1 while Cell timestamps represent logical revisions");
      }
    }
  }

  private static void validateListeners(
      List<RuntimeConfiguration.Listener> listeners,
      Map<String, RuntimeConfiguration.TableGroup> tableGroups,
      Map<String, RuntimeConfiguration.HBaseProfile> hbaseProfiles) {
    Set<String> ids = new HashSet<>();
    List<ListenerAddress> addresses = new ArrayList<>();
    for (int index = 0; index < listeners.size(); index++) {
      RuntimeConfiguration.Listener listener = listeners.get(index);
      String path = "listeners[" + index + "]";
      if (listener == null) {
        throw invalid(path, "is required");
      }
      validateId(listener.id(), path + ".id");
      if (!ids.add(listener.id())) {
        throw invalid(path + ".id", "must be unique");
      }
      String host = normalizeHost(listener.bindHost(), path + ".bind_host");
      requireListenerPort(listener.port(), path + ".port");
      if (listener.port() == 0 && !isLoopbackHost(host)) {
        throw invalid(path + ".port", "port 0 requires a loopback listener");
      }
      RuntimeConfiguration.TableGroup group = tableGroups.get(listener.tableGroupId());
      if (group == null) {
        throw invalid(path + ".table_group_id", "references an unknown table group");
      }
      RuntimeConfiguration.HBaseProfile hbaseProfile = hbaseProfiles.get(group.hbaseProfileId());
      if (hbaseProfile.authenticationMode() == AuthenticationMode.DEV_NO_AUTH
          && !isLoopbackHost(host)) {
        throw invalid(path + ".bind_host", "DEV_NO_AUTH requires a loopback listener");
      }

      ListenerAddress address = new ListenerAddress(host, listener.port(), isWildcardHost(host));
      for (ListenerAddress existing : addresses) {
        if (address.port() != 0
            && existing.port() != 0
            && existing.port() == address.port()
            && (existing.wildcard() || address.wildcard() || existing.host().equals(address.host()))) {
          throw invalid(path + ".port", "conflicts with another listener address");
        }
      }
      addresses.add(address);
    }
  }

  private void validateAuthentication(RuntimeConfiguration.HBaseProfile profile, String path) {
    if (profile.authenticationMode() == null) {
      throw invalid(path + ".authentication_mode", "is required");
    }
    String reference = profile.credentialReference();
    if (profile.authenticationMode() == AuthenticationMode.DEV_NO_AUTH) {
      if (reference != null) {
        throw invalid(path + ".credential_reference", "must be omitted for DEV_NO_AUTH");
      }
      return;
    }
    if (reference == null
        || !(ENV_REFERENCE_PATTERN.matcher(reference).matches()
            || SECRET_REFERENCE_PATTERN.matcher(reference).matches())
        || reference.contains("../")
        || reference.endsWith("/..")) {
      throw invalid(path + ".credential_reference", "must be a controlled env:// or secret:// reference");
    }
  }

  private static void validateResources(RuntimeConfiguration.ResourceBudgets resources, int actualBytes) {
    if (resources == null) {
      throw invalid("resources", "is required");
    }
    if (resources.maxConfigBytes() < actualBytes
        || resources.maxConfigBytes() > RuntimeConfigurationLoader.ABSOLUTE_MAX_CONFIG_BYTES) {
      throw invalid(
          "resources.max_config_bytes", "must cover the effective configuration and not exceed 65536 bytes");
    }
    if (resources.maxConcurrentRequests() < 1
        || resources.maxConcurrentRequests() > MAX_CONCURRENT_REQUESTS) {
      throw invalid("resources.max_concurrent_requests", "must be between 1 and 100000 requests");
    }
    if (resources.maxScannersPerTable() < 1
        || resources.maxScannersPerTable() > MAX_SCANNERS_PER_TABLE) {
      throw invalid("resources.max_scanners_per_table", "must be between 1 and 1024 scanners per table");
    }
    if (resources.requestDeadlineMs() < 1
        || resources.requestDeadlineMs() > MAX_REQUEST_DEADLINE_MILLIS) {
      throw invalid("resources.request_deadline_ms", "must be between 1 and 600000 milliseconds");
    }
    if (resources.maxRpcBytes() < 1 || resources.maxRpcBytes() > MAX_RPC_BYTES) {
      throw invalid("resources.max_rpc_bytes", "must be between 1 and 67108864 bytes");
    }
    if (resources.maxRequestBytes() < 1 || resources.maxRequestBytes() > MAX_REQUEST_BYTES) {
      throw invalid("resources.max_request_bytes", "must be between 1 and 67108864 bytes");
    }
    if (resources.maxRpcBytes() > resources.maxRequestBytes()) {
      throw invalid("resources.max_rpc_bytes", "must not exceed max_request_bytes");
    }
    if (resources.maxQueuedRequests() < 1 || resources.maxQueuedRequests() > MAX_QUEUED_REQUESTS) {
      throw invalid("resources.max_queued_requests", "must be between 1 and 100000 requests");
    }
  }

  private static void requireSupportedId(String value, Set<String> supported, String path) {
    validateId(value, path);
    if (supported != null && !supported.contains(value)) {
      throw invalid(path, "does not name a registered implementation");
    }
  }

  private static void validateId(String value, String path) {
    if (value == null || !ID_PATTERN.matcher(value).matches()) {
      throw invalid(path, "must be a nonempty identifier");
    }
  }

  private static void requirePort(int port, String path) {
    if (port < 1 || port > 65_535) {
      throw invalid(path, "must be between 1 and 65535");
    }
  }

  private static void requireListenerPort(int port, String path) {
    if (port < 0 || port > 65_535) {
      throw invalid(path, "must be between 0 and 65535");
    }
  }

  private static void requireText(String value, String path) {
    if (value == null || value.isBlank()) {
      throw invalid(path, "is required");
    }
  }

  private static String normalizeHost(String host, String path) {
    requireText(host, path);
    if (host.length() > 253 || containsWhitespaceOrControl(host)) {
      throw invalid(path, "must be a bounded host name without whitespace");
    }
    return host.toLowerCase(Locale.ROOT);
  }

  private static boolean containsWhitespaceOrControl(String value) {
    return value.chars()
        .anyMatch(character -> Character.isWhitespace(character) || Character.isISOControl(character));
  }

  private static boolean isWildcardHost(String host) {
    return host.equals("*") || host.equals("0.0.0.0") || host.equals("::") || host.equals("[::]");
  }

  private static boolean isLoopbackHost(String host) {
    return host.equals("localhost")
        || host.equals("::1")
        || host.equals("[::1]")
        || isIpv4Loopback(host);
  }

  private static boolean isIpv4Loopback(String host) {
    String[] octets = host.split("\\.", -1);
    if (octets.length != 4 || !"127".equals(octets[0])) {
      return false;
    }
    for (String octet : octets) {
      if (octet.isEmpty() || octet.length() > 3) {
        return false;
      }
      for (int index = 0; index < octet.length(); index++) {
        if (!Character.isDigit(octet.charAt(index))) {
          return false;
        }
      }
      if (Integer.parseInt(octet) > 255) {
        return false;
      }
    }
    return true;
  }

  private static byte[] decodeHex(String value) {
    byte[] decoded = new byte[value.length() / 2];
    for (int index = 0; index < decoded.length; index++) {
      int high = Character.digit(value.charAt(index * 2), 16);
      int low = Character.digit(value.charAt(index * 2 + 1), 16);
      decoded[index] = (byte) ((high << 4) | low);
    }
    return decoded;
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    for (int index = 0; index < Math.min(left.length, right.length); index++) {
      int comparison = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static RuntimeConfigurationException invalid(String path, String problem) {
    return new RuntimeConfigurationException(path + " " + problem);
  }

  private record ListenerAddress(String host, int port, boolean wildcard) {}
}
