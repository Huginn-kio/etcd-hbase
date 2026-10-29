package io.etcdhbase.admin.config;

import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfiguration.ColumnFamilySchema;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayoutCandidate;
import io.etcdhbase.admin.config.InitializationConfiguration.IdentityPermission;
import io.etcdhbase.admin.config.InitializationConfiguration.RecordCategory;
import io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.TableGroup;
import io.etcdhbase.admin.config.InitializationConfiguration.TablePermission;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Validates only the init candidate and its physical table scope, without contacting HBase. */
public final class InitializationConfigurationValidator {
  private static final int MAX_PRE_SPLIT_REGIONS = 1024;
  private static final Set<RecordCategory> SYSTEM_CATEGORIES =
      EnumSet.of(
          RecordCategory.DECISION,
          RecordCategory.EVENT,
          RecordCategory.LEASE,
          RecordCategory.AUTH,
          RecordCategory.CONTROL);

  private InitializationConfigurationValidator() {}

  public static void validate(InitializationConfiguration configuration) {
    require(configuration.schemaVersion() == 1, "unsupported initialization schema version");
    Map<String, HBaseProfile> profiles = index(configuration.hbaseProfiles(), HBaseProfile::id);
    Map<String, SaltProfile> salts = index(configuration.saltProfiles(), SaltProfile::id);
    Map<String, TableGroup> groups = index(configuration.tableGroups(), TableGroup::id);
    profiles.values().forEach(InitializationConfigurationValidator::validateBackend);
    salts.values().forEach(InitializationConfigurationValidator::validateSalt);
    Map<HBaseBackendScope, Set<String>> tablesByScope = new HashMap<>();
    for (TableGroup group : groups.values()) {
      require(group.schemaVersion() == 1, "unsupported table-group schema version");
      id(group.identityProviderId());
      HBaseProfile profile = profiles.get(group.hbaseProfileId());
      SaltProfile salt = salts.get(group.saltProfileId());
      require(
          profile != null && salt != null,
          "table group references an unknown backend or salt profile");
      require(group.historyLayout() != null, "history layout is required");
      if (profile.authenticationMode() == AuthenticationMode.DEV_NO_AUTH) {
        require(
            group.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID),
            "development profile requires the explicit dev-no-auth provider");
      }
      Set<String> groupTables = validateGroupTables(group, salt);
      Set<String> occupied =
          tablesByScope.computeIfAbsent(HBaseBackendScope.of(profile), ignored -> new HashSet<>());
      require(
          groupTables.stream().noneMatch(occupied::contains),
          "table groups overlap in a physical backend namespace");
      occupied.addAll(groupTables);
    }
  }

  private static void validateBackend(HBaseProfile profile) {
    require(
        profile.namespace() != null
            && profile.namespace().matches("[A-Za-z_][A-Za-z0-9_]{0,63}")
            && !Set.of("default", "hbase").contains(profile.namespace()),
        "initialization requires a dedicated non-system namespace");
    require(
        profile.clientPort() >= 1 && profile.clientPort() <= 65535,
        "backend client port must be between 1 and 65535");
    require(profile.authenticationMode() != null, "backend authentication mode is required");
    HBaseBackendScope scope = HBaseBackendScope.of(profile);
    if (profile.authenticationMode() == AuthenticationMode.DEV_NO_AUTH) {
      require(scope.isLoopbackOnly(), "development initialization requires loopback endpoints");
      require(
          profile.credentialReference() == null,
          "development initialization must not declare credentials");
    } else {
      String reference = profile.credentialReference();
      require(
          reference != null
              && reference.length() <= 256
              && (reference.matches("env://[A-Z][A-Z0-9_]{0,127}")
                  || reference.matches("secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,240}")),
          "backend credentials require a bounded env:// or secret:// reference");
    }
  }

  private static void validateSalt(SaltProfile profile) {
    require(
        profile.bucketCount() <= MAX_PRE_SPLIT_REGIONS,
        "initialization pre-split region budget exceeded");
    HashSaltStrategy strategy;
    try {
      strategy =
          new HashSaltStrategy(
              new io.etcdhbase.spi.salt.SaltProfile(
                  profile.strategyId(),
                  profile.encodingVersion(),
                  profile.bucketCount(),
                  profile.saltWidthBytes()));
    } catch (IllegalArgumentException failure) {
      throw new IllegalArgumentException("unsupported or invalid initialization salt profile");
    }
    List<byte[]> expected = strategy.splitKeys();
    require(
        profile.splitKeysHex().size() == expected.size(),
        "salt profile requires one split boundary per nonzero bucket");
    for (int index = 0; index < expected.size(); index++) {
      require(
          Arrays.equals(expected.get(index), decodeSplit(profile.splitKeysHex().get(index))),
          "split boundary does not match the salt bucket encoding");
    }
  }

  private static Set<String> validateGroupTables(TableGroup group, SaltProfile salt) {
    validateTable(group.systemTable(), salt);
    require(
        categorySet(group.systemTable().recordCategories()).equals(SYSTEM_CATEGORIES),
        "system table must cover the system record categories");
    require(group.historyCandidates().size() == 2, "both history layout candidates are required");
    Set<HistoryLayout> layouts = EnumSet.noneOf(HistoryLayout.class);
    Set<String> tables = new HashSet<>();
    tables.add(group.systemTable().tableName());
    for (HistoryLayoutCandidate candidate : group.historyCandidates()) {
      require(
          candidate.historyLayout() != null
              && candidate.historyLayout() != HistoryLayout.UNDECIDED
              && layouts.add(candidate.historyLayout()),
          "history candidates must be distinct concrete layouts");
      validateHistoryCandidate(candidate, salt);
      for (TableSchemaCandidate table : candidate.tables()) {
        require(
            !table.tableName().equals(group.systemTable().tableName()),
            "system and history tables must be distinct");
        tables.add(table.tableName());
      }
    }
    return tables;
  }

  private static void validateHistoryCandidate(HistoryLayoutCandidate candidate, SaltProfile salt) {
    int expectedTables = candidate.historyLayout() == HistoryLayout.SEPARATE_HISTORY_TABLE ? 2 : 1;
    require(
        candidate.tables().size() == expectedTables,
        "history candidate has an invalid table count");
    Set<String> names = new HashSet<>();
    Set<RecordCategory> combined = EnumSet.noneOf(RecordCategory.class);
    for (TableSchemaCandidate table : candidate.tables()) {
      validateTable(table, salt);
      require(names.add(table.tableName()), "history candidate table names must be unique");
      Set<RecordCategory> categories = categorySet(table.recordCategories());
      require(
          Set.of(RecordCategory.CURRENT, RecordCategory.HISTORY).containsAll(categories),
          "history candidate contains non-history record categories");
      if (candidate.historyLayout() == HistoryLayout.SEPARATE_HISTORY_TABLE) {
        require(categories.size() == 1, "separate history tables require one category each");
      }
      combined.addAll(categories);
    }
    require(
        combined.equals(Set.of(RecordCategory.CURRENT, RecordCategory.HISTORY)),
        "history candidate must cover current and history records");
  }

  private static void validateTable(TableSchemaCandidate table, SaltProfile salt) {
    require(table != null, "table schema candidate is required");
    require(
        table.tableName() != null && table.tableName().matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}"),
        "table name must be a bounded unqualified name");
    require(
        table.schemaVersion() == 1 && table.profileVersion() > 0,
        "unsupported table schema or profile version");
    validateColumnFamilies(table.columnFamilies());
    validateSplits(table.splitKeysHex(), salt.splitKeysHex());
    categorySet(table.recordCategories());
    validateIdentityPermissions(table.identityPermissions());
  }

  private static void validateColumnFamilies(List<ColumnFamilySchema> familiesToValidate) {
    require(
        !familiesToValidate.isEmpty() && familiesToValidate.size() <= 8,
        "candidate must declare between one and eight column families");
    Set<String> families = new HashSet<>();
    for (ColumnFamilySchema family : familiesToValidate) {
      require(
          family.name() != null
              && family.name().matches("[A-Za-z][A-Za-z0-9_-]{0,31}")
              && families.add(family.name()),
          "column-family names must be bounded and distinct");
      require(
          family.minVersions() >= 0
              && family.maxVersions() >= 1
              && family.maxVersions() <= 1024
              && family.minVersions() <= family.maxVersions(),
          "invalid candidate column-family version bounds");
      require(
          family.ttlSeconds() == -1
              || (family.ttlSeconds() > 0 && family.ttlSeconds() < Integer.MAX_VALUE),
          "TTL must be -1 or a finite positive value below the HBase unlimited sentinel");
      require(family.keepDeletedCells() != null, "keep-deleted-cells mode is required");
    }
  }

  private static void validateSplits(List<String> actual, List<String> expected) {
    require(actual.size() == expected.size(), "table split count differs from its salt profile");
    for (int index = 0; index < actual.size(); index++) {
      require(
          Arrays.equals(decodeSplit(actual.get(index)), decodeSplit(expected.get(index))),
          "table splits differ from its salt profile");
    }
  }

  private static void validateIdentityPermissions(List<IdentityPermission> permissionsToValidate) {
    require(
        !permissionsToValidate.isEmpty() && permissionsToValidate.size() <= 16,
        "candidate permission intents must be declared and bounded");
    Set<String> roles = new HashSet<>();
    for (IdentityPermission permission : permissionsToValidate) {
      id(permission.identityRef());
      require(roles.add(permission.identityRef()), "permission role references must be unique");
      require(
          !permission.permissions().isEmpty()
              && permission.permissions().size() <= TablePermission.values().length
              && permission.permissions().stream().allMatch(Objects::nonNull)
              && new HashSet<>(permission.permissions()).size() == permission.permissions().size(),
          "permission intents must be nonempty finite sets");
    }
  }

  private static Set<RecordCategory> categorySet(List<RecordCategory> values) {
    require(
        !values.isEmpty()
            && values.size() <= RecordCategory.values().length
            && values.stream().allMatch(Objects::nonNull)
            && new HashSet<>(values).size() == values.size(),
        "record categories must be a nonempty finite set");
    return Set.copyOf(values);
  }

  private static byte[] decodeSplit(String text) {
    require(
        text != null && text.length() <= 8 && text.matches("(?:[0-9a-fA-F]{2}){1,4}"),
        "split boundaries must be bounded hexadecimal bytes");
    return HexFormat.of().parseHex(text);
  }

  private static <T> Map<String, T> index(List<T> values, Function<T, String> identifier) {
    require(
        !values.isEmpty() && values.size() <= 32,
        "configuration collections must contain one to thirty-two entries");
    Map<String, T> indexed = new LinkedHashMap<>();
    for (T value : values) {
      require(value != null, "configuration entries must not be null");
      String name = identifier.apply(value);
      id(name);
      require(indexed.putIfAbsent(name, value) == null, "configuration identifiers must be unique");
    }
    return indexed;
  }

  private static void id(String text) {
    require(
        text != null && text.matches("[A-Za-z][A-Za-z0-9._-]{0,63}"),
        "configuration identifiers must be bounded names");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
