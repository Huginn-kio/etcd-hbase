package io.etcdhbase.server.config;

import io.etcdhbase.server.config.RuntimeConfiguration.AuthenticationMode;
import io.etcdhbase.spi.salt.Crc32BucketSaltStrategy;
import io.etcdhbase.spi.salt.SaltParameters;
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
  private static final long MAX_REQUEST_BYTES = 64L * 1024 * 1024;
  private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9._-]{0,63}");
  private static final Pattern ENV_REFERENCE_PATTERN =
      Pattern.compile("env://[A-Za-z_][A-Za-z0-9_]*");
  private static final Pattern SECRET_REFERENCE_PATTERN =
      Pattern.compile("secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,127}");
  private static final Pattern HEX_PATTERN = Pattern.compile("[0-9A-Fa-f]+");

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
        new SaltParameters(profile.encodingVersion(), profile.bucketCount(), profile.saltWidthBytes());
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

    if (Crc32BucketSaltStrategy.STRATEGY_ID.equals(profile.strategyId())) {
      if (profile.encodingVersion() != 1) {
        throw invalid(path + ".encoding_version", "crc32-bucket-v1 requires version 1");
      }
      if ((long) profile.bucketCount() - 1 != splitKeys.size()) {
        throw invalid(path + ".split_keys_hex", "must contain one boundary for each nonzero bucket");
      }
      SaltParameters parameters =
          new SaltParameters(profile.encodingVersion(), profile.bucketCount(), profile.saltWidthBytes());
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
    }
    return byId;
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
      requirePort(listener.port(), path + ".port");
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
        if (existing.port() == address.port()
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
      throw invalid("resources.max_config_bytes", "must cover this file and not exceed 65536");
    }
    if (resources.maxConcurrentRequests() < 1
        || resources.maxConcurrentRequests() > MAX_CONCURRENT_REQUESTS) {
      throw invalid("resources.max_concurrent_requests", "must be between 1 and 100000");
    }
    if (resources.maxScannersPerTable() < 1
        || resources.maxScannersPerTable() > MAX_SCANNERS_PER_TABLE) {
      throw invalid("resources.max_scanners_per_table", "must be between 1 and 1024");
    }
    if (resources.maxRequestBytes() < 1 || resources.maxRequestBytes() > MAX_REQUEST_BYTES) {
      throw invalid("resources.max_request_bytes", "must be between 1 and 67108864");
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
