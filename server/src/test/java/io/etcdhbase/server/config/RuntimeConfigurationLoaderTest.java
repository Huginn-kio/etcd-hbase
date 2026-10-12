package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.server.config.RuntimeConfiguration.AuthenticationMode;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationLoaderTest {
  private static final String VALID_YAML =
      """
      schema_version: 1
      listeners:
        - id: local-etcd
          bind_host: 127.0.0.1
          port: 2379
          table_group_id: local
      hbase_profiles:
        - id: local-hbase
          quorum: 127.0.0.1
          client_port: 2181
          namespace: etcd_candidate
          authentication_mode: DEV_NO_AUTH
      salt_profiles:
        - id: crc32-four-buckets
          strategy_id: crc32-bucket-v1
          encoding_version: 1
          bucket_count: 4
          salt_width_bytes: 1
          split_keys_hex: ["01", "02", "03"]
      table_groups:
        - id: local
          schema_version: 1
          hbase_profile_id: local-hbase
          salt_profile_id: crc32-four-buckets
          identity_provider_id: dev-no-auth
          history_layout: UNDECIDED
      resources:
        max_config_bytes: 65536
        max_concurrent_requests: 100
        max_scanners_per_table: 32
        max_request_bytes: 4194304
      """;

  private final RuntimeConfigurationLoader loader =
      new RuntimeConfigurationLoader(Set.of("dev-no-auth", "backend-hbase"), Set.of("crc32-bucket-v1"));

  @Test
  void loadsCandidateAndRetainsUnresolvedHistoryLayout() {
    RuntimeConfiguration configuration = loader.load(bytes(VALID_YAML));

    assertEquals(1, configuration.schemaVersion());
    assertEquals("127.0.0.1", configuration.listeners().getFirst().bindHost());
    assertEquals(
        AuthenticationMode.DEV_NO_AUTH,
        configuration.hbaseProfiles().getFirst().authenticationMode());
    assertEquals(HistoryLayout.UNDECIDED, configuration.tableGroups().getFirst().historyLayout());
    assertEquals(4, configuration.saltProfiles().getFirst().bucketCount());
    assertEquals(65536, configuration.resources().maxConfigBytes());
    assertThrows(
        UnsupportedOperationException.class,
        () -> configuration.listeners().add(configuration.listeners().getFirst()));
  }

  @Test
  void loadsTheCheckedInCandidateProfile() throws IOException {
    try (var input = getClass().getResourceAsStream("/runtime-candidate.yaml")) {
      assertNotNull(input);
      assertEquals("local", loader.load(input).tableGroups().getFirst().id());
    }
  }

  @Test
  void acceptsControlledCredentialReferenceAndRequiresAuthenticationOutsideLoopback() {
    String securedYaml =
        VALID_YAML
            .replace("127.0.0.1", "service.example")
            .replace(
                "authentication_mode: DEV_NO_AUTH",
                "authentication_mode: CREDENTIAL_REFERENCE\n"
                    + "    credential_reference: env://HBASE_CREDENTIALS");

    RuntimeConfiguration configuration = loader.load(bytes(securedYaml));

    assertEquals("env://HBASE_CREDENTIALS", configuration.hbaseProfiles().getFirst().credentialReference());
  }

  @Test
  void rejectsInvalidPortWithFieldPath() {
    assertRejected(VALID_YAML.replace("port: 2379", "port: 0"), "listeners[0].port", null);
  }

  @Test
  void rejectsDuplicateListenerAddress() {
    String duplicate =
        VALID_YAML.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - id: second-listener\n"
                + "    bind_host: 127.0.0.1\n"
                + "    port: 2379\n"
                + "    table_group_id: local\n"
                + "hbase_profiles:");

    assertRejected(duplicate, "listeners[1].port", null);
  }

  @Test
  void rejectsWildcardListenerConflictOnTheSamePort() {
    String secured =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: env://HBASE_CREDENTIALS");
    String wildcardConflict =
        secured.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - id: wildcard-listener\n"
                + "    bind_host: 0.0.0.0\n"
                + "    port: 2379\n"
                + "    table_group_id: local\n"
                + "hbase_profiles:");

    assertRejected(wildcardConflict, "listeners[1].port", null);
  }

  @Test
  void rejectsUnknownReferencesAndProviderIds() {
    assertRejected(
        VALID_YAML.replace("table_group_id: local", "table_group_id: missing"),
        "listeners[0].table_group_id",
        null);
    assertRejected(
        VALID_YAML.replace("identity_provider_id: dev-no-auth", "identity_provider_id: unknown-provider"),
        "identity_provider_id",
        null);
    assertRejected(
        VALID_YAML.replace("strategy_id: crc32-bucket-v1", "strategy_id: unknown-salt"),
        "strategy_id",
        null);
  }

  @Test
  void rejectsUnknownSchemaVersionsAndScalarCoercion() {
    assertRejected(VALID_YAML.replace("schema_version: 1", "schema_version: 2"), "schema_version", null);
    assertRejected(VALID_YAML.replace("port: 2379", "port: \"2379\""), "listeners[0].port", null);
  }

  @Test
  void rejectsUnsortedAndIncorrectCrcSplitKeys() {
    assertRejected(
        VALID_YAML.replace("[\"01\", \"02\", \"03\"]", "[\"02\", \"01\", \"03\"]"),
        "split_keys_hex[1]",
        null);
    assertRejected(
        VALID_YAML.replace("[\"01\", \"02\", \"03\"]", "[\"01\", \"02\", \"04\"]"),
        "split_keys_hex[2]",
        null);
  }

  @Test
  void rejectsEmbeddedSecretAndDoesNotEchoIt() {
    String sentinel = "do-not-disclose-this-secret-value";
    RuntimeConfigurationException exception =
        assertThrows(
            RuntimeConfigurationException.class,
            () -> loader.load(bytes(VALID_YAML + "\nunknown_password: " + sentinel + "\n")));

    assertFalse(exception.getMessage().contains(sentinel));
    assertTrue(exception.getMessage().contains("unknown_password"), exception.getMessage());
    assertNull(exception.getCause());
  }

  @Test
  void rejectsCredentialValueInsteadOfControlledReferenceWithoutEchoingIt() {
    String sentinel = "inline-secret-value";
    String secretYaml =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: "
                + sentinel);
    RuntimeConfigurationException exception =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(secretYaml)));

    assertTrue(exception.getMessage().contains("credential_reference"));
    assertFalse(exception.getMessage().contains(sentinel));
    assertNull(exception.getCause());
  }

  @Test
  void rejectsDuplicateKeysCollectionAliasesAndMultipleDocumentsWithoutParserDetails() {
    String sentinel = "duplicate-secret-marker";
    String duplicate =
        VALID_YAML.replace(
            "schema_version: 1", "schema_version: 1\nschema_version: " + sentinel);
    RuntimeConfigurationException duplicateError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(duplicate)));
    assertFalse(duplicateError.getMessage().contains(sentinel));
    assertNull(duplicateError.getCause());

    assertRejected(VALID_YAML + "\n---\nschema_version: 1\n", "one YAML document", null);
    String anchored = VALID_YAML.replace("  - id: local-etcd", "  - &listener\n    id: local-etcd");
    String withAlias =
        anchored.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - *listener\n"
                + "hbase_profiles:");
    assertRejected(withAlias, "YAML", null);
  }

  @Test
  void rejectsInvalidBudgetsAndNonLoopbackNoAuth() {
    assertRejected(
        VALID_YAML.replace("max_scanners_per_table: 32", "max_scanners_per_table: 1025"),
        "resources.max_scanners_per_table",
        null);
    assertRejected(
        VALID_YAML.replace("127.0.0.1", "0.0.0.0"),
        "DEV_NO_AUTH requires a loopback listener",
        null);
    assertRejected(
        VALID_YAML.replace("127.0.0.1", "127.untrusted"),
        "DEV_NO_AUTH requires a loopback listener",
        null);
  }

  @Test
  void enforcesByteLimitUtf8AndDoesNotCloseCallerStream() {
    byte[] tooLarge = new byte[RuntimeConfigurationLoader.ABSOLUTE_MAX_CONFIG_BYTES + 1];
    RuntimeConfigurationException sizeError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(tooLarge));
    assertTrue(sizeError.getMessage().contains("65536-byte limit"));
    assertThrows(
        RuntimeConfigurationException.class,
        () -> loader.load(new ByteArrayInputStream(tooLarge)));

    RuntimeConfigurationException encodingError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(new byte[] {(byte) 0xC3, 0x28}));
    assertTrue(encodingError.getMessage().contains("UTF-8"));

    TrackingInputStream input = new TrackingInputStream(bytes(VALID_YAML));
    assertNotNull(loader.load(input));
    assertFalse(input.closed);
  }

  private void assertRejected(String yaml, String messagePart, String forbidden) {
    RuntimeConfigurationException exception =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(yaml)));
    assertTrue(exception.getMessage().contains(messagePart), exception.getMessage());
    assertNull(exception.getCause());
    if (forbidden != null) {
      assertFalse(exception.getMessage().contains(forbidden));
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static final class TrackingInputStream extends ByteArrayInputStream {
    private boolean closed;

    private TrackingInputStream(byte[] content) {
      super(content);
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
