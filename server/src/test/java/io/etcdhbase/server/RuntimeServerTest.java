package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RuntimeServerTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void startsMultipleLoopbackAliasesAndClosesIdempotently() throws Exception {
    var tree = example();
    var listeners = (ArrayNode) tree.path("listeners");
    listeners.add(
        ((ObjectNode) listeners.get(0))
            .deepCopy()
            .put("id", "second")
            .put("bind_host", "LOCALHOST."));
    var server = RuntimeServer.start(load(tree));
    var bound = server.boundListeners();
    assertEquals(2, bound.size());
    server.close();
    server.close();
    for (var listener : bound) {
      try (var rebound = new ServerSocket()) {
        rebound.bind(new InetSocketAddress(listener.bindHost(), listener.port()));
      }
    }
  }

  @Test
  void rollsBackAnEarlierListenerIfALaterBindFails() throws Exception {
    try (var held = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
      int firstPort;
      try (var reservation = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
        firstPort = reservation.getLocalPort();
      }
      var tree = example();
      var listeners = (ArrayNode) tree.path("listeners");
      ((ObjectNode) listeners.get(0)).put("port", firstPort);
      listeners.add(
          ((ObjectNode) listeners.get(0))
              .deepCopy()
              .put("id", "busy")
              .put("port", held.getLocalPort()));
      assertThrows(IOException.class, () -> RuntimeServer.start(load(tree)));
      try (var rebound = new ServerSocket()) {
        rebound.bind(new InetSocketAddress("127.0.0.1", firstPort));
      }
    }
  }

  @Test
  void validatesCredentialReferencesWithoutResolvingOrActivatingThem() throws Exception {
    var tree = example();
    var profile = (ObjectNode) tree.path("hbase_profiles").path(0);
    profile.put("authentication_mode", "CREDENTIAL_REFERENCE");
    profile.put("credential_reference", "env://UNRESOLVED_TEST_REFERENCE");
    ((ObjectNode) tree.path("table_groups").path(0)).put("identity_provider_id", "custom-provider");
    var configuration = load(tree);
    assertEquals(
        "env://UNRESOLVED_TEST_REFERENCE",
        configuration.hbaseProfiles().getFirst().credentialReference());
    assertThrows(IllegalArgumentException.class, () -> RuntimeServer.start(configuration));
    profile.put("credential_reference", "cleartext-password");
    assertThrows(IllegalArgumentException.class, () -> load(tree));
  }

  private ObjectNode example() throws Exception {
    return (ObjectNode)
        mapper.readTree(Files.readAllBytes(Path.of("../config/examples/j06/server.json")));
  }

  private RuntimeConfiguration load(ObjectNode tree) throws Exception {
    return new RuntimeConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)), Map.of());
  }
}
