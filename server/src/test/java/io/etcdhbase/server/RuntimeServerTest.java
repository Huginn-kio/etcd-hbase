package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    assertFalse(server.dataReady());
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
    try (var held =
        new ServerSocket(0, 1, java.net.InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))) {
      int firstPort;
      try (var reservation =
          new ServerSocket(0, 1, java.net.InetAddress.getByAddress(new byte[] {127, 0, 0, 1}))) {
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
    var activation = new M3RuntimeConfiguration("1", 6000, 1000, 30000, 30000,
        512L << 20, 1_048_576, 2, 16, 60000);
    assertThrows(IllegalArgumentException.class, () -> RuntimeServer.startM3(configuration, activation));
    profile.put("credential_reference", "cleartext-password");
    assertThrows(IllegalArgumentException.class, () -> load(tree));
  }

  @Test
  void rejectsCacheActivationBeforeStartingTheBackend(@TempDir Path directory) throws Exception {
    var activation = (ObjectNode) mapper.readTree(Files.readAllBytes(Path.of("../config/examples/m3/activation.json")));
    activation.put("read_cache_enabled", true);
    Path activationFile = directory.resolve("activation.json");
    Files.write(activationFile, mapper.writeValueAsBytes(activation));
    assertEquals(2, RuntimeServer.run(new String[] {"--config", "../config/examples/m3/server.json",
        "--m3-config", activationFile.toString()}));
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
