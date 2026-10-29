package io.etcdhbase.hbase.connection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class DevelopmentConnectionConfigurationTest {
  @Test
  void acceptsLoopbackWithoutDnsAndRejectsRemoteOrAuthenticatedProfiles() {
    var configuration = new Configuration(false);
    configuration.set("hbase.zookeeper.quorum", "localhost,127.0.0.1:2181,[::1]:2181");
    assertDoesNotThrow(() -> DevelopmentConnectionConfiguration.validate(configuration));
    for (String endpoint :
        new String[] {
          "example.invalid", "0.0.0.0", "127.0.0.999", "::", "127.0.0.1,remote.invalid"
        }) {
      configuration.set("hbase.zookeeper.quorum", endpoint);
      assertThrows(
          IllegalArgumentException.class,
          () -> DevelopmentConnectionConfiguration.validate(configuration));
    }
    configuration.set("hbase.zookeeper.quorum", "localhost");
    configuration.set("hbase.security.authentication", "kerberos");
    assertThrows(
        IllegalArgumentException.class,
        () -> DevelopmentConnectionConfiguration.validate(configuration));
    configuration.set("hbase.security.authentication", "simple");
    configuration.setBoolean("hbase.security.authorization", true);
    assertThrows(
        IllegalArgumentException.class,
        () -> DevelopmentConnectionConfiguration.validate(configuration));
  }
}
