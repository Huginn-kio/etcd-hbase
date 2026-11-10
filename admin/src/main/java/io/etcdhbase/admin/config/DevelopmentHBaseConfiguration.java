package io.etcdhbase.admin.config;

import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/** Builds the explicit loopback-only SIMPLE profile used by local initialization and runtime. */
public final class DevelopmentHBaseConfiguration {
  private DevelopmentHBaseConfiguration() {}

  public static Configuration create(HBaseProfile profile) {
    Objects.requireNonNull(profile, "profile");
    if (profile.authenticationMode() != AuthenticationMode.DEV_NO_AUTH
        || !HBaseBackendScope.of(profile).isLoopbackOnly()) {
      throw new IllegalArgumentException("development HBase requires an explicit loopback profile");
    }
    var configuration = HBaseConfiguration.create();
    String quorum = HBaseQuorum.connectionString(profile);
    configuration.set("hbase.zookeeper.quorum", quorum);
    configuration.set("hbase.client.zookeeper.quorum", quorum);
    configuration.setInt("hbase.zookeeper.property.clientPort", profile.clientPort());
    configuration.setInt("hbase.client.zookeeper.property.clientPort", profile.clientPort());
    configuration.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.ZKConnectionRegistry");
    configuration.set("hbase.security.authentication", "simple");
    configuration.set("hadoop.security.authentication", "simple");
    configuration.setBoolean("hbase.security.authorization", false);
    return configuration;
  }
}
