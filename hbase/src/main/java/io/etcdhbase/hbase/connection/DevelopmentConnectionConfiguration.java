package io.etcdhbase.hbase.connection;

import org.apache.hadoop.conf.Configuration;

/** Restricts the process-identity shortcut to an explicitly local, unauthenticated ZK profile. */
final class DevelopmentConnectionConfiguration {
  private DevelopmentConnectionConfiguration() {}

  static void validate(Configuration configuration) {
    if (!"simple".equals(configuration.get("hbase.security.authentication", "simple"))
        || !"simple".equals(configuration.get("hadoop.security.authentication", "simple"))
        || configuration.getBoolean("hbase.security.authorization", false)) {
      throw new IllegalArgumentException(
          "development connections require simple local authentication");
    }
    String registry = configuration.get("hbase.client.registry.impl", "");
    if (!registry.isEmpty()
        && !registry.equals("org.apache.hadoop.hbase.client.ZKConnectionRegistry")) {
      throw new IllegalArgumentException("development connections require a ZooKeeper registry");
    }
    configuration.setIfUnset(
        "hbase.client.registry.impl", "org.apache.hadoop.hbase.client.ZKConnectionRegistry");
    for (String key : new String[] {"hbase.zookeeper.quorum", "hbase.client.zookeeper.quorum"}) {
      String endpoints = configuration.get(key);
      if (endpoints == null) {
        continue;
      }
      for (String endpoint : endpoints.split(",", -1)) {
        if (!isLoopback(endpoint.strip())) {
          throw new IllegalArgumentException(
              "development ZooKeeper endpoints must be loopback literals");
        }
      }
    }
  }

  private static boolean isLoopback(String endpoint) {
    if (endpoint.equals("::1") || endpoint.equals("[::1]")) {
      return true;
    }
    String host = endpoint;
    if (endpoint.startsWith("[::1]:")) {
      host = "::1";
    } else if (endpoint.indexOf(':') >= 0) {
      host = endpoint.substring(0, endpoint.indexOf(':'));
    }
    if (host.equals("localhost") || host.equals("::1")) {
      return true;
    }
    String[] octets = host.split("\\.", -1);
    if (octets.length != 4 || !octets[0].equals("127")) {
      return false;
    }
    for (String octet : octets) {
      if (!octet.matches("[0-9]{1,3}") || Integer.parseInt(octet) > 255) {
        return false;
      }
    }
    return true;
  }
}
