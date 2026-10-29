package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.EndpointHost;

/** Listener literals use the same host parser and transport normalization as HBase endpoints. */
public final class ListenerAddress {
  private ListenerAddress() {}

  static boolean overlaps(EndpointHost left, EndpointHost right) {
    return left.connectionName().equals(right.connectionName())
        || (left.connectionName().equals("localhost") && right.isLoopback())
        || (right.connectionName().equals("localhost") && left.isLoopback());
  }

  public static EndpointHost parse(String input) {
    if (input == null || input.isBlank() || !input.equals(input.strip())) {
      throw new IllegalArgumentException("listener host must be an explicit host or IP literal");
    }
    String host = input;
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
      if (!host.contains(":")) {
        throw new IllegalArgumentException("brackets require an IPv6 literal");
      }
    }
    return EndpointHost.parse(host);
  }
}
