package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.EndpointHost;

/**
 * Listener literals use the same host parser and conservative alias identity as HBase endpoints.
 */
public final class ListenerAddress {
  private ListenerAddress() {}

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
