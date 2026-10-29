package io.etcdhbase.admin.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

/** Physical namespace identity used only to protect configured sibling groups during init. */
public record HBaseBackendScope(List<String> endpoints, String namespace) {
  public HBaseBackendScope {
    endpoints = List.copyOf(endpoints);
  }

  public static HBaseBackendScope of(InitializationConfiguration.HBaseProfile profile) {
    if (profile.quorum() == null
        || profile.quorum().isBlank()
        || profile.quorum().length() > 4096) {
      throw new IllegalArgumentException("quorum must contain bounded explicit endpoints");
    }
    TreeSet<String> endpoints = new TreeSet<>();
    for (String value : profile.quorum().split(",", -1)) {
      String endpoint = value.strip();
      String host = endpoint;
      int port = profile.clientPort();
      if (endpoint.startsWith("[")) {
        int closing = endpoint.indexOf(']');
        if (closing < 0) {
          throw new IllegalArgumentException("invalid bracketed quorum endpoint");
        }
        host = endpoint.substring(1, closing);
        String suffix = endpoint.substring(closing + 1);
        if (!suffix.isEmpty()) {
          if (!suffix.startsWith(":")) {
            throw new IllegalArgumentException("invalid quorum endpoint port");
          }
          port = parsePort(suffix.substring(1));
        }
      } else if (endpoint.indexOf(':') >= 0 && endpoint.indexOf(':') == endpoint.lastIndexOf(':')) {
        int separator = endpoint.indexOf(':');
        host = endpoint.substring(0, separator);
        port = parsePort(endpoint.substring(separator + 1));
      }
      endpoints.add(normalizeHost(host) + ":" + port);
    }
    return new HBaseBackendScope(List.copyOf(endpoints), profile.namespace());
  }

  public boolean isLoopbackOnly() {
    return !endpoints.isEmpty()
        && endpoints.stream().allMatch(value -> value.startsWith("loopback:"));
  }

  private static int parsePort(String value) {
    if (!value.matches("[0-9]{1,5}")) {
      throw new IllegalArgumentException("quorum ports must be integers between 1 and 65535");
    }
    int port = Integer.parseInt(value);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("quorum ports must be integers between 1 and 65535");
    }
    return port;
  }

  private static String normalizeHost(String input) {
    String host = input.toLowerCase(Locale.ROOT);
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }
    if (host.equals("localhost")) {
      return "loopback";
    }
    try {
      InetAddress address = null;
      if (host.contains(":")) {
        if (!host.matches("[0-9a-f:.]{2,64}")) {
          throw new IllegalArgumentException("quorum IPv6 must be a literal without a zone ID");
        }
        // Only colon-containing numeric literals reach this call; hostnames never invoke DNS.
        address = InetAddress.getByName(host);
      } else if (host.matches("[0-9.]+")) {
        String[] octets = host.split("\\.", -1);
        if (octets.length != 4) {
          throw new IllegalArgumentException("quorum IPv4 requires four explicit octets");
        }
        byte[] bytes = new byte[4];
        for (int index = 0; index < 4; index++) {
          if (!octets[index].matches("[0-9]{1,3}") || Integer.parseInt(octets[index]) > 255) {
            throw new IllegalArgumentException("invalid quorum IPv4 literal");
          }
          bytes[index] = (byte) Integer.parseInt(octets[index]);
        }
        address = InetAddress.getByAddress(bytes);
      }
      if (address != null) {
        if (address.isAnyLocalAddress() || address.isMulticastAddress()) {
          throw new IllegalArgumentException("quorum must not use wildcard or multicast addresses");
        }
        return address.isLoopbackAddress()
            ? "loopback"
            : HexFormat.of().formatHex(address.getAddress());
      }
    } catch (UnknownHostException failure) {
      throw new IllegalArgumentException("invalid numeric quorum endpoint");
    }
    if (host.length() > 253 || host.isEmpty()) {
      throw new IllegalArgumentException("quorum hostname must be bounded");
    }
    for (String label : host.split("\\.", -1)) {
      if (!label.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")) {
        throw new IllegalArgumentException("invalid quorum hostname");
      }
    }
    return "host:" + host;
  }
}
