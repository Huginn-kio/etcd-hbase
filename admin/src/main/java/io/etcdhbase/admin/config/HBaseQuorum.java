package io.etcdhbase.admin.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Parses initialization endpoints without resolving hostnames. */
public final class HBaseQuorum {
  private HBaseQuorum() {}

  public static String connectionString(InitializationConfiguration.HBaseProfile profile) {
    return endpoints(profile).stream()
        .map(Endpoint::connectionString)
        .collect(Collectors.joining(","));
  }

  static List<String> scopeEndpoints(InitializationConfiguration.HBaseProfile profile) {
    return endpoints(profile).stream().map(Endpoint::scopeIdentity).distinct().sorted().toList();
  }

  private static List<Endpoint> endpoints(InitializationConfiguration.HBaseProfile profile) {
    String quorum = profile.quorum();
    if (quorum == null || quorum.isBlank() || quorum.length() > 4096) {
      throw new IllegalArgumentException("quorum must contain bounded explicit endpoints");
    }
    return List.of(quorum.split(",", -1)).stream()
        .map(value -> parseEndpoint(value.strip(), profile.clientPort()))
        .toList();
  }

  private static Endpoint parseEndpoint(String endpoint, int defaultPort) {
    if (endpoint.startsWith("[")) {
      return parseBracketedEndpoint(endpoint, defaultPort);
    }
    int separator = endpoint.indexOf(':');
    if (separator >= 0 && separator == endpoint.lastIndexOf(':')) {
      int port = parsePort(endpoint.substring(separator + 1));
      return new Endpoint(
          normalizeHost(endpoint.substring(0, separator)), port);
    }
    return new Endpoint(normalizeHost(endpoint), defaultPort);
  }

  private static Endpoint parseBracketedEndpoint(String endpoint, int defaultPort) {
    int closing = endpoint.indexOf(']');
    if (closing < 0) {
      throw new IllegalArgumentException("invalid bracketed quorum endpoint");
    }
    String host = endpoint.substring(1, closing);
    String suffix = endpoint.substring(closing + 1);
    if (suffix.isEmpty()) {
      return new Endpoint(normalizeHost(host), defaultPort);
    }
    if (!suffix.startsWith(":")) {
      throw new IllegalArgumentException("invalid quorum endpoint port");
    }
    int port = parsePort(suffix.substring(1));
    return new Endpoint(normalizeHost(host), port);
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

  private static Host normalizeHost(String input) {
    String host = input.toLowerCase(Locale.ROOT);
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }
    if (host.equals("localhost")) {
      return new Host(host, "loopback");
    }
    InetAddress address = numericAddress(host);
    if (address != null) {
      return normalizeAddress(address);
    }
    validateHostname(host);
    return new Host(host, "host:" + host);
  }

  private static InetAddress numericAddress(String host) {
    try {
      if (host.contains(":")) {
        if (!host.matches("[0-9a-f:.]{2,64}")) {
          throw new IllegalArgumentException("quorum IPv6 must be a literal without a zone ID");
        }
        // Only numeric, colon-containing literals reach this call; hostnames never invoke DNS.
        return InetAddress.getByName(host);
      }
      if (host.matches("[0-9.]+")) {
        return ipv4Address(host);
      }
      return null;
    } catch (UnknownHostException failure) {
      throw new IllegalArgumentException("invalid numeric quorum endpoint");
    }
  }

  private static InetAddress ipv4Address(String host) throws UnknownHostException {
    String[] octets = host.split("\\.", -1);
    if (octets.length != 4) {
      throw new IllegalArgumentException("quorum IPv4 requires four explicit octets");
    }
    byte[] bytes = new byte[4];
    for (int index = 0; index < bytes.length; index++) {
      if (!octets[index].matches("[0-9]{1,3}") || Integer.parseInt(octets[index]) > 255) {
        throw new IllegalArgumentException("invalid quorum IPv4 literal");
      }
      bytes[index] = (byte) Integer.parseInt(octets[index]);
    }
    return InetAddress.getByAddress(bytes);
  }

  private static Host normalizeAddress(InetAddress address) {
    if (address.isAnyLocalAddress() || address.isMulticastAddress()) {
      throw new IllegalArgumentException("quorum must not use wildcard or multicast addresses");
    }
    String scope =
        address.isLoopbackAddress() ? "loopback" : HexFormat.of().formatHex(address.getAddress());
    // Scope aliases are conservative, but transport must retain each actual 127/8 address.
    String host =
        address.isLoopbackAddress() && address.getAddress().length == 16
            ? "::1"
            : address.getHostAddress();
    return new Host(host, scope);
  }

  private static void validateHostname(String host) {
    if (host.length() > 253 || host.isEmpty()) {
      throw new IllegalArgumentException("quorum hostname must be bounded");
    }
    for (String label : host.split("\\.", -1)) {
      if (!label.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")) {
        throw new IllegalArgumentException("invalid quorum hostname");
      }
    }
  }

  private record Host(String connectionName, String scopeIdentity) {}

  private record Endpoint(Host host, int port) {
    private String connectionString() {
      String name = host.connectionName();
      return (name.contains(":") ? "[" + name + "]" : name) + ":" + port;
    }

    private String scopeIdentity() {
      return host.scopeIdentity() + ":" + port;
    }
  }
}
