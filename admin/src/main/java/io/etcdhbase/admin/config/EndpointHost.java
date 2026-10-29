package io.etcdhbase.admin.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HexFormat;
import java.util.Locale;

/** A canonical transport host and conservative physical-scope identity; never resolves DNS. */
public record EndpointHost(String connectionName, String scopeIdentity) {
  public boolean isLoopback() {
    return scopeIdentity.equals("loopback");
  }

  public static EndpointHost parse(String input) {
    String host = input.toLowerCase(Locale.ROOT);
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }
    if (host.equals("localhost")) {
      return new EndpointHost(host, "loopback");
    }
    InetAddress address = numericAddress(host);
    if (address != null) {
      return normalizeAddress(address);
    }
    validateHostname(host);
    return new EndpointHost(host, "host:" + host);
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

  private static EndpointHost normalizeAddress(InetAddress address) {
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
    return new EndpointHost(host, scope);
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
}
