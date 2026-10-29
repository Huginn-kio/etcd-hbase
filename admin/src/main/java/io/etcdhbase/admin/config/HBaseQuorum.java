package io.etcdhbase.admin.config;

import java.util.List;
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
      return new Endpoint(EndpointHost.parse(endpoint.substring(0, separator)), port);
    }
    return new Endpoint(EndpointHost.parse(endpoint), defaultPort);
  }

  private static Endpoint parseBracketedEndpoint(String endpoint, int defaultPort) {
    int closing = endpoint.indexOf(']');
    if (closing < 0) {
      throw new IllegalArgumentException("invalid bracketed quorum endpoint");
    }
    String host = endpoint.substring(1, closing);
    String suffix = endpoint.substring(closing + 1);
    if (suffix.isEmpty()) {
      return new Endpoint(EndpointHost.parse(host), defaultPort);
    }
    if (!suffix.startsWith(":")) {
      throw new IllegalArgumentException("invalid quorum endpoint port");
    }
    int port = parsePort(suffix.substring(1));
    return new Endpoint(EndpointHost.parse(host), port);
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

  private record Endpoint(EndpointHost host, int port) {
    private String connectionString() {
      String name = host.connectionName();
      return (name.contains(":") ? "[" + name + "]" : name) + ":" + port;
    }

    private String scopeIdentity() {
      return host.scopeIdentity() + ":" + port;
    }
  }
}
