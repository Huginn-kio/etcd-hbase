package io.etcdhbase.admin.config;

import java.util.List;

/** Physical namespace identity used only to protect configured sibling groups during init. */
public record HBaseBackendScope(List<String> endpoints, String namespace) {
  public HBaseBackendScope {
    endpoints = List.copyOf(endpoints);
  }

  public static HBaseBackendScope of(InitializationConfiguration.HBaseProfile profile) {
    return new HBaseBackendScope(HBaseQuorum.scopeEndpoints(profile), profile.namespace());
  }

  public boolean isLoopbackOnly() {
    return !endpoints.isEmpty()
        && endpoints.stream().allMatch(value -> value.startsWith("loopback:"));
  }
}
