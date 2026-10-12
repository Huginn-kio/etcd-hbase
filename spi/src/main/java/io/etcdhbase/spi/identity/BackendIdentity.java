package io.etcdhbase.spi.identity;

/**
 * Provider-owned, immutable backend identity handle.
 *
 * <p>Implementations may hold provider-specific connection context, but must not expose passwords,
 * tokens, or other secret material through these accessors or {@code toString()}.
 */
public interface BackendIdentity {
  String providerId();

  String principal();

  String tableGroupId();
}
