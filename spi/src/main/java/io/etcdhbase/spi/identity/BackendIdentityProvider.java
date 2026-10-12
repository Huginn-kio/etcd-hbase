package io.etcdhbase.spi.identity;

/**
 * Authenticates backend credentials for one configured table group.
 *
 * <p>This provider does not create or validate etcd tokens or sessions. It must verify access to the
 * requested table group before returning an identity and must distinguish rejected credentials,
 * insufficient permission, and backend unavailability. Implementations must not log credentials.
 */
public interface BackendIdentityProvider {
  String providerId();

  BackendIdentity authenticate(BackendCredentials credentials, BackendIdentityRequest request)
      throws BackendIdentityException;
}
