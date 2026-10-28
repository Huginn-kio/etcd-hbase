package io.etcdhbase.hbase.api;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import org.apache.hadoop.conf.Configuration;

/**
 * Backend-specific credential conversion, including custom HBase SASL providers.
 *
 * <p>Called on a bounded provider worker, never an RPC thread. The returned context is owned by the
 * provider. A context alone is not authentication evidence: an actual backend access check must
 * succeed before an authorized identity is published. Implementations must not change global UGI or
 * JAAS configuration, log credentials, or retain the supplied credentials after this call.
 */
@FunctionalInterface
public interface HBaseCredentialAuthenticator {
  String SIMPLE_FALLBACK = "hbase.ipc.client.fallback-to-simple-auth-allowed";

  HBaseUserContext authenticate(BackendCredentials credentials, AuthenticationScope scope)
      throws Exception;

  /** Validate a private configuration snapshot before starting any backend work. */
  default void validateConfiguration(Configuration configuration) {
    // Public HBase enables SASL with kerberos configuration, including delegation-token users.
    if (!"kerberos".equals(configuration.get("hbase.security.authentication", "simple"))
        || configuration.getBoolean(SIMPLE_FALLBACK, false)) {
      throw new IllegalArgumentException(
          "secure HBase authentication requires SASL without fallback");
    }
  }
}
