package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.connection.DevelopmentConnectionConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Standard credential adapters; custom mechanisms implement {@link HBaseCredentialAuthenticator}.
 */
public final class HBaseAuthenticators {
  private HBaseAuthenticators() {}

  /** Explicit local development only: SIMPLE cannot prove ownership of a claimed username. */
  public static HBaseCredentialAuthenticator simpleDevelopment() {
    return new HBaseCredentialAuthenticator() {
      @Override
      public void validateConfiguration(Configuration configuration) {
        DevelopmentConnectionConfiguration.validate(configuration);
      }

      @Override
      public HBaseUserContext authenticate(
          BackendCredentials credentials, AuthenticationScope scope) {
        requirePrincipal(credentials.principal());
        char[] secret = credentials.copySecret(0);
        Arrays.fill(secret, '\0');
        User user = User.create(UserGroupInformation.createRemoteUser(credentials.principal()));
        return new HBaseUserContext() {
          private boolean closed;

          @Override
          public synchronized User user() {
            if (closed) {
              throw new IllegalStateException("backend user context is closed");
            }
            return user;
          }

          @Override
          public synchronized void close() {
            closed = true;
          }
        };
      }
    };
  }

  /** Exchanges the submitted user's password for an isolated Kerberos Subject. */
  public static HBaseCredentialAuthenticator kerberosPassword() {
    return (credentials, scope) -> KerberosUserContext.fromPassword(credentials);
  }

  /** Uses an existing standard HBase delegation token, submitted as its Hadoop URL encoding. */
  public static HBaseCredentialAuthenticator delegationToken(String expectedClusterId) {
    requirePrincipal(expectedClusterId);
    return (credentials, scope) -> DelegationTokenUser.create(credentials, expectedClusterId);
  }

  /** Select only for a backend explicitly implementing account-password DIGEST semantics. */
  public static HBaseCredentialAuthenticator digestPassword(String expectedClusterId) {
    requirePrincipal(expectedClusterId);
    return (credentials, scope) -> DigestPasswordUser.create(credentials, expectedClusterId);
  }

  static void requirePrincipal(String principal) {
    if (principal == null
        || principal.isBlank()
        || principal.length() > 256
        || principal.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException("backend identity must be bounded non-control text");
    }
  }
}
