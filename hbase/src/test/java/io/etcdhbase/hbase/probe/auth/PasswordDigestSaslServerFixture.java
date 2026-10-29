package io.etcdhbase.hbase.probe.auth;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.AttemptingUserProvidingSaslServer;
import org.apache.hadoop.hbase.security.provider.SaslAuthMethod;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/** Loopback-only test server extension, not an implementation for a target account database. */
public final class PasswordDigestSaslServerFixture implements SaslServerAuthenticationProvider {
  public static final String REALM_KEY = "etcdhbase.test.security.realm";
  public static final String FIXTURE_PASSWORD = "temporary-fixture-password";
  private Set<String> accounts;

  @Override
  public void init(Configuration configuration) {
    String realm = configuration.get(REALM_KEY);
    accounts = Set.of("alice@" + realm, "reader@" + realm, "denied@" + realm);
  }

  @Override
  public SaslAuthMethod getSaslAuthMethod() {
    return PasswordDigestSaslClientFixture.METHOD;
  }

  @Override
  public String getTokenKind() {
    return PasswordDigestSaslClientFixture.TOKEN_KIND;
  }

  @Override
  public AttemptingUserProvidingSaslServer createServer(
      SecretManager<TokenIdentifier> secretManager, Map<String, String> properties)
      throws IOException {
    var attemptingUser = new AtomicReference<UserGroupInformation>();
    var server =
        Sasl.createSaslServer(
            "DIGEST-MD5",
            null,
            "default",
            properties,
            callbacks -> {
              for (var callback : callbacks) {
                if (callback instanceof NameCallback name) {
                  attemptingUser.set(user(identifierUser(name.getDefaultName())));
                } else if (callback instanceof PasswordCallback password) {
                  if (attemptingUser.get() == null
                      || !accounts.contains(attemptingUser.get().getUserName())) {
                    throw invalidIdentity();
                  }
                  char[] encoded =
                      Base64.getEncoder()
                          .encodeToString(FIXTURE_PASSWORD.getBytes(StandardCharsets.UTF_8))
                          .toCharArray();
                  try {
                    password.setPassword(encoded);
                  } finally {
                    Arrays.fill(encoded, '\0');
                  }
                } else if (callback instanceof AuthorizeCallback authorization) {
                  authorization.setAuthorized(
                      authorization
                          .getAuthenticationID()
                          .equals(authorization.getAuthorizationID()));
                } else if (callback instanceof RealmCallback realm) {
                  realm.setText(realm.getDefaultText());
                } else {
                  throw new UnsupportedCallbackException(callback);
                }
              }
            });
    if (server == null) throw new IOException("temporary DIGEST server is unavailable");
    return new AttemptingUserProvidingSaslServer(server, attemptingUser::get);
  }

  @Override
  public boolean supportsProtocolAuthentication() {
    return false;
  }

  @Override
  public UserGroupInformation getAuthorizedUgi(
      String authorizationId, SecretManager<TokenIdentifier> secretManager) throws IOException {
    String account = identifierUser(authorizationId);
    if (!accounts.contains(account)) throw invalidIdentity();
    return user(account);
  }

  private static UserGroupInformation user(String name) {
    var user = UserGroupInformation.createRemoteUser(name);
    user.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);
    return user;
  }

  private static String identifierUser(String encoded) throws IOException {
    if (encoded == null || encoded.length() > 16_384) throw invalidIdentity();
    byte[] decoded = null;
    try {
      decoded = Base64.getDecoder().decode(encoded);
      try (var input = new DataInputStream(new ByteArrayInputStream(decoded))) {
        if (decoded.length < 4 || decoded.length > 8192 || input.readInt() != decoded.length - 4)
          throw invalidIdentity();
        input.reset();
        var identifier = new AuthenticationTokenIdentifier();
        identifier.readFields(input);
        if (input.available() != 0 || identifier.getUsername() == null) throw invalidIdentity();
        return identifier.getUsername();
      }
    } catch (IllegalArgumentException failure) {
      throw invalidIdentity();
    } finally {
      if (decoded != null) Arrays.fill(decoded, (byte) 0);
    }
  }

  private static InvalidToken invalidIdentity() {
    return new InvalidToken("temporary DIGEST identity was rejected");
  }
}
