package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider;
import org.junit.jupiter.api.Test;

/** Protocol fixture, not a claim that a target HBase authenticated or authorized these users. */
class DigestPasswordUserTest {
  @Test
  void carrierOwnsCopiedUtf8CredentialsAndClearsThemOnIdempotentClose() {
    var credentials = new BackendCredentials("probe-user", "test-口令".toCharArray());
    var carrier = DigestPasswordUser.create(credentials, "probe-cluster");
    var token = carrier.user().getTokens().iterator().next();
    byte[] ownedPassword = token.getPassword();
    credentials.close();
    assertArrayEquals("test-口令".getBytes(StandardCharsets.UTF_8), ownedPassword);
    assertEquals("probe-user", carrier.user().getName());
    assertEquals("probe-cluster", token.getService().toString());
    assertFalse(carrier.toString().contains("probe-user"));
    carrier.close();
    carrier.close();
    assertArrayEquals(new byte[ownedPassword.length], ownedPassword);
    assertThrows(IllegalStateException.class, carrier::user);
  }

  @Test
  void officialHBaseDigestClientCompletesARealSaslExchangeAndRejectsWrongPasswords()
      throws Exception {
    exchange("probe-password", "probe-password");
    assertThrows(SaslException.class, () -> exchange("wrong-password", "probe-password"));
  }

  @Test
  void rejectsEmptyOversizedAndMalformedPasswordsWithoutEchoingThem() {
    for (char[] secret : new char[][] {new char[0], new char[4097], new char[] {'\ud800'}}) {
      try (var credentials = new BackendCredentials("probe-user", secret)) {
        assertThrows(
            IllegalArgumentException.class,
            () -> DigestPasswordUser.create(credentials, "probe-cluster"));
      }
      Arrays.fill(secret, '\0');
    }
  }

  private static void exchange(String suppliedPassword, String serverPassword) throws Exception {
    Map<String, String> properties = Map.of(Sasl.QOP, "auth");
    try (var credentials = new BackendCredentials("probe-user", suppliedPassword.toCharArray());
        var carrier = DigestPasswordUser.create(credentials, "probe-cluster")) {
      var token = carrier.user().getTokens().iterator().next();
      String expectedIdentifier = Base64.getEncoder().encodeToString(token.getIdentifier());
      var server =
          Sasl.createSaslServer(
              "DIGEST-MD5",
              null,
              "default",
              properties,
              callbacks -> {
                for (var callback : callbacks) {
                  if (callback instanceof NameCallback name) {
                    if (!expectedIdentifier.equals(name.getDefaultName())) {
                      throw new SaslException("invalid fixture identity");
                    }
                  } else if (callback instanceof PasswordCallback password) {
                    char[] encoded =
                        Base64.getEncoder()
                            .encodeToString(serverPassword.getBytes(StandardCharsets.UTF_8))
                            .toCharArray();
                    password.setPassword(encoded);
                    Arrays.fill(encoded, '\0');
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
      var client =
          new DigestSaslClientAuthenticationProvider()
              .createClient(
                  new Configuration(false),
                  InetAddress.getLoopbackAddress(),
                  (String) null,
                  token,
                  false,
                  properties);
      assertNotNull(server);
      assertNotNull(client);
      try {
        byte[] challenge = server.evaluateResponse(new byte[0]);
        byte[] response = client.evaluateChallenge(challenge);
        byte[] finalChallenge = server.evaluateResponse(response);
        client.evaluateChallenge(finalChallenge);
        assertTrue(client.isComplete());
        assertTrue(server.isComplete());
        assertEquals(expectedIdentifier, server.getAuthorizationID());
      } finally {
        client.dispose();
        server.dispose();
      }
    }
  }
}
