package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;

/** Token-carrier/decoder tests. These fixture tokens are not credentials accepted by a cluster. */
class DelegationTokenUserTest {
  @Test
  void readsOfficialHadoopEncodingAndClearsOwnedTokenMaterial() throws Exception {
    var source = token("alice", "cluster", System.currentTimeMillis() + 60_000);
    try (var credentials =
            new BackendCredentials("alice", source.encodeToUrlString().toCharArray());
        var context =
            HBaseAuthenticators.delegationToken("cluster")
                .authenticate(credentials, new AuthenticationScope("group", "backend"))) {
      var actual = context.user().getTokens().iterator().next();
      byte[] owned = actual.getPassword();
      assertArrayEquals(source.getPassword(), owned);
      assertEquals(source.getService(), actual.getService());
      assertEquals("alice", context.user().getName());
      assertFalse(context.toString().contains("alice"));
      credentials.close();
      assertArrayEquals(source.getPassword(), owned);
      context.close();
      assertArrayEquals(new byte[owned.length], owned);
      assertThrows(IllegalStateException.class, context::user);
      assertNotEquals(
          0, source.getPassword()[0], "the caller's token must remain owned by the caller");
    }
  }

  @Test
  void rejectsWrongOwnerClusterKindExpiryAndTrailingData() throws Exception {
    var wrongKind = token("alice", "cluster", System.currentTimeMillis() + 60_000);
    wrongKind.setKind(new Text("WRONG_TOKEN_KIND"));
    for (String encoded :
        new String[] {
          token("mallory", "cluster", System.currentTimeMillis() + 60_000).encodeToUrlString(),
          token("alice", "other-cluster", System.currentTimeMillis() + 60_000).encodeToUrlString(),
          token("alice", "cluster", 1).encodeToUrlString(),
          wrongKind.encodeToUrlString(),
          "not a token"
        }) {
      try (var credentials = new BackendCredentials("alice", encoded.toCharArray())) {
        var failure =
            assertThrows(
                InvalidToken.class, () -> DelegationTokenUser.create(credentials, "cluster"));
        assertFalse(failure.getMessage().contains(encoded));
      }
    }
    byte[] bytes =
        Base64.getUrlDecoder()
            .decode(
                token("alice", "cluster", System.currentTimeMillis() + 60_000).encodeToUrlString());
    assertInvalid(
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(java.util.Arrays.copyOf(bytes, bytes.length + 1)));
  }

  @Test
  void boundsEveryUntrustedLengthBeforeAllocatingOrDecodingHBaseIdentifier() throws Exception {
    var output = new ByteArrayOutputStream();
    try (var data = new DataOutputStream(output)) {
      WritableUtils.writeVInt(data, Integer.MAX_VALUE);
      data.writeByte(0);
    }
    assertInvalid(Base64.getUrlEncoder().withoutPadding().encodeToString(output.toByteArray()));
    var malformedIdentifier =
        new Token<AuthenticationTokenIdentifier>(
            ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array(),
            new byte[] {1},
            AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE,
            new Text("cluster"));
    assertInvalid(malformedIdentifier.encodeToUrlString());
    try (var credentials = new BackendCredentials("alice", new char[32_769])) {
      assertThrows(
          IllegalArgumentException.class, () -> DelegationTokenUser.create(credentials, "cluster"));
    }
  }

  private static void assertInvalid(String encoded) {
    try (var credentials = new BackendCredentials("alice", encoded.toCharArray())) {
      assertThrows(InvalidToken.class, () -> DelegationTokenUser.create(credentials, "cluster"));
    }
  }

  private static Token<AuthenticationTokenIdentifier> token(
      String owner, String cluster, long expires) {
    var identifier =
        new AuthenticationTokenIdentifier(owner, 1, System.currentTimeMillis(), expires);
    return new Token<>(
        identifier.getBytes(),
        "fixture-secret".getBytes(StandardCharsets.UTF_8),
        identifier.getKind(),
        new Text(cluster));
  }
}
