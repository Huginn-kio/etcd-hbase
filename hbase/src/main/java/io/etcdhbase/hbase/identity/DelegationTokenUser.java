package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/** Standard HBase delegation-token credentials, distinct from account-password DIGEST. */
final class DelegationTokenUser implements HBaseUserContext {
  private final User user;
  private final Token<AuthenticationTokenIdentifier> token;
  private boolean closed;

  private DelegationTokenUser(User user, Token<AuthenticationTokenIdentifier> token) {
    this.user = user;
    this.token = token;
  }

  static DelegationTokenUser create(BackendCredentials credentials, String expectedClusterId)
      throws IOException {
    HBaseAuthenticators.requirePrincipal(credentials.principal());
    char[] secret = credentials.copySecret(32_768);
    byte[] encoded = new byte[secret.length];
    byte[] decoded = null;
    byte[] password = null;
    boolean transferred = false;
    try {
      for (int i = 0; i < secret.length; i++) {
        char character = secret[i];
        if (!(character >= 'A' && character <= 'Z')
            && !(character >= 'a' && character <= 'z')
            && !(character >= '0' && character <= '9')
            && character != '-'
            && character != '_'
            && character != '=') {
          throw invalidToken();
        }
        encoded[i] = (byte) character;
      }
      decoded = Base64.getUrlDecoder().decode(encoded);
      try (var input = new DataInputStream(new ByteArrayInputStream(decoded))) {
        byte[] identifierBytes = boundedBytes(input, 8192);
        password = boundedBytes(input, 8192);
        String kind = text(input, 64);
        String service = text(input, 256);
        if (input.available() != 0
            || password.length == 0
            || !AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE.toString().equals(kind)
            || !expectedClusterId.equals(service)) {
          throw invalidToken();
        }
        var identifier = new AuthenticationTokenIdentifier();
        try (var identifierInput = new DataInputStream(new ByteArrayInputStream(identifierBytes))) {
          // HBase's readFields allocates the leading int unchecked. Validate it before decoding.
          if (identifierBytes.length < 4
              || identifierInput.readInt() != identifierBytes.length - 4) {
            throw invalidToken();
          }
          identifierInput.reset();
          identifier.readFields(identifierInput);
          if (identifierInput.available() != 0
              || !credentials.principal().equals(identifier.getUsername())
              || identifier.getExpirationDate() <= System.currentTimeMillis()) {
            throw invalidToken();
          }
        }
        var token =
            new Token<AuthenticationTokenIdentifier>(
                identifierBytes, password, new Text(kind), new Text(service));
        var ugi = UserGroupInformation.createRemoteUser(credentials.principal());
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);
        User user = User.create(ugi);
        user.addToken(token);
        transferred = true;
        return new DelegationTokenUser(user, token);
      }
    } catch (IllegalArgumentException | IOException failure) {
      throw invalidToken();
    } finally {
      Arrays.fill(secret, '\0');
      Arrays.fill(encoded, (byte) 0);
      if (decoded != null) {
        Arrays.fill(decoded, (byte) 0);
      }
      if (!transferred && password != null) {
        Arrays.fill(password, (byte) 0);
      }
    }
  }

  private static byte[] boundedBytes(DataInputStream input, int limit) throws IOException {
    int length = WritableUtils.readVInt(input);
    if (length < 0 || length > limit || length > input.available()) {
      throw invalidToken();
    }
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private static String text(DataInputStream input, int limit) throws IOException {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .decode(ByteBuffer.wrap(boundedBytes(input, limit)))
          .toString();
    } catch (CharacterCodingException failure) {
      throw invalidToken();
    }
  }

  private static IOException invalidToken() {
    return new org.apache.hadoop.security.token.SecretManager.InvalidToken(
        "backend delegation token was rejected");
  }

  @Override
  public synchronized User user() {
    if (closed) {
      throw new IllegalStateException("backend user context is closed");
    }
    return user;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      Arrays.fill(token.getPassword(), (byte) 0);
      token.setPassword(new byte[0]);
      closed = true;
    }
  }

  @Override
  public String toString() {
    return "DelegationTokenUser[redacted]";
  }
}
