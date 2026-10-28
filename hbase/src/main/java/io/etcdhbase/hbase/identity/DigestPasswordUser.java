package io.etcdhbase.hbase.identity;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Credential carrier for an explicitly configured username/password DIGEST backend.
 *
 * <p>This does not authenticate a user, obtain an HBase delegation token, or authorize a table. The
 * backend provider must verify an actual operation before returning an authorized identity. The
 * cluster ID is the backend's actual HBase cluster ID. A standard HBase delegation-token server
 * does not accept arbitrary account passwords through this adapter.
 *
 * <p>The owning provider must close all connections using this user before closing this carrier. It
 * never installs a process-wide login user or puts a password into Configuration or a String.
 */
public final class DigestPasswordUser implements AutoCloseable {
  private final User user;
  private final Token<AuthenticationTokenIdentifier> token;
  private boolean closed;

  private DigestPasswordUser(User user, Token<AuthenticationTokenIdentifier> token) {
    this.user = user;
    this.token = token;
  }

  public static DigestPasswordUser create(BackendCredentials credentials, String clusterId) {
    Objects.requireNonNull(credentials, "credentials");
    requireText(clusterId, 128);
    requireText(credentials.principal(), 256);
    char[] secret = credentials.copySecret();
    ByteBuffer encoded = null;
    byte[] password = null;
    boolean transferred = false;
    try {
      if (secret.length == 0 || secret.length > 4096) {
        throw new IllegalArgumentException("DIGEST password length must be between 1 and 4096");
      }
      encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(secret));
      password = new byte[encoded.remaining()];
      encoded.get(password);
      var identifier = new AuthenticationTokenIdentifier(credentials.principal());
      var token =
          new Token<AuthenticationTokenIdentifier>(
              identifier.getBytes(), password, identifier.getKind(), new Text(clusterId));
      var ugi = UserGroupInformation.createRemoteUser(credentials.principal());
      ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);
      User user = User.create(ugi);
      user.addToken(token);
      transferred = true;
      return new DigestPasswordUser(user, token);
    } catch (CharacterCodingException failure) {
      throw new IllegalArgumentException("DIGEST password must be valid UTF-8 text");
    } finally {
      Arrays.fill(secret, '\0');
      if (!transferred && password != null) {
        Arrays.fill(password, (byte) 0);
      }
      if (encoded != null && encoded.hasArray()) {
        Arrays.fill(encoded.array(), (byte) 0);
      }
    }
  }

  /** Borrowed credential-bearing User; it must remain inside its owning backend provider. */
  public synchronized User user() {
    if (closed) {
      throw new IllegalStateException("DIGEST credentials have been closed");
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
    return "DigestPasswordUser[redacted]";
  }

  private static void requireText(String text, int limit) {
    Objects.requireNonNull(text, "identity text");
    if (text.isBlank()
        || text.codePointCount(0, text.length()) > limit
        || text.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException("DIGEST identity fields must be bounded non-control text");
    }
  }
}
