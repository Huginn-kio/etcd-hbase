package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Bounded reference to the complete durable response for one invocation. */
public record ResultReference(ResultIdentity identity, ByteKey digest, int pageCount,
    int itemCount, long totalBytes) {
  private static final int TOKEN_FORMAT_VERSION = 1;
  private static final int TOKEN_BYTES = Integer.BYTES + 32 + Integer.BYTES * 2 + Long.BYTES;

  public ResultReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (digest.length() != 32 || pageCount <= 0 || pageCount > ResultDirectory.MAX_PAGES
        || itemCount != 1 || totalBytes < pageCount
        || totalBytes > (long) pageCount * ResultPage.MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("invalid result reference");
    }
  }

  public boolean matches(ResultDirectory directory) {
    return equals(Objects.requireNonNull(directory, "directory").reference());
  }

  /** Compact invocation-state token. Identity is supplied from the durable invocation itself. */
  public ByteKey token() {
    return ByteKey.copyOf(ByteBuffer.allocate(TOKEN_BYTES).putInt(TOKEN_FORMAT_VERSION)
        .put(digest.bytes()).putInt(pageCount).putInt(itemCount).putLong(totalBytes).array());
  }

  public static ResultReference fromToken(ResultIdentity identity, ByteKey token) {
    Objects.requireNonNull(token, "token");
    if (token.length() != TOKEN_BYTES) throw new IllegalArgumentException("invalid result token length");
    ByteBuffer bytes = ByteBuffer.wrap(token.bytes());
    if (bytes.getInt() != TOKEN_FORMAT_VERSION) throw new IllegalArgumentException("unsupported result token");
    byte[] digest = new byte[32];
    bytes.get(digest);
    return new ResultReference(identity, ByteKey.copyOf(digest), bytes.getInt(), bytes.getInt(), bytes.getLong());
  }
}
