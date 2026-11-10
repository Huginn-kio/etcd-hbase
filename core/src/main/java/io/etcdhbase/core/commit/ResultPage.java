package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Immutable, bounded result bytes for one invocation and one page index. */
public final class ResultPage {
  public static final int MAX_PAYLOAD_BYTES = 64 * 1024;

  private final ResultIdentity identity;
  private final int index;
  private final byte[] payload;
  private final ByteKey digest;

  public ResultPage(ResultIdentity identity, int index, byte[] payload) {
    this(identity, index, payload, null);
  }

  private ResultPage(ResultIdentity identity, int index, byte[] payload, ByteKey expectedDigest) {
    this.identity = Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(payload, "payload");
    if (index < 0 || index >= ResultDirectory.MAX_PAGES || payload.length == 0
        || payload.length > MAX_PAYLOAD_BYTES
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid result page bounds");
    }
    this.index = index;
    this.payload = payload.clone();
    this.digest = ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.writeByte(ManifestRecordFormat.RESULT_PAGE);
      identity.writeTo(output);
      output.writeInt(index);
      output.writeInt(index == 0 ? 1 : 0);
      ManifestDigest.writeBytes(output, this.payload);
    });
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("result page digest mismatch");
    }
  }

  public static ResultPage restore(ResultIdentity identity, int index, byte[] payload,
      ByteKey digest) {
    return new ResultPage(identity, index, payload, Objects.requireNonNull(digest, "digest"));
  }

  public ResultIdentity identity() { return identity; }
  public int index() { return index; }
  public byte[] payload() { return payload.clone(); }
  public int payloadBytes() { return payload.length; }
  /** One result tree is length-framed by the directory; it starts in the first page. */
  public int itemCount() { return index == 0 ? 1 : 0; }
  public ByteKey digest() { return digest; }

  public ByteKey systemIdentity() {
    return systemIdentity(identity, index);
  }

  public static ByteKey systemIdentity(ResultIdentity identity, int index) {
    Objects.requireNonNull(identity, "identity");
    if (index < 0 || index >= ResultDirectory.MAX_PAGES) {
      throw new IllegalArgumentException("invalid result page index");
    }
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(identity.transactionId().bytes()).put(ManifestRecordFormat.RESULT_PAGE)
        .putInt(index).array());
  }
}
