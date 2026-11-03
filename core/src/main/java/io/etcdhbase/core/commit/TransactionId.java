package io.etcdhbase.core.commit;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/** Fixed-width identity for one immutable transaction preparation attempt. */
public record TransactionId(UUID value) {
  public TransactionId {
    Objects.requireNonNull(value, "value");
  }

  public static TransactionId create() {
    return new TransactionId(UUID.randomUUID());
  }

  public static TransactionId fromBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    if (bytes.length != Long.BYTES * 2) {
      throw new IllegalArgumentException("transaction id must contain 16 bytes");
    }
    var encoded = ByteBuffer.wrap(bytes);
    return new TransactionId(new UUID(encoded.getLong(), encoded.getLong()));
  }

  public byte[] bytes() {
    return ByteBuffer.allocate(Long.BYTES * 2)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }
}
