package io.etcdhbase.core.commit;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/** Fixed-width identity for one internally forwarded or recovered invocation. */
public record InvocationId(UUID value) {
  public InvocationId {
    Objects.requireNonNull(value, "value");
  }

  public static InvocationId create() {
    return new InvocationId(UUID.randomUUID());
  }

  public static InvocationId fromBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    if (bytes.length != Long.BYTES * 2) {
      throw new IllegalArgumentException("invocation id must contain 16 bytes");
    }
    var encoded = ByteBuffer.wrap(bytes);
    return new InvocationId(new UUID(encoded.getLong(), encoded.getLong()));
  }

  public byte[] bytes() {
    return ByteBuffer.allocate(Long.BYTES * 2)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }
}
