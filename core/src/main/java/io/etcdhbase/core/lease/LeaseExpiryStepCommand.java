package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Internal request to let the fenced owner advance exactly one durable expiry scan step. */
public record LeaseExpiryStepCommand(long expectedCheckpointSequence) {
  private static final byte[] MAGIC = {'L', 'E', 'S', 2};
  private static final int ENCODED_BYTES = MAGIC.length + Long.BYTES;

  public LeaseExpiryStepCommand {
    if (expectedCheckpointSequence < 0) {
      throw new IllegalArgumentException("expiry checkpoint sequence must be non-negative");
    }
  }

  /** Sequence zero means the first checkpoint has not yet been durably seeded. */
  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES)
        .put(MAGIC).putLong(expectedCheckpointSequence).array());
  }

  public static LeaseExpiryStepCommand decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    byte[] bytes = encoded.bytes();
    if (bytes.length != ENCODED_BYTES
        || !Arrays.equals(bytes, 0, MAGIC.length, MAGIC, 0, MAGIC.length)) {
      throw new IllegalArgumentException("invalid Lease expiry step command");
    }
    long sequence = ByteBuffer.wrap(bytes, MAGIC.length, Long.BYTES).getLong();
    try {
      return new LeaseExpiryStepCommand(sequence);
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("Lease expiry step command violates its storage contract", invalid);
    }
  }
}
