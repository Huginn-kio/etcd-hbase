package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.util.Arrays;
import java.util.Objects;

/** Immutable progress for one captured, contiguous legacy-decision backfill window. */
public record EventIndexBackfillCheckpoint(long ownerEpoch, long sequence, long firstSequence,
    long upperSequence, long nextSequence, long firstBaseRevision, long currentRevision,
    long upperRevision, long compactRevision) {
  private static final byte[] ROUTE = {'E', 'V', 'B', 'F', 1};
  private static final byte[] MAGIC = {'E', 'V', 'B', 'C', 1};
  public static final int ROUTE_BYTES = ROUTE.length;
  public static final int IDENTITY_BYTES = ROUTE_BYTES + Long.BYTES;
  public static final int ENCODED_BYTES = MAGIC.length + 9 * Long.BYTES;

  public EventIndexBackfillCheckpoint {
    if (ownerEpoch <= 0 || firstSequence <= 0 || upperSequence < firstSequence - 1
        || upperSequence == Long.MAX_VALUE || sequence <= upperSequence
        || nextSequence < firstSequence || nextSequence > upperSequence + 1
        || firstBaseRevision <= 0 || currentRevision < firstBaseRevision
        || upperRevision < currentRevision || compactRevision < 0 || compactRevision > upperRevision
        || nextSequence == firstSequence && currentRevision != firstBaseRevision
        || nextSequence == upperSequence + 1 && currentRevision != upperRevision) {
      throw new IllegalArgumentException("invalid event index backfill checkpoint");
    }
  }

  public boolean complete() { return nextSequence == upperSequence + 1; }

  public static ByteKey routePrefix() { return ByteKey.copyOf(ROUTE); }

  public static ByteKey versionIdentity(long sequence) {
    if (sequence <= 0) throw new IllegalArgumentException("checkpoint sequence must be positive");
    return ByteKey.copyOf(ByteBuffer.allocate(IDENTITY_BYTES).put(ROUTE).putLong(sequence).array());
  }

  public static long sequence(ByteKey identity) {
    Objects.requireNonNull(identity, "identity");
    byte[] bytes = identity.bytes();
    if (bytes.length != IDENTITY_BYTES || !Arrays.equals(bytes, 0, ROUTE_BYTES, ROUTE, 0, ROUTE_BYTES)) {
      throw new IllegalArgumentException("invalid event backfill checkpoint identity");
    }
    long value = ByteBuffer.wrap(bytes, ROUTE_BYTES, Long.BYTES).getLong();
    if (value <= 0) throw new IllegalArgumentException("invalid event backfill checkpoint sequence");
    return value;
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES).put(MAGIC)
        .putLong(ownerEpoch).putLong(sequence).putLong(firstSequence).putLong(upperSequence)
        .putLong(nextSequence).putLong(firstBaseRevision).putLong(currentRevision)
        .putLong(upperRevision).putLong(compactRevision).array());
  }

  public static EventIndexBackfillCheckpoint decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() != ENCODED_BYTES) throw new IllegalArgumentException("invalid event backfill checkpoint");
    try {
      ByteBuffer input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[MAGIC.length];
      input.get(magic);
      if (!Arrays.equals(magic, MAGIC)) throw new IllegalArgumentException("invalid event backfill checkpoint");
      return new EventIndexBackfillCheckpoint(input.getLong(), input.getLong(), input.getLong(),
          input.getLong(), input.getLong(), input.getLong(), input.getLong(), input.getLong(), input.getLong());
    } catch (BufferUnderflowException failure) {
      throw new IllegalArgumentException("invalid event backfill checkpoint", failure);
    }
  }

  /**
   * Advances only one sequence. The caller must first verify the old decision and every sealed
   * event under an active history pin, then publish this progress in an owner-fenced M2 commit.
   */
  public EventIndexBackfillCheckpoint advance(long nextOwnerEpoch, long nextCommitSequence,
      CommitDecision verifiedDecision) {
    Objects.requireNonNull(verifiedDecision, "verifiedDecision");
    if (complete() || nextOwnerEpoch < ownerEpoch || nextCommitSequence <= sequence
        || verifiedDecision.sequence() != nextSequence
        || verifiedDecision.baseRevision() != currentRevision) {
      throw new IllegalArgumentException("event backfill cannot skip or regress its captured history");
    }
    return new EventIndexBackfillCheckpoint(nextOwnerEpoch, nextCommitSequence, firstSequence,
        upperSequence, Math.addExact(nextSequence, 1), firstBaseRevision,
        verifiedDecision.revision(), upperRevision, compactRevision);
  }

  /** Rejects altered windows or cursor jumps when a newer checkpoint is prepared. */
  public static void requireForwardProgress(EventIndexBackfillCheckpoint previous,
      EventIndexBackfillCheckpoint replacement) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(replacement, "replacement");
    if (replacement.ownerEpoch < previous.ownerEpoch || replacement.sequence <= previous.sequence
        || replacement.firstSequence != previous.firstSequence || replacement.upperSequence != previous.upperSequence
        || replacement.firstBaseRevision != previous.firstBaseRevision
        || replacement.upperRevision != previous.upperRevision || replacement.compactRevision != previous.compactRevision
        || replacement.nextSequence != previous.nextSequence + 1
        || replacement.currentRevision < previous.currentRevision
        || replacement.currentRevision - previous.currentRevision > 1) {
      throw new IllegalArgumentException("event backfill checkpoint is not contiguous");
    }
  }
}
