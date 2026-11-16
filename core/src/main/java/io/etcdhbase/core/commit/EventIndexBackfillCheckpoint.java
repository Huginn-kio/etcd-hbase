package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.util.Arrays;
import java.util.Objects;

/** Immutable progress for one captured, contiguous legacy-decision backfill window. */
public record EventIndexBackfillCheckpoint(long ownerEpoch, long sequence, long firstSequence,
    long upperSequence, long nextSequence, long firstBaseRevision, long currentRevision,
    long upperRevision, long compactRevision, ByteKey registryIdentity) {
  private static final byte[] ROUTE = {'E', 'V', 'B', 'F', 1};
  private static final byte[] MAGIC = {'E', 'V', 'B', 'C', 2};
  private static final int REGISTRY_ID_BYTES = 16;
  public static final int ROUTE_BYTES = ROUTE.length;
  public static final int IDENTITY_BYTES = ROUTE_BYTES + Long.BYTES;
  public static final int ENCODED_BYTES = MAGIC.length + 9 * Long.BYTES + REGISTRY_ID_BYTES;

  public EventIndexBackfillCheckpoint {
    Objects.requireNonNull(registryIdentity, "registryIdentity");
    if (ownerEpoch <= 0 || firstSequence <= 0 || upperSequence < firstSequence - 1
        || upperSequence == Long.MAX_VALUE || sequence <= upperSequence
        || nextSequence < firstSequence || nextSequence > upperSequence + 1
        || firstBaseRevision <= 0 || currentRevision < firstBaseRevision
        || upperRevision < currentRevision || compactRevision < 0 || compactRevision > upperRevision
        || nextSequence == firstSequence && currentRevision != firstBaseRevision
        || nextSequence == upperSequence + 1 && currentRevision != upperRevision
        || registryIdentity.length() != REGISTRY_ID_BYTES) {
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
        .putLong(upperRevision).putLong(compactRevision).put(registryIdentity.bytes()).array());
  }

  public static EventIndexBackfillCheckpoint decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() != ENCODED_BYTES) throw new IllegalArgumentException("invalid event backfill checkpoint");
    try {
      ByteBuffer input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[MAGIC.length];
      input.get(magic);
      if (!Arrays.equals(magic, MAGIC)) throw new IllegalArgumentException("invalid event backfill checkpoint");
      long ownerEpoch = input.getLong();
      long sequence = input.getLong();
      long firstSequence = input.getLong();
      long upperSequence = input.getLong();
      long nextSequence = input.getLong();
      long firstBaseRevision = input.getLong();
      long currentRevision = input.getLong();
      long upperRevision = input.getLong();
      long compactRevision = input.getLong();
      byte[] registryIdentity = new byte[REGISTRY_ID_BYTES];
      input.get(registryIdentity);
      return new EventIndexBackfillCheckpoint(ownerEpoch, sequence, firstSequence, upperSequence,
          nextSequence, firstBaseRevision, currentRevision, upperRevision, compactRevision,
          ByteKey.copyOf(registryIdentity));
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
        verifiedDecision.revision(), upperRevision, compactRevision, registryIdentity);
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
        || !replacement.registryIdentity.equals(previous.registryIdentity)
        || replacement.nextSequence != previous.nextSequence + 1
        || replacement.currentRevision < previous.currentRevision
        || replacement.currentRevision - previous.currentRevision > 1) {
      throw new IllegalArgumentException("event backfill checkpoint is not contiguous");
    }
  }
}
