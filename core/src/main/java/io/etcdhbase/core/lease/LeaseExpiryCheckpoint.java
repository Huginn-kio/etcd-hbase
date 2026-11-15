package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Durable, immutable checkpoint version for bounded Lease expiry scans. */
public record LeaseExpiryCheckpoint(long ownerEpoch, long sequence, int bucketCount,
    long protectedSequence, long trustedTimeLowerEpochMillis, Position position) {
  private static final byte[] IDENTITY_PREFIX = {'L', 'E', 'X', 'P', 1};
  private static final byte[] VALUE_MAGIC = {'L', 'E', 'C', 1};
  public static final int ROUTE_BYTES = IDENTITY_PREFIX.length;
  public static final int IDENTITY_BYTES = ROUTE_BYTES + Long.BYTES;
  private static final int MAX_ENCODED_BYTES = 128;
  private static final int MAX_BUCKETS = 65_536;

  /** The next deadline window and salt bucket to scan. {@code afterIdentity} is exclusive. */
  public record Position(long windowStartInclusive, long windowStopExclusive, int bucket,
      Optional<ByteKey> afterIdentity) {
    public Position {
      Objects.requireNonNull(afterIdentity, "afterIdentity");
      if (windowStartInclusive < 0 || windowStopExclusive < windowStartInclusive || bucket < 0
          || windowStartInclusive == windowStopExclusive && (bucket != 0 || afterIdentity.isPresent())) {
        throw new IllegalArgumentException("invalid Lease expiry checkpoint position");
      }
      afterIdentity.ifPresent(identity -> {
        ExpiryCandidateIdentity candidate = ExpiryCandidateIdentity.decode(identity);
        if (windowStartInclusive == windowStopExclusive
            || candidate.deadlineEpochMillis() < windowStartInclusive
            || candidate.deadlineEpochMillis() >= windowStopExclusive) {
          throw new IllegalArgumentException("Lease expiry cursor is outside its deadline window");
        }
      });
    }

    public boolean idle() {
      return windowStartInclusive == windowStopExclusive;
    }
  }

  public LeaseExpiryCheckpoint {
    Objects.requireNonNull(position, "position");
    if (ownerEpoch <= 0 || sequence <= 0 || bucketCount < 1 || bucketCount > MAX_BUCKETS
        || protectedSequence < 0 || protectedSequence >= sequence
        || trustedTimeLowerEpochMillis < 0 || position.bucket() >= bucketCount
        || !isClosedBy(position.windowStopExclusive(), trustedTimeLowerEpochMillis)) {
      throw new IllegalArgumentException("invalid Lease expiry checkpoint identity");
    }
  }

  /** Stable routing prefix shared by all immutable checkpoint versions. */
  public static ByteKey routePrefix() {
    return ByteKey.copyOf(IDENTITY_PREFIX);
  }

  /** Builds the immutable row identity for one selected M2 sequence. */
  public static ByteKey versionIdentity(long sequence) {
    if (sequence <= 0) throw new IllegalArgumentException("checkpoint sequence must be positive");
    return ByteKey.copyOf(ByteBuffer.allocate(IDENTITY_BYTES)
        .put(IDENTITY_PREFIX).putLong(sequence).array());
  }

  public static long sequence(ByteKey identity) {
    Objects.requireNonNull(identity, "identity");
    byte[] bytes = identity.bytes();
    if (bytes.length != IDENTITY_BYTES
        || !Arrays.equals(bytes, 0, ROUTE_BYTES, IDENTITY_PREFIX, 0, ROUTE_BYTES)) {
      throw new IllegalArgumentException("invalid Lease expiry checkpoint version identity");
    }
    long sequence = ByteBuffer.wrap(bytes, ROUTE_BYTES, Long.BYTES).getLong();
    if (sequence <= 0) throw new IllegalArgumentException("invalid Lease expiry checkpoint sequence");
    return sequence;
  }

  public ByteKey encode() {
    byte[] cursor = position.afterIdentity().map(ByteKey::bytes).orElseGet(() -> new byte[0]);
    return ByteKey.copyOf(ByteBuffer.allocate(VALUE_MAGIC.length + Long.BYTES * 6
            + Integer.BYTES * 3 + cursor.length)
        .put(VALUE_MAGIC)
        .putLong(ownerEpoch)
        .putLong(sequence)
        .putInt(bucketCount)
        .putLong(protectedSequence)
        .putLong(trustedTimeLowerEpochMillis)
        .putLong(position.windowStartInclusive())
        .putLong(position.windowStopExclusive())
        .putInt(position.bucket())
        .putInt(position.afterIdentity().isPresent() ? cursor.length : -1)
        .put(cursor)
        .array());
  }

  public static LeaseExpiryCheckpoint decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    int minimumBytes = VALUE_MAGIC.length + Long.BYTES * 6 + Integer.BYTES * 3;
    if (encoded.length() < minimumBytes || encoded.length() > MAX_ENCODED_BYTES) throw invalid();
    try {
      ByteBuffer input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[VALUE_MAGIC.length];
      input.get(magic);
      if (!Arrays.equals(magic, VALUE_MAGIC)) throw invalid();
      long ownerEpoch = input.getLong();
      long sequence = input.getLong();
      int bucketCount = input.getInt();
      long protectedSequence = input.getLong();
      long timeLower = input.getLong();
      long start = input.getLong();
      long stop = input.getLong();
      int bucket = input.getInt();
      int cursorLength = input.getInt();
      Optional<ByteKey> cursor;
      if (cursorLength == -1) {
        cursor = Optional.empty();
      } else if (cursorLength == ExpiryCandidateIdentity.ENCODED_BYTES
          && input.remaining() == cursorLength) {
        byte[] bytes = new byte[cursorLength];
        input.get(bytes);
        cursor = Optional.of(ByteKey.copyOf(bytes));
      } else {
        throw invalid();
      }
      if (input.hasRemaining()) throw invalid();
      return new LeaseExpiryCheckpoint(ownerEpoch, sequence, bucketCount,
          protectedSequence, timeLower, new Position(start, stop, bucket, cursor));
    } catch (IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted Lease expiry checkpoint", failure);
    } catch (java.nio.BufferUnderflowException malformed) {
      throw new IllegalArgumentException("invalid persisted Lease expiry checkpoint", malformed);
    }
  }

  /**
   * Rejects any cursor, owner, time-watermark, or protected-sequence regression before the next
   * immutable checkpoint can be selected.
   */
  public static void requireForwardProgress(LeaseExpiryCheckpoint previous,
      LeaseExpiryCheckpoint replacement) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(replacement, "replacement");
    if (replacement.sequence() <= previous.sequence()
        || replacement.ownerEpoch() < previous.ownerEpoch()
        || replacement.bucketCount() != previous.bucketCount()
        || replacement.protectedSequence() < previous.protectedSequence()
        || replacement.trustedTimeLowerEpochMillis() < previous.trustedTimeLowerEpochMillis()) {
      throw new IllegalArgumentException("Lease expiry checkpoint sequence, epoch, or watermark regressed");
    }
    requirePositionProgress(previous.position(), replacement.position(),
        replacement.trustedTimeLowerEpochMillis(), replacement.bucketCount());
  }

  /** Creates a Lease-mutation checkpoint without moving the already selected scan cursor. */
  public static LeaseExpiryCheckpoint afterLeaseMutation(long ownerEpoch, long sequence,
      int bucketCount, long baseSequence, Optional<LeaseExpiryCheckpoint> previous,
      LeaseTimeObservation observation) {
    return afterMutation(ownerEpoch, sequence, bucketCount, baseSequence, previous,
        Optional.of(Objects.requireNonNull(observation, "observation")));
  }

  /** Advances the checkpoint for a Lease state change, retaining the trusted clock when unused. */
  public static LeaseExpiryCheckpoint afterMutation(long ownerEpoch, long sequence,
      int bucketCount, long baseSequence, Optional<LeaseExpiryCheckpoint> previous,
      Optional<LeaseTimeObservation> observation) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(observation, "observation");
    observation.ifPresent(LeaseTimeRules::requireHealthy);
    LeaseExpiryCheckpoint before = previous.orElse(null);
    long timeFence = observation.map(LeaseTimeObservation::nowLowerEpochMillis).orElse(0L);
    Position position = idleAt(0);
    if (before != null) {
      if (before.bucketCount() != bucketCount
          || observation.isPresent() && observation.orElseThrow().nowLowerEpochMillis()
              < before.trustedTimeLowerEpochMillis()) {
        throw new LeaseFailure(LeaseFailure.Reason.CLOCK_UNHEALTHY);
      }
      timeFence = Math.max(timeFence, before.trustedTimeLowerEpochMillis());
      position = before.position();
    }
    if (position.windowStopExclusive() > timeFence) {
      throw new LeaseFailure(LeaseFailure.Reason.CLOCK_UNHEALTHY);
    }
    return new LeaseExpiryCheckpoint(ownerEpoch, sequence, bucketCount, baseSequence,
        timeFence, position);
  }

  public static Position idleAt(long deadlineEpochMillis) {
    return new Position(deadlineEpochMillis, deadlineEpochMillis, 0, Optional.empty());
  }

  /** Returns the exclusive stop needed to include every deadline at or before this time. */
  public static long closedWindowStopExclusive(long trustedTimeLowerEpochMillis) {
    if (trustedTimeLowerEpochMillis < 0) {
      throw new IllegalArgumentException("trusted Lease time must be non-negative");
    }
    return trustedTimeLowerEpochMillis == Long.MAX_VALUE
        ? Long.MAX_VALUE : trustedTimeLowerEpochMillis + 1;
  }

  public static void requirePositionProgress(Position before, Position after,
      long trustedTimeLowerEpochMillis, int bucketCount) {
    Objects.requireNonNull(before, "before");
    Objects.requireNonNull(after, "after");
    if (bucketCount < 1 || bucketCount > MAX_BUCKETS) {
      throw new IllegalArgumentException("invalid Lease expiry bucket count");
    }
    if (!isClosedBy(after.windowStopExclusive(), trustedTimeLowerEpochMillis)) {
      throw new IllegalArgumentException("Lease expiry window exceeds its trusted time fence");
    }
    if (before.windowStartInclusive() == after.windowStartInclusive()
        && before.windowStopExclusive() == after.windowStopExclusive()
        && before.bucket() == after.bucket()) {
      if (after.afterIdentity().equals(before.afterIdentity())
          || advances(after.afterIdentity(), before.afterIdentity())) {
        return;
      }
      throw new IllegalArgumentException("Lease expiry candidate cursor regressed");
    }
    if (before.idle()) {
      if (after.idle() && after.windowStartInclusive() == before.windowStartInclusive()) return;
      if (after.windowStartInclusive() == before.windowStartInclusive()
          && after.windowStopExclusive() > before.windowStopExclusive()
          && after.bucket() == 0 && after.afterIdentity().isEmpty()) return;
      throw new IllegalArgumentException("Lease expiry checkpoint opened a non-contiguous window");
    }
    if (after.windowStartInclusive() == before.windowStartInclusive()
        && after.windowStopExclusive() == before.windowStopExclusive()) {
      if (after.bucket() == before.bucket() + 1 && after.afterIdentity().isEmpty()) return;
      if (before.bucket() == bucketCount - 1 && after.idle()
          && after.windowStartInclusive() == before.windowStopExclusive()) return;
    }
    throw new IllegalArgumentException("Lease expiry checkpoint regressed or skipped coverage");
  }

  /** Requires a scan step to advance; unchanged cursors are reserved for Lease mutations. */
  public static void requireScanProgress(Position before, Position after,
      long trustedTimeLowerEpochMillis, int bucketCount) {
    requirePositionProgress(before, after, trustedTimeLowerEpochMillis, bucketCount);
    if (before.equals(after)) {
      throw new IllegalArgumentException("Lease expiry scan step did not advance its cursor");
    }
  }

  private static boolean advances(Optional<ByteKey> replacement, Optional<ByteKey> previous) {
    if (previous.isEmpty()) return replacement.isPresent();
    return replacement.isPresent()
        && replacement.orElseThrow().compareTo(previous.orElseThrow()) > 0;
  }

  private static boolean isClosedBy(long windowStopExclusive, long trustedTimeLowerEpochMillis) {
    return windowStopExclusive <= closedWindowStopExclusive(trustedTimeLowerEpochMillis);
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("Lease expiry checkpoint violates its storage contract");
  }
}
