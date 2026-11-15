package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseExpiryCheckpointTest {
  @Test
  void checkpointRoundTripsAndAdvancesWithinOneBucket() {
    var start = checkpoint(4, 11, 8,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2, Optional.empty()));
    var next = checkpoint(4, 12, 8,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2,
            Optional.of(candidate(1_200, 19).encode())));

    assertEquals(start, LeaseExpiryCheckpoint.decode(start.encode()));
    assertEquals(next, LeaseExpiryCheckpoint.decode(next.encode()));
    LeaseExpiryCheckpoint.requireForwardProgress(start, next);
  }

  @Test
  void checkpointCanCloseAWindowAndOpenTheNextWithoutAnOverlap() {
    var completed = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 1_000, 0, Optional.empty()));
    var nextWindow = checkpoint(5, 12, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 2_000, 0, Optional.empty()));

    LeaseExpiryCheckpoint.requireForwardProgress(completed, nextWindow);
  }

  @Test
  void closedWindowIncludesDeadlineEqualToTrustedLowerBound() {
    var checkpoint = new LeaseExpiryCheckpoint(4, 11, 4, 10, 5_000,
        new LeaseExpiryCheckpoint.Position(4_000, 5_001, 2, Optional.empty()));

    assertEquals(5_001, LeaseExpiryCheckpoint.closedWindowStopExclusive(5_000));
    assertEquals(checkpoint, LeaseExpiryCheckpoint.decode(checkpoint.encode()));
    assertThrows(IllegalArgumentException.class, () -> new LeaseExpiryCheckpoint(
        4, 12, 4, 11, 5_000,
        new LeaseExpiryCheckpoint.Position(4_000, 5_002, 2, Optional.empty())));
  }

  @Test
  void closedWindowStopSaturatesWithoutOverflow() {
    assertEquals(Long.MAX_VALUE,
        LeaseExpiryCheckpoint.closedWindowStopExclusive(Long.MAX_VALUE));
  }

  @Test
  void rejectsBackwardWindowAndMalformedCandidateCursor() {
    var current = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2,
            Optional.of(candidate(2_000, 19).encode())));
    var backward = checkpoint(4, 12, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2,
            Optional.of(candidate(1_500, 18).encode())));

    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(current, backward));
    assertThrows(IllegalArgumentException.class, () -> new LeaseExpiryCheckpoint.Position(
        1_000, 5_000, 2, Optional.of(candidate(5_000, 20).encode())));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.decode(ByteKey.copyOf(new byte[] {0, 0, 0, 2})));
  }

  @Test
  void rejectsSkippedBucketsAndHorizonExtensionBeforeTheActiveWindowIsComplete() {
    var current = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 1,
            Optional.of(candidate(1_200, 19).encode())));
    var skippedBucket = checkpoint(4, 12, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 3, Optional.empty()));
    var extendedWindow = checkpoint(4, 12, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 6_000, 0, Optional.empty()));

    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(current, skippedBucket));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(current, extendedWindow));
  }

  @Test
  void cannotCloseAWindowBeforeEveryBucketIsExhausted() {
    var middleBucket = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2, Optional.empty()));
    var incorrectlyClosed = checkpoint(4, 12, 4,
        LeaseExpiryCheckpoint.idleAt(5_000));

    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(middleBucket, incorrectlyClosed));
  }

  @Test
  void lastBucketCanCloseItsWindowOnlyAtTheNextWindowBoundary() {
    var lastBucket = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 3,
            Optional.of(candidate(4_900, 19).encode())));
    var nextWindow = checkpoint(4, 12, 4, LeaseExpiryCheckpoint.idleAt(5_000));

    LeaseExpiryCheckpoint.requireForwardProgress(lastBucket, nextWindow);
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(
            checkpoint(4, 11, 4,
                new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2, Optional.empty())),
            nextWindow));
  }

  @Test
  void scanStepsMustAdvanceButLeaseMutationsMayRetainTheCursor() {
    var cursor = new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2, Optional.empty());
    LeaseExpiryCheckpoint.requirePositionProgress(cursor, cursor, 5_000, 4);

    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireScanProgress(cursor, cursor, 5_000, 4));
  }

  @Test
  void leaseMutationRetainsWindowClosedAtTheTrustedTimeBoundaryWithoutNewObservation() {
    var previous = new LeaseExpiryCheckpoint(4, 11, 4, 10, 5_000,
        new LeaseExpiryCheckpoint.Position(4_000, 5_001, 2, Optional.empty()));

    var updated = LeaseExpiryCheckpoint.afterMutation(5, 12, 4, 11,
        Optional.of(previous), Optional.empty());

    assertEquals(previous.position(), updated.position());
    assertEquals(previous.trustedTimeLowerEpochMillis(), updated.trustedTimeLowerEpochMillis());
    LeaseExpiryCheckpoint.requireForwardProgress(previous, updated);
  }

  @Test
  void rejectsSaltProfileChangesAndMalformedBucketPositions() {
    var current = checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 1, Optional.empty()));
    var changedProfile = checkpoint(4, 12, 8,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 2, Optional.empty()));

    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryCheckpoint.requireForwardProgress(current, changedProfile));
    assertThrows(IllegalArgumentException.class, () -> checkpoint(4, 11, 4,
        new LeaseExpiryCheckpoint.Position(1_000, 5_000, 4, Optional.empty())));
  }

  private static LeaseExpiryCheckpoint checkpoint(long ownerEpoch, long sequence, int buckets,
      LeaseExpiryCheckpoint.Position position) {
    return new LeaseExpiryCheckpoint(ownerEpoch, sequence, buckets, sequence - 1,
        Math.max(5_000, position.windowStopExclusive()), position);
  }

  private static ExpiryCandidateIdentity candidate(long deadline, long leaseId) {
    return new ExpiryCandidateIdentity(deadline, leaseId, 1, 0);
  }
}
