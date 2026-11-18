package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ProgressFrontierTest {
  @Test
  void emptyWatchSetDoesNotProduceProgress() {
    assertEquals(OptionalLong.empty(), ProgressFrontier.forAllActive(List.of()));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(), 1));
  }

  @Test
  void filteredEmptyRangeNeedsAContiguousVerifiedFrontier() {
    var withoutCoverage = watcher(1, Optional.empty(), true, true, false);
    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(withoutCoverage), 1));

    var verifiedEmptyRange = watcher(1, Optional.of(frontier(8, 12)), true, true, false);
    assertEquals(OptionalLong.of(12),
        ProgressFrontier.forWatcher(List.of(verifiedEmptyRange), 1));
  }

  @Test
  void futureStartMustBeReachedBeforeProgress() {
    var waiting = watcher(1, Optional.of(frontier(8, 12)), true, false, false);

    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(waiting), 1));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forAllActive(List.of(waiting)));
  }

  @Test
  void historyReplayMustCompleteBeforeProgress() {
    var replaying = watcher(1, Optional.of(frontier(8, 12)), false, true, false);

    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(replaying), 1));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forAllActive(List.of(replaying)));
  }

  @Test
  void pendingMatchingEventBlocksProgressThroughItsFrontier() {
    var pending = watcher(1, Optional.of(frontier(8, 12)), true, true, true);

    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(pending), 1));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forAllActive(List.of(pending)));
  }

  @Test
  void allWatchProgressReturnsTheMinimumSafeKvRevision() {
    var aheadInSequence = watcher(1, Optional.of(frontier(100, 9)), true, true, false);
    var aheadInRevision = watcher(2, Optional.of(frontier(98, 7)), true, true, false);

    assertEquals(OptionalLong.of(7),
        ProgressFrontier.forAllActive(List.of(aheadInSequence, aheadInRevision)));
    assertEquals(OptionalLong.of(7),
        ProgressFrontier.forWatcher(List.of(aheadInSequence, aheadInRevision), 2));
  }

  @Test
  void metadataOnlySequenceAdvanceDoesNotInventKvRevision() {
    var metadataOnlyAdvance = watcher(1, Optional.of(frontier(23, 6)), true, true, false);

    assertEquals(OptionalLong.of(6),
        ProgressFrontier.forWatcher(List.of(metadataOnlyAdvance), 1));
  }

  @Test
  void rawProtoNegativeWatchIdCanAdvertiseProgress() {
    var rawWatch = watcher(-1, Optional.of(frontier(23, 6)), true, true, false);

    assertEquals(OptionalLong.of(6), ProgressFrontier.forWatcher(List.of(rawWatch), -1));
    assertEquals(OptionalLong.of(6), ProgressFrontier.forAllActive(List.of(rawWatch)));
  }

  @Test
  void aBlockedWatchDoesNotPreventAnotherWatchFromReportingItsOwnFrontier() {
    var ready = watcher(1, Optional.of(frontier(12, 10)), true, true, false);
    var waiting = watcher(2, Optional.of(frontier(13, 11)), true, false, false);

    assertEquals(OptionalLong.of(10), ProgressFrontier.forWatcher(List.of(ready, waiting), 1));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forAllActive(List.of(ready, waiting)));
  }

  @Test
  void closedWatchDoesNotBlockTheRemainingActiveWatches() {
    var active = watcher(1, Optional.of(frontier(12, 10)), true, true, false);
    var closed = new ProgressFrontier.WatcherState(
        2, Optional.of(frontier(8, 4)), false, false, false, true);

    assertEquals(OptionalLong.of(10), ProgressFrontier.forAllActive(List.of(active, closed)));
    assertEquals(OptionalLong.empty(), ProgressFrontier.forWatcher(List.of(closed), 2));
  }

  @Test
  void rejectsInvalidFrontiersAndAmbiguousWatcherSnapshots() {
    assertThrows(IllegalArgumentException.class, () -> frontier(-1, 1));
    assertThrows(IllegalArgumentException.class, () -> frontier(0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> ProgressFrontier.forAllActive(List.of(
            watcher(1, Optional.of(frontier(3, 4)), true, true, false),
            watcher(1, Optional.of(frontier(3, 4)), true, true, false))));
  }

  private static ProgressFrontier.VerifiedFrontier frontier(long sequence, long revision) {
    return new ProgressFrontier.VerifiedFrontier(sequence, revision);
  }

  private static ProgressFrontier.WatcherState watcher(
      long watchId,
      Optional<ProgressFrontier.VerifiedFrontier> frontier,
      boolean replayComplete,
      boolean futureStartReached,
      boolean hasPendingMatchingEvents) {
    return new ProgressFrontier.WatcherState(watchId, frontier, true, replayComplete,
        futureStartReached, hasPendingMatchingEvents);
  }
}
