package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import org.junit.jupiter.api.Test;

class EventIndexBackfillCheckpointTest {
  private final ByteKey digest = ByteKey.copyOf(new byte[32]);
  private final ByteKey root = ByteKey.copyOf(new byte[] {1});

  @Test
  void advancesContiguousMetadataAndKvDecisionsAndSurvivesEncoding() {
    var initial = new EventIndexBackfillCheckpoint(1, 8, 2, 7, 2, 10, 10, 11, 4);
    var metadata = decision(2, 10, false);
    var advanced = initial.advance(1, 9, metadata);
    EventIndexBackfillCheckpoint.requireForwardProgress(initial, advanced);
    assertEquals(advanced, EventIndexBackfillCheckpoint.decode(advanced.encode()));
    assertEquals(9, EventIndexBackfillCheckpoint.sequence(EventIndexBackfillCheckpoint.versionIdentity(9)));
    assertFalse(advanced.complete());
    for (long sequence = 3; sequence <= 7; sequence++) {
      advanced = advanced.advance(1, sequence + 7, decision(sequence, 10, sequence == 7));
    }
    assertTrue(advanced.complete());
    assertEquals(11, advanced.currentRevision());
  }

  @Test
  void rejectsGapsRegressionsAndFalseCompletion() {
    var initial = new EventIndexBackfillCheckpoint(2, 8, 2, 7, 2, 10, 10, 11, 4);
    assertThrows(IllegalArgumentException.class, () -> initial.advance(2, 9, decision(3, 10, false)));
    assertThrows(IllegalArgumentException.class, () -> initial.advance(1, 9, decision(2, 10, false)));
    assertThrows(IllegalArgumentException.class, () -> initial.advance(2, 8, decision(2, 10, false)));
    assertThrows(IllegalArgumentException.class,
        () -> new EventIndexBackfillCheckpoint(2, 9, 2, 7, 8, 10, 10, 11, 4));
    assertThrows(IllegalArgumentException.class, () -> EventIndexBackfillCheckpoint.decode(ByteKey.copyOf(new byte[2])));
    assertThrows(IllegalArgumentException.class,
        () -> EventIndexBackfillCheckpoint.sequence(ByteKey.copyOf(new byte[EventIndexBackfillCheckpoint.IDENTITY_BYTES])));
  }

  private CommitDecision decision(long sequence, long baseRevision, boolean changesKey) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), sequence, baseRevision,
        baseRevision + (changesKey ? 1 : 0), root, digest, changesKey);
  }
}
