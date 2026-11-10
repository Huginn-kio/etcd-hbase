package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RangeSelectorTest {
  @Test
  void limitAndMoreApplyAfterTombstonesAndRevisionFilters() {
    var selector = selector(1, false, new RangeCommand.RevisionFilter(5, 0, 0, 0));
    assertEquals(RangeSelector.Action.SKIP, selector.observe(record("a", 2, false)));
    assertEquals(RangeSelector.Action.SKIP, selector.observe(record("b", 6, true)));
    assertEquals(RangeSelector.Action.INCLUDE, selector.observe(record("c", 5, false)));
    assertFalse(selector.more());
    assertEquals(RangeSelector.Action.STOP, selector.observe(record("d", 6, false)));
    assertTrue(selector.more());
    assertEquals(1, selector.included());
    assertEquals(3, selector.observedLive());
    assertThrows(IllegalStateException.class, () -> selector.observe(record("e", 7, false)));
  }

  @Test
  void countOnlyUsesExistenceWithoutApplyingRevisionFilters() {
    var selector = selector(1, true, new RangeCommand.RevisionFilter(0, -1, 0, 0));
    assertEquals(RangeSelector.Action.SKIP, selector.observe(record("a", 2, true)));
    assertEquals(RangeSelector.Action.STOP, selector.observe(record("b", 2, false)));
    assertEquals(1, selector.observedLive());
    assertEquals(0, selector.included());
    assertFalse(selector.more());
  }

  @Test
  void exhaustedFilteredTailDoesNotSetMoreAndUnsupportedCountsAreRejected() {
    var selector = selector(1, false, new RangeCommand.RevisionFilter(5, 0, 0, 0));
    assertEquals(RangeSelector.Action.INCLUDE, selector.observe(record("a", 5, false)));
    assertEquals(RangeSelector.Action.SKIP, selector.observe(record("b", 6, true)));
    assertEquals(RangeSelector.Action.SKIP, selector.observe(record("c", 2, false)));
    assertFalse(selector.more());
    assertEquals(KvFailure.Reason.UNSUPPORTED_OPERATION, assertThrows(KvFailure.class, () ->
        selector(0, true, new RangeCommand.RevisionFilter(0, 0, 0, 0))).reason());
  }

  @Test
  void unlimitedAndExtremeLimitsDoNotOverflowOrHidePreFilterCount() {
    for (long limit : new long[] {0, -1, Long.MIN_VALUE, Long.MAX_VALUE}) {
      var selector = selector(limit, false, new RangeCommand.RevisionFilter(3, 0, 0, 0));
      assertEquals(RangeSelector.Action.SKIP, selector.observe(record("a", 2, false)));
      assertEquals(RangeSelector.Action.INCLUDE, selector.observe(record("b", 3, false)));
      assertEquals(2, selector.observedLive());
      assertEquals(1, selector.included());
      assertFalse(selector.more());
    }
  }

  @Test
  void aRecordOutsideTheRequestedIntervalIsRejected() {
    var command = new RangeCommand(KeyRange.point(key("b")), RangeCommand.Context.STANDALONE,
        0, 0, new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0),
        false, false, false);
    var selector = new RangeSelector(command);
    assertThrows(IllegalStateException.class, () -> selector.observe(record("a", 2, false)));
  }

  private static RangeSelector selector(long limit, boolean countOnly, RangeCommand.RevisionFilter filter) {
    return new RangeSelector(new RangeCommand(KeyRange.all(), RangeCommand.Context.STANDALONE,
        0, limit, new RangeCommand.Sort(0, 0), filter, false, false, countOnly));
  }

  private static StoredRecord record(String key, long revision, boolean tombstone) {
    return StoredRecord.metadataOnly(key(key),
        new StorageMetadata(1, revision, 1, 0,
            tombstone ? StorageMetadata.State.TOMBSTONE : StorageMetadata.State.LIVE, tombstone ? 0 : 1));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8)); }
}
