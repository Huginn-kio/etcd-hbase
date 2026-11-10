package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.storage.StorageMetadata;
import org.junit.jupiter.api.Test;

class RangeCommandTest {
  private static final StorageMetadata VERSION =
      new StorageMetadata(3, 7, 2, 0, StorageMetadata.State.LIVE, 4);

  @Test
  void revisionFilterAppliesEveryNonzeroBoundaryInclusively() {
    assertTrue(filter(0, 0, 0, 0).matches(VERSION));
    assertTrue(filter(7, 7, 3, 3).matches(VERSION));
    assertFalse(filter(8, 0, 0, 0).matches(VERSION));
    assertFalse(filter(0, 6, 0, 0).matches(VERSION));
    assertFalse(filter(0, 0, 4, 0).matches(VERSION));
    assertFalse(filter(0, 0, 0, 2).matches(VERSION));
    assertFalse(filter(1, 6, 2, 4).matches(VERSION));
  }

  @Test
  void negativeBoundariesRemainRealBoundaries() {
    assertTrue(filter(-1, 0, -1, 0).matches(VERSION));
    assertFalse(filter(0, -1, 0, 0).matches(VERSION));
    assertFalse(filter(0, 0, 0, Long.MIN_VALUE).matches(VERSION));
    assertTrue(filter(Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE).matches(VERSION));
  }

  private static RangeCommand.RevisionFilter filter(long minMod, long maxMod, long minCreate, long maxCreate) {
    return new RangeCommand.RevisionFilter(minMod, maxMod, minCreate, maxCreate);
  }
}
