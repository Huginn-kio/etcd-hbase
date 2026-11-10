package io.etcdhbase.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ScanRequestDirectionTest {
  private final KeyRange range = KeyRange.between(key(1), key(9));

  @Test
  void legacyConstructionRemainsAscendingAndExcludesTheConsumedKey() {
    var request = new ScanRequest(1, range, Optional.of(key(4)), 7,
        ReadProjection.METADATA, 2, 4096, 16);
    assertEquals(ScanDirection.ASCENDING, request.direction());
    assertEquals(Optional.of(key(4)), request.after());
    assertEquals(key(4), request.checkpoint().orElseThrow().key());
  }

  @Test
  void checkpointIsBoundToDirectionRevisionBucketAndLogicalBounds() {
    var initial = new ScanRequest(1, range, Optional.empty(), 7,
        ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING);
    var checkpoint = ScanCheckpoint.after(initial, key(4));
    var resumed = initial.resumeFrom(checkpoint);
    assertEquals(Optional.of(key(4)), resumed.after());
    for (var other : new ScanRequest[] {
        new ScanRequest(1, range, Optional.empty(), 7, ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING),
        new ScanRequest(1, range, Optional.empty(), 8, ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING),
        new ScanRequest(1, range, Optional.empty(), 7, ReadProjection.METADATA, 2, 4096, 16, ScanDirection.ASCENDING),
        new ScanRequest(2, range, Optional.empty(), 7, ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING),
        new ScanRequest(1, KeyRange.all(), Optional.empty(), 7, ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING)}) {
      assertThrows(IllegalArgumentException.class, () -> other.resumeFrom(checkpoint));
    }
    var progress = PhysicalScanProgress.after(initial, key(5));
    assertEquals(Optional.of(progress), initial.resumePhysical(progress).physicalProgress());
    var otherView = new ScanRequest(1, range, Optional.empty(), 7,
        ReadProjection.METADATA, 2, 4096, 16, ScanDirection.DESCENDING);
    assertThrows(IllegalArgumentException.class, () -> otherView.resumePhysical(progress));
    assertThrows(IllegalArgumentException.class, () -> ScanCheckpoint.after(initial, key(9)));
  }

  private static ByteKey key(int value) { return ByteKey.copyOf(new byte[] {(byte) value}); }
}
