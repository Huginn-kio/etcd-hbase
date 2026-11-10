package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeletePlannerTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff});

  @Test
  void plansARealTombstoneAndRetainsTheCompletePreviousValueForTheEvent() {
    StoredRecord old = live(2, 5, 3, 0);
    var plan = new DeletePlanner().plan(invocation(5), Optional.of(old), 7).orElseThrow();
    assertEquals(KEY, plan.candidate().record().key());
    assertEquals(new StorageMetadata(2, 6, 3, 0, StorageMetadata.State.TOMBSTONE, 0),
        plan.candidate().record().metadata());
    assertTrue(plan.candidate().record().value().isEmpty());
    assertEquals(PreparedEvent.Kind.DELETE, plan.event().kind());
    assertEquals(7, plan.event().ordinal());
    assertEquals(old, plan.event().previous().orElseThrow());
  }

  @Test
  void absentAndAlreadyDeletedRecordsDoNotProduceAnEvent() {
    var planner = new DeletePlanner();
    assertTrue(planner.plan(invocation(5), Optional.empty(), 0).isEmpty());
    StoredRecord deleted = StoredRecord.metadataOnly(KEY,
        new StorageMetadata(2, 5, 3, 0, StorageMetadata.State.TOMBSTONE, 0));
    assertTrue(planner.plan(invocation(5), Optional.of(deleted), 0).isEmpty());
    StoredRecord earlierOperation = StoredRecord.metadataOnly(KEY,
        new StorageMetadata(2, 6, 3, 0, StorageMetadata.State.TOMBSTONE, 0));
    assertTrue(planner.planWorking(invocation(5), Optional.of(earlierOperation), 1).isEmpty());
    assertThrows(IllegalArgumentException.class,
        () -> planner.plan(invocation(5), Optional.of(earlierOperation), 0));
  }

  @Test
  void rejectsUnresolvedLeaseMissingValuesFutureVersionsAndOverflow() {
    var planner = new DeletePlanner();
    KvFailure lease = assertThrows(KvFailure.class,
        () -> planner.plan(invocation(5), Optional.of(live(2, 5, 3, 7)), 0));
    assertEquals(KvFailure.Reason.LEASE_UNAVAILABLE, lease.reason());
    assertThrows(IllegalArgumentException.class,
        () -> planner.plan(invocation(5), Optional.of(live(2, 6, 3, 0)), 0));
    assertThrows(IllegalArgumentException.class, () -> planner.plan(invocation(5), Optional.of(
        StoredRecord.metadataOnly(KEY, new StorageMetadata(2, 7, 3, 0, StorageMetadata.State.TOMBSTONE, 0))), 0));
    assertThrows(IllegalArgumentException.class, () -> planner.plan(invocation(5), Optional.of(
        StoredRecord.metadataOnly(KEY, new StorageMetadata(2, 5, 3, 0, StorageMetadata.State.LIVE, 1))), 0));
    assertThrows(IllegalArgumentException.class,
        () -> planner.plan(invocation(5), Optional.of(live(2, 5, 3, 0)), -1));
    assertThrows(ArithmeticException.class,
        () -> planner.plan(invocation(Long.MAX_VALUE), Optional.of(live(2, 5, 3, 0)), 0));
  }

  private static InvocationBinding invocation(long baseRevision) {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, baseRevision);
  }

  private static StoredRecord live(long create, long mod, long version, long lease) {
    return StoredRecord.live(KEY, new StorageMetadata(create, mod, version, lease,
        StorageMetadata.State.LIVE, 2), new byte[] {0, (byte) 0xff});
  }
}
