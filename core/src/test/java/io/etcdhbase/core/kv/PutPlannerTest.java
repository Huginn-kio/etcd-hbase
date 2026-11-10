package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PutPlannerTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff});

  @Test
  void createsUpdatesAndRecreatesWithCorrectRevisionsAndVersions() {
    InvocationBinding invocation = invocation(5);
    PutPlanner planner = new PutPlanner();
    var created = planner.plan(command(new byte[] {1}, true, false, false), invocation, Optional.empty());
    assertEquals(new StorageMetadata(6, 6, 1, 0, StorageMetadata.State.LIVE, 1),
        created.candidate().record().metadata());
    assertTrue(created.result().previous().isEmpty());
    StoredRecord old = live(2, 5, 3, 0, new byte[] {1});
    var sameValue = planner.plan(command(new byte[] {1}, true, false, false), invocation, Optional.of(old));
    assertEquals(new StorageMetadata(2, 6, 4, 0, StorageMetadata.State.LIVE, 1),
        sameValue.candidate().record().metadata());
    assertEquals(old, sameValue.result().previous().orElseThrow());
    assertEquals(old, sameValue.event().previous().orElseThrow());
    StoredRecord tombstone = StoredRecord.metadataOnly(KEY,
        new StorageMetadata(2, 5, 3, 0, StorageMetadata.State.TOMBSTONE, 0));
    var recreated = planner.plan(command(new byte[0], true, false, false), invocation, Optional.of(tombstone));
    assertEquals(new StorageMetadata(6, 6, 1, 0, StorageMetadata.State.LIVE, 0),
        recreated.candidate().record().metadata());
    assertTrue(recreated.result().previous().isEmpty());
    assertTrue(recreated.event().previous().isEmpty());
  }

  @Test
  void preservesValueOnlyWhenRequestedAndRetainsEventPreviousWithoutResponsePrevious() {
    StoredRecord old = live(2, 5, 3, 0, new byte[] {0, (byte) 0xff});
    var plan = new PutPlanner().plan(command(new byte[0], false, true, true), invocation(5), Optional.of(old));
    assertArrayEquals(new byte[] {0, (byte) 0xff}, plan.candidate().record().value().orElseThrow());
    assertTrue(plan.result().previous().isEmpty());
    assertEquals(old, plan.event().previous().orElseThrow());
  }

  @Test
  void rejectsMissingIgnoreLeaseUnavailableAndInconsistentBase() {
    PutPlanner planner = new PutPlanner();
    KvFailure missing = assertThrows(KvFailure.class, () -> planner.plan(
        command(new byte[0], false, true, false), invocation(5), Optional.empty()));
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND, missing.reason());
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND, assertThrows(KvFailure.class, () -> planner.plan(
        command(new byte[0], false, false, true), invocation(5), Optional.empty())).reason());
    PutCommand explicitLease = new PutCommand(KEY, new byte[] {1}, -5, false, false, false);
    assertEquals(KvFailure.Reason.LEASE_UNAVAILABLE, assertThrows(KvFailure.class,
        () -> planner.plan(explicitLease, invocation(5), Optional.empty())).reason());
    assertEquals(KvFailure.Reason.LEASE_UNAVAILABLE, assertThrows(KvFailure.class,
        () -> planner.plan(command(new byte[] {1}, false, false, false), invocation(5),
            Optional.of(live(1, 3, 1, 7, new byte[] {1})))).reason());
    assertThrows(IllegalArgumentException.class, () -> planner.plan(
        command(new byte[] {1}, false, false, false), invocation(5),
        Optional.of(live(1, 6, 1, 0, new byte[] {1}))));
    assertThrows(IllegalArgumentException.class, () -> planner.plan(
        command(new byte[] {1}, false, false, false), invocation(5),
        Optional.of(StoredRecord.metadataOnly(KEY,
            new StorageMetadata(1, 5, 1, 0, StorageMetadata.State.LIVE, 1)))));
    assertThrows(IllegalArgumentException.class, () -> planner.plan(
        command(new byte[] {1}, false, false, false), invocation(5),
        Optional.of(StoredRecord.live(ByteKey.copyOf(new byte[] {'x'}),
            new StorageMetadata(1, 5, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {1}))));
    assertThrows(ArithmeticException.class, () -> planner.plan(
        command(new byte[] {1}, false, false, false), invocation(Long.MAX_VALUE), Optional.empty()));
    assertThrows(ArithmeticException.class, () -> planner.plan(
        command(new byte[] {1}, false, false, false), invocation(5),
        Optional.of(live(1, 5, Long.MAX_VALUE, 0, new byte[] {1}))));
  }

  private static PutCommand command(byte[] value, boolean prev, boolean ignoreValue, boolean ignoreLease) {
    return new PutCommand(KEY, value, 0, prev, ignoreValue, ignoreLease);
  }

  private static InvocationBinding invocation(long baseRevision) {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, baseRevision);
  }

  private static StoredRecord live(long create, long mod, long version, long lease, byte[] value) {
    return StoredRecord.live(KEY, new StorageMetadata(create, mod, version, lease,
        StorageMetadata.State.LIVE, value.length), value);
  }
}
