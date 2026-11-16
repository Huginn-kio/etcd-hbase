package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LeaseBindingStatePlannerTest {
  private static final long A = -7;
  private static final long B = 9;
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {1});
  private static final long HISTORY_BYTES = (16L << 10) + 8;
  private final LeaseBindingStatePlanner planner =
      new LeaseBindingStatePlanner(new LeaseCostAdmission(LeaseCostAdmission.DEFAULT_LIMITS));

  @Test
  void attachingUpdatingAndRecreatingChargeOnlyNewBindingHistory() throws Exception {
    LeaseState initial = state(A, 0, 0, 0);
    LeaseState attached = planner.plan(11, List.of(change(0, A, 0, 3, 1)),
        Map.of(A, initial), Set.of()).get(A);
    assertAccounting(attached, 1, HISTORY_BYTES + 12, HISTORY_BYTES);

    LeaseState updated = planner.plan(12, List.of(change(A, A, 3, 5, 1)),
        Map.of(A, attached), Set.of()).get(A);
    assertAccounting(updated, 1, HISTORY_BYTES + 20, HISTORY_BYTES);

    LeaseState recreated = planner.plan(13, List.of(change(A, A, 5, 2, 6)),
        Map.of(A, updated), Set.of()).get(A);
    assertAccounting(recreated, 1, HISTORY_BYTES + 8, 2 * HISTORY_BYTES);
    assertEquals(0, initial.boundKeyCount());
    assertEquals(10, initial.sequence());
  }

  @Test
  void rebindingAndUnbindingChargeEachLeaseItsOwnHistoryVersion() throws Exception {
    Map<Long, LeaseState> before = Map.of(A, state(A, 1, 3, HISTORY_BYTES), B, state(B, 0, 0, 0));
    Map<Long, LeaseState> rebound = planner.plan(11, List.of(change(A, B, 3, 5, 1)), before, Set.of());
    assertAccounting(rebound.get(A), 0, 0, 2 * HISTORY_BYTES);
    assertAccounting(rebound.get(B), 1, HISTORY_BYTES + 20, HISTORY_BYTES);

    LeaseState unbound = planner.plan(12, List.of(change(B, 0, 5, 0, 1)), rebound, Set.of()).get(B);
    assertAccounting(unbound, 0, 0, 2 * HISTORY_BYTES);
    assertEquals(LeaseState.Status.ACTIVE, unbound.status());
    assertEquals(1, before.get(A).boundKeyCount());
  }

  @Test
  void revocationRemovesLiveCostsAndRetainsHistoryAndExpiryIdentity() throws Exception {
    LeaseState previous = state(A, 1, 3, HISTORY_BYTES);
    LeaseState revoked = planner.plan(11, List.of(change(A, 0, 3, 0, 1)),
        Map.of(A, previous), Set.of(A)).get(A);
    assertAccounting(revoked, 0, 0, 2 * HISTORY_BYTES);
    assertEquals(LeaseState.Status.REVOKED, revoked.status());
    assertEquals(previous.identity(), revoked.identity());
    assertEquals(previous.renewalGeneration(), revoked.renewalGeneration());
    assertEquals(previous.deadlineEpochMillis(), revoked.deadlineEpochMillis());
    assertEquals(previous.expiryCandidateSequence(), revoked.expiryCandidateSequence());
    assertEquals(11, revoked.sequence());
  }

  @Test
  void revocationCannotLeaveLiveBindingsOrReservedCost() {
    assertThrows(IllegalStateException.class, () -> planner.plan(11, List.of(),
        Map.of(A, state(A, 1, 3, HISTORY_BYTES)), Set.of(A)));
  }

  @Test
  void emptyRevocationRetainsAlreadyChargedHistory() throws Exception {
    LeaseState revoked = planner.plan(11, List.of(),
        Map.of(A, state(A, 0, 0, 2 * HISTORY_BYTES)), Set.of(A)).get(A);
    assertAccounting(revoked, 0, 0, 2 * HISTORY_BYTES);
    assertEquals(LeaseState.Status.REVOKED, revoked.status());
  }

  @Test
  void attachmentReservesFutureUnbindAtTheExactCapacityBoundary() throws Exception {
    var exact = new LeaseBindingStatePlanner(new LeaseCostAdmission(
        new LeaseCostAdmission.Limits(1, 1, 2 * HISTORY_BYTES)));
    var tooSmall = new LeaseBindingStatePlanner(new LeaseCostAdmission(
        new LeaseCostAdmission.Limits(1, 1, 2 * HISTORY_BYTES - 1)));
    Map<Long, LeaseState> before = Map.of(A, state(A, 0, 0, 0));
    List<LeaseBindingChange> changes = List.of(change(0, A, 0, 0, 1));
    assertAccounting(exact.plan(11, changes, before, Set.of()).get(A),
        1, HISTORY_BYTES, HISTORY_BYTES);
    BackendFailure failure = assertThrows(BackendFailure.class,
        () -> tooSmall.plan(11, changes, before, Set.of()));
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
    assertEquals(0, before.get(A).bindingHistoryBytes());
  }

  @Test
  void resultsAreOrderedImmutableAndDoNotRetainRequestState() throws Exception {
    var other = new LeaseBindingChange(ByteKey.copyOf(new byte[] {2}), 0, B, 0, 0, 0, 1, 6, 0);
    var changes = List.of(other, change(0, A, 0, 0, 1));
    var base = Map.of(B, state(B, 0, 0, 0), A, state(A, 0, 0, 0));
    Map<Long, LeaseState> planned = planner.plan(11, changes, base, Set.of());
    assertEquals(List.of(A, B), new ArrayList<>(planned.keySet()));
    assertThrows(UnsupportedOperationException.class, () -> planned.put(A, base.get(A)));
    assertEquals(planned, planner.plan(11, changes, base, Set.of()));
    assertEquals(0, base.get(A).boundKeyCount());
    assertEquals(0, base.get(B).boundKeyCount());
  }

  @Test
  void invalidBaseAndAccountingUnderflowFailWithoutAPlan() {
    var attach = List.of(change(0, A, 0, 0, 1));
    assertThrows(IllegalArgumentException.class,
        () -> planner.plan(10, attach, Map.of(A, state(A, 0, 0, 0)), Set.of()));
    assertThrows(IllegalArgumentException.class,
        () -> planner.plan(11, attach, Map.of(A, state(B, 0, 0, 0)), Set.of()));
    LeaseState revoked = state(A, 0, 0, 0).revoke(11);
    assertThrows(IllegalStateException.class,
        () -> planner.plan(12, attach, Map.of(A, revoked), Set.of()));
    assertThrows(IllegalStateException.class, () -> planner.plan(11,
        List.of(change(A, 0, 3, 0, 1)), Map.of(A, state(A, 0, 0, 0)), Set.of()));
  }

  @Test
  void bindingCountOverflowDoesNotWrapIntoAnAdmissiblePlan() {
    LeaseState overflowing = new LeaseState(new LeaseIdentity(A, 1), 60, 60_000, 2,
        LeaseState.Status.ACTIVE, 10, 3, Long.MAX_VALUE, 0, 0, 0);
    assertThrows(ArithmeticException.class, () -> planner.plan(11,
        List.of(change(0, A, 0, 0, 1)), Map.of(A, overflowing), Set.of()));
  }

  private static LeaseState state(long id, long count, long valueBytes, long historyBytes) {
    return new LeaseState(new LeaseIdentity(id, 1), 60, 60_000, 2, LeaseState.Status.ACTIVE,
        10, 3, count, count, count * (HISTORY_BYTES + 4 * valueBytes), historyBytes);
  }

  private static LeaseBindingChange change(long previous, long current,
      long previousValueBytes, long currentValueBytes, long currentCreateRevision) {
    return new LeaseBindingChange(KEY, previous, current, previous == 0 ? 0 : 1,
        previous == 0 ? 0 : 5, previousValueBytes, currentCreateRevision, 6, currentValueBytes);
  }

  private static void assertAccounting(LeaseState state, long count, long revokeCost, long history) {
    assertEquals(count, state.boundKeyCount());
    assertEquals(count, state.boundKeyBytes());
    assertEquals(revokeCost, state.revokeCostBytes());
    assertEquals(history, state.bindingHistoryBytes());
  }
}
