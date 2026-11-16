package io.etcdhbase.core.lease;

import io.etcdhbase.core.error.BackendFailure;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Computes immutable Lease accounting versions for the net KV changes at one commit barrier. */
public final class LeaseBindingStatePlanner {
  private final LeaseCostAdmission costs;

  public LeaseBindingStatePlanner(LeaseCostAdmission costs) {
    this.costs = Objects.requireNonNull(costs, "costs");
  }

  /**
   * Returns changed Lease versions in ID order, without mutating the resolved base states.
   * The caller owns state/binding reads and validates attachment, expiry and revocation identity.
   * This planner owns net accounting, capacity admission and ACTIVE/REVOKED version generation.
   */
  public Map<Long, LeaseState> plan(long sequence, List<LeaseBindingChange> changes,
      Map<Long, LeaseState> resolved, Set<Long> revokingLeaseIds) throws BackendFailure {
    Objects.requireNonNull(changes, "changes");
    Objects.requireNonNull(resolved, "resolved");
    Objects.requireNonNull(revokingLeaseIds, "revokingLeaseIds");
    if (sequence <= 0) throw new IllegalArgumentException("Lease planning needs a positive sequence");
    Map<Long, Delta> deltas = collectDeltas(changes, revokingLeaseIds);
    Map<Long, LeaseState> updatedStates = new TreeMap<>();
    for (Map.Entry<Long, Delta> entry : deltas.entrySet()) {
      updatedStates.put(entry.getKey(), updateLeaseState(entry.getKey(), entry.getValue(),
          resolved, revokingLeaseIds, sequence));
    }
    return Collections.unmodifiableMap(updatedStates);
  }

  private Map<Long, Delta> collectDeltas(List<LeaseBindingChange> changes, Set<Long> revokingLeaseIds) {
    Map<Long, Delta> deltas = new HashMap<>();
    for (LeaseBindingChange change : changes) {
      accumulateBindingDelta(change, deltas);
      accumulateHistoryDelta(change, deltas);
    }
    for (long leaseId : revokingLeaseIds) deltas.computeIfAbsent(leaseId, ignored -> new Delta());
    return deltas;
  }

  private void accumulateBindingDelta(LeaseBindingChange change, Map<Long, Delta> deltas) {
    long previous = change.previousLeaseId();
    long current = change.currentLeaseId();
    if (previous != 0) {
      Delta delta = deltas.computeIfAbsent(previous, ignored -> new Delta());
      if (previous != current) delta.bindingCount = Math.subtractExact(delta.bindingCount, 1);
      delta.keyBytes = Math.subtractExact(delta.keyBytes, change.key().length());
      delta.revokeCostBytes = Math.subtractExact(delta.revokeCostBytes,
          costs.revokeCostBytes(change.key(), change.previousValueBytes()));
    }
    if (current != 0) {
      Delta delta = deltas.computeIfAbsent(current, ignored -> new Delta());
      if (previous != current) delta.bindingCount = Math.addExact(delta.bindingCount, 1);
      delta.keyBytes = Math.addExact(delta.keyBytes, change.key().length());
      delta.revokeCostBytes = Math.addExact(delta.revokeCostBytes,
          costs.revokeCostBytes(change.key(), change.currentValueBytes()));
    }
  }

  private void accumulateHistoryDelta(LeaseBindingChange change, Map<Long, Delta> deltas) {
    long previous = change.previousLeaseId();
    long current = change.currentLeaseId();
    int newVersions = change.newBindingVersionCount();
    if (newVersions > 0) {
      long historyCost = Math.multiplyExact(costs.bindingHistoryRecordBytes(change.key()), newVersions);
      if (previous != 0) {
        Delta delta = deltas.computeIfAbsent(previous, ignored -> new Delta());
        if (current == previous) {
          delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes, historyCost);
        } else {
          delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes,
              costs.bindingHistoryRecordBytes(change.key()));
        }
      }
      if (current != 0 && current != previous) {
        Delta delta = deltas.computeIfAbsent(current, ignored -> new Delta());
        delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes,
            costs.bindingHistoryRecordBytes(change.key()));
      }
    }
  }

  private LeaseState updateLeaseState(long leaseId, Delta delta, Map<Long, LeaseState> resolved,
      Set<Long> revokingLeaseIds, long sequence) throws BackendFailure {
    LeaseState old = Objects.requireNonNull(resolved.get(leaseId), "Lease state is missing");
    if (old.identity().leaseId() != leaseId || sequence <= old.sequence()) {
      throw new IllegalArgumentException("resolved Lease differs from the planning base");
    }
    if (old.status() != LeaseState.Status.ACTIVE) {
      throw new IllegalStateException("Lease planning requires an active base state");
    }
    long count = Math.addExact(old.boundKeyCount(), delta.bindingCount);
    long keyBytes = Math.addExact(old.boundKeyBytes(), delta.keyBytes);
    long revokeCostBytes = Math.addExact(old.revokeCostBytes(), delta.revokeCostBytes);
    long bindingHistoryBytes = Math.addExact(old.bindingHistoryBytes(), delta.bindingHistoryBytes);
    costs.requireWithin(count, keyBytes, revokeCostBytes, bindingHistoryBytes);
    if (revokingLeaseIds.contains(leaseId)) {
      if (count != 0 || keyBytes != 0 || revokeCostBytes != 0) {
        throw new IllegalStateException("Lease revocation plan leaves live bindings or reserved cost");
      }
      return new LeaseState(old.identity(), old.grantedTtlSeconds(),
          old.deadlineEpochMillis(), old.renewalGeneration(), LeaseState.Status.REVOKED,
          sequence, old.expiryCandidateSequence(), 0, 0, 0, bindingHistoryBytes);
    } else {
      return old.withBindingAccounting(sequence, count, keyBytes, revokeCostBytes, bindingHistoryBytes);
    }
  }

  private static final class Delta {
    private long bindingCount;
    private long keyBytes;
    private long revokeCostBytes;
    private long bindingHistoryBytes;
  }
}
