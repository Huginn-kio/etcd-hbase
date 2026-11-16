package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Locates the retained legacy interval at a finalized, owner-fenced control barrier. */
public final class EventIndexBackfillPlanner {
  private final CommitControlStore control;
  private final SelectedDecisionReader decisions;

  public EventIndexBackfillPlanner(CommitControlStore control, CommitDecisionRepository decisions) {
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = new SelectedDecisionReader(control, decisions);
  }

  /** The caller must still capture protection and publish the checkpoint on the owner lane. */
  public CompletableFuture<EventIndexBackfillCheckpoint> plan(CommitOwnerService.OwnerToken owner,
      ByteKey namespace, ByteKey registry, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(namespace, "namespace");
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (namespace.length() != 16 || registry.length() != 16 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid backfill planner scope or registry"));
    }
    return budget.read(ManifestCatalog.maximumProofReadPageBytes(), () -> control.read(scope))
        .thenCompose(found -> {
          CommitState state = found.orElseThrow(() -> new IllegalStateException("backfill control row is missing")).state();
          if (state.ownerEpoch() != owner.epoch()
              || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
              || !state.eventRegistryNamespace().equals(Optional.of(namespace))
              || state.publication().isPresent() || state.publishedSequence() != state.finalizedSequence()
              || state.publishedSequence() == Long.MAX_VALUE) {
            throw new IllegalStateException("backfill capture requires a finalized owner barrier");
          }
          long upper = state.finalizedSequence();
          long commit = upper + 1;
          if (upper == 0) {
            return CompletableFuture.completedFuture(new EventIndexBackfillCheckpoint(owner.epoch(), commit,
                1, 0, 1, state.finalizedRevision(), state.finalizedRevision(), state.finalizedRevision(),
                state.compactRevision(), registry));
          }
          long minimumRevision = Math.max(1, state.compactRevision());
          return findFirst(1, upper, minimumRevision, scope, budget).thenCompose(first ->
              read(first, scope, budget).thenApply(decision -> {
                if (decision.revision() < minimumRevision || decision.revision() > state.finalizedRevision()) {
                  throw new IllegalStateException("retained decision lies outside the captured barrier");
                }
                return new EventIndexBackfillCheckpoint(owner.epoch(), commit, first, upper, first,
                    decision.baseRevision(), decision.baseRevision(), state.finalizedRevision(),
                    state.compactRevision(), registry);
              }));
        });
  }

  private CompletableFuture<Long> findFirst(long lower, long upper, long minimumRevision,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    if (lower == upper) return CompletableFuture.completedFuture(lower);
    long middle = lower + (upper - lower) / 2;
    return read(middle, scope, budget).thenCompose(decision -> decision.revision() >= minimumRevision
        ? findFirst(lower, middle, minimumRevision, scope, budget)
        : findFirst(middle + 1, upper, minimumRevision, scope, budget));
  }

  private CompletableFuture<CommitDecision> read(long sequence, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    return decisions.read(sequence, scope, budget).thenApply(found -> found.orElseThrow(
        () -> new IllegalStateException("captured legacy decision is missing")));
  }
}
