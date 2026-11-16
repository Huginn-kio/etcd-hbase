package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/** Reads durable and selected backfill progress at a single auxiliary-protected M2 barrier. */
public final class EventIndexBackfillReader {
  private final SystemRecordStore records;
  private final ManifestCatalog manifests;

  public EventIndexBackfillReader(SystemRecordStore records, ManifestCatalog manifests) {
    this.records = Objects.requireNonNull(records, "records");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
  }

  public CompletableFuture<Optional<EventIndexBackfillCheckpoint>> read(PinnedReadView pinned,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(pinned, "pinned");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (!pinned.belongsTo(scope) || !pinned.protectsAuxiliary() || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("backfill read requires an auxiliary-protected view"));
    }
    PinnedReadView.ReaderLease lease;
    try {
      lease = pinned.retainReader();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    CompletableFuture<Optional<EventIndexBackfillCheckpoint>> operation;
    try {
      operation = pinned.renew(scope).thenCompose(ignored -> readStable(pinned.view(), scope, budget))
          .thenCompose(stable -> overlay(pinned, stable, scope, budget));
    } catch (RuntimeException | Error failure) {
      lease.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return operation.thenCompose(result -> pinned.renew(scope).thenApply(ignored -> {
      requireActive(pinned, scope);
      return result;
    })).whenComplete((ignored, failure) -> lease.close()).copy();
  }

  private CompletableFuture<Optional<EventIndexBackfillCheckpoint>> readStable(
      ReadView view, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    if (view.finalizedSequence() == 0) return CompletableFuture.completedFuture(Optional.empty());
    return budget.readMeasured(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE,
        value -> value.map(SystemRecordEntry::encodedBytes).orElse(0L),
        () -> records.latestAtOrBefore(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
            EventIndexBackfillCheckpoint.routePrefix(), view.finalizedSequence(), scope))
        .thenApply(entry -> entry.map(value -> validate(value.identity(), value.value(), view.finalizedSequence())));
  }

  private CompletableFuture<Optional<EventIndexBackfillCheckpoint>> overlay(PinnedReadView pinned,
      Optional<EventIndexBackfillCheckpoint> stable, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    ReadView view = pinned.view();
    if (view.selectedRoot().isEmpty() || view.selectedRoot().orElseThrow().auxiliaryReference().isEmpty()) {
      return CompletableFuture.completedFuture(stable);
    }
    var selected = new AtomicReference<EventIndexBackfillCheckpoint>();
    return manifests.forEachAuxiliary(view.selectedRoot().orElseThrow(), scope, budget, record -> {
      requireActive(pinned, scope);
      if (record.targetType() == SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT) {
        EventIndexBackfillCheckpoint checkpoint = validate(record.targetIdentity(), record.payload(), view.barrierSequence());
        if (checkpoint.sequence() != view.barrierSequence() || checkpoint.ownerEpoch() != record.ownerEpoch()
            || !selected.compareAndSet(null, checkpoint)) {
          throw new IllegalStateException("selected event checkpoint differs from its commit barrier");
        }
      }
      return CompletableFuture.completedFuture(null);
    }).thenApply(ignored -> {
      EventIndexBackfillCheckpoint checkpoint = selected.get();
      if (checkpoint == null) return stable;
      if (stable.isPresent()) {
        EventIndexBackfillCheckpoint.requireForwardProgress(stable.orElseThrow(), checkpoint);
      } else if (checkpoint.nextSequence() != checkpoint.firstSequence()) {
        throw new IllegalStateException("selected initial event checkpoint skips history");
      }
      return Optional.of(checkpoint);
    });
  }

  private static EventIndexBackfillCheckpoint validate(ByteKey identity, ByteKey payload, long barrier) {
    long sequence = EventIndexBackfillCheckpoint.sequence(identity);
    EventIndexBackfillCheckpoint checkpoint = EventIndexBackfillCheckpoint.decode(payload);
    if (sequence > barrier || checkpoint.sequence() != sequence) {
      throw new IllegalStateException("event checkpoint differs from its identity or read barrier");
    }
    return checkpoint;
  }

  private static void requireActive(PinnedReadView pinned, RequestScope scope) {
    if (!pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
  }
}
