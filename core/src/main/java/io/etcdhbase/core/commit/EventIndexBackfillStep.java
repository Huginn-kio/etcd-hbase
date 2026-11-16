package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bounded proof of one legacy sequence before preparing its owner-fenced backfill commit. */
public final class EventIndexBackfillStep implements AutoCloseable {
  /** A proposal only: the caller must atomically publish the index and checkpoint through M2. */
  public record Prepared(EventIndexBackfillCheckpoint checkpoint, Optional<EventRevisionIndex> index) {
    public Prepared {
      Objects.requireNonNull(checkpoint, "checkpoint");
      Objects.requireNonNull(index, "index");
    }
  }

  private final EventIndexBackfillCheckpoint before;
  private final SelectedSequenceReader.Selected selected;
  private final CommitControlStore control;
  private final RequestScope scope;
  private final long ownerEpoch;
  private final long commitSequence;
  private boolean reading;
  private boolean finished;
  private boolean closed;

  private EventIndexBackfillStep(EventIndexBackfillCheckpoint before,
      SelectedSequenceReader.Selected selected, CommitControlStore control,
      RequestScope scope, long ownerEpoch, long commitSequence) {
    this.before = before;
    this.selected = selected;
    this.control = control;
    this.scope = scope;
    this.ownerEpoch = ownerEpoch;
    this.commitSequence = commitSequence;
  }

  /** The caller supplies a checkpoint read from a selected M2 auxiliary version. */
  public static CompletableFuture<EventIndexBackfillStep> open(SelectedSequenceReader source,
      CommitControlStore control, EventIndexBackfillCheckpoint checkpoint, ByteKey authorizationDigest,
      long ownerEpoch, long commitSequence, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(control, "control");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(scope, "scope");
    if (ownerEpoch < checkpoint.ownerEpoch() || commitSequence <= checkpoint.sequence()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event backfill publication fence"));
    }
    var output = new CompletableFuture<EventIndexBackfillStep>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    source.openLegacy(checkpoint, authorizationDigest, scope, budget).whenComplete((found, failure) -> {
      if (failure != null) {
        scope.close();
        output.completeExceptionally(failure);
        return;
      }
      if (found.isEmpty()) {
        scope.close();
        output.completeExceptionally(new IllegalStateException("captured legacy sequence is missing"));
        return;
      }
      var step = new EventIndexBackfillStep(checkpoint, found.orElseThrow(), control,
          scope, ownerEpoch, commitSequence);
      if (!output.complete(step)) step.close();
    });
    return output;
  }

  /** Reads at most one event; the proposal appears only after all sealed events are verified. */
  public synchronized CompletableFuture<Optional<Prepared>> next(ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(budget, "budget");
    if (closed || finished || reading || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalStateException("event backfill step is not readable"));
    }
    reading = true;
    CompletableFuture<Optional<Prepared>> operation;
    if (selected.events().isEmpty()) {
      try {
        operation = CompletableFuture.completedFuture(Optional.of(proposal()));
      } catch (RuntimeException failure) {
        reading = false;
        close();
        return CompletableFuture.failedFuture(failure);
      }
    } else {
      ProtectedEventCursor events = selected.events().orElseThrow();
      operation = events.next(budget).thenCompose(event -> {
        if (event.isPresent()) return CompletableFuture.completedFuture(Optional.empty());
        return events.renew().thenCompose(ignored -> budget.read(ManifestCatalog.maximumProofReadPageBytes(),
            () -> control.read(scope))).thenCompose(snapshot -> {
          CommitState current = snapshot.orElseThrow(
              () -> new IllegalStateException("authoritative commit control is missing")).state();
          if (current.compactRevision() != before.compactRevision()
              || current.finalizedSequence() < before.sequence() || current.ownerEpoch() != ownerEpoch
              || current.publishedSequence() == Long.MAX_VALUE
              || current.publishedSequence() + 1 != commitSequence) {
            throw new IllegalStateException("event backfill boundary or owner changed");
          }
          events.close();
          return events.releaseCompletion().thenApply(ignored -> Optional.of(proposal())).toCompletableFuture();
        });
      });
    }
    var output = new CompletableFuture<Optional<Prepared>>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) close(); });
    operation.whenComplete((prepared, failure) -> {
      boolean stopped;
      synchronized (EventIndexBackfillStep.this) {
        reading = false;
        stopped = closed;
        if (!stopped && failure == null && prepared.isPresent()) finished = true;
      }
      if (stopped && failure == null) {
        output.completeExceptionally(new IllegalStateException("event backfill step is closed"));
      } else if (failure != null) {
        close();
        output.completeExceptionally(failure);
      } else if (!output.complete(prepared)) {
        close();
      }
    });
    return output;
  }

  private Prepared proposal() {
    var next = before.advance(ownerEpoch, commitSequence, selected.decision());
    return new Prepared(next, selected.index());
  }

  @Override public synchronized void close() {
    if (closed) return;
    closed = true;
    selected.events().ifPresent(ProtectedEventCursor::close);
    scope.close();
  }
}
