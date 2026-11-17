package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bounded proof of one legacy sequence before preparing its owner-fenced backfill commit. */
public final class EventIndexBackfillStep implements AutoCloseable {
  /**
   * A proposal only: {@code checkpoint.sequence()} is provisional until the owner lane rebinds it
   * to the latest finalized M2 base before publishing through M2.
   */
  public record Prepared(EventIndexBackfillCheckpoint checkpoint, Optional<EventRevisionIndex> index) {
    public Prepared {
      Objects.requireNonNull(checkpoint, "checkpoint");
      Objects.requireNonNull(index, "index");
    }
  }

  private final EventIndexBackfillCheckpoint before;
  private final SelectedSequenceReader.Selected selected;
  private final CommitControlStore control;
  private final EventBackfillWindow window;
  private final RequestScope scope;
  private final long ownerEpoch;
  private final long provisionalCommitSequence;
  private boolean reading;
  private boolean finished;
  private boolean closed;

  private EventIndexBackfillStep(EventIndexBackfillCheckpoint before,
      SelectedSequenceReader.Selected selected, CommitControlStore control, EventBackfillWindow window,
      RequestScope scope, long ownerEpoch, long provisionalCommitSequence) {
    this.before = before;
    this.selected = selected;
    this.control = control;
    this.window = window;
    this.scope = scope;
    this.ownerEpoch = ownerEpoch;
    this.provisionalCommitSequence = provisionalCommitSequence;
  }

  /** The caller supplies a checkpoint read from a selected M2 auxiliary version. */
  public static CompletableFuture<EventIndexBackfillStep> open(SelectedSequenceReader source,
      CommitControlStore control, EventBackfillWindow window, EventIndexBackfillCheckpoint checkpoint,
      ByteKey authorizationDigest, long ownerEpoch, long provisionalCommitSequence, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget, ManifestCatalog.ProofReadBudget windowBudget) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(control, "control");
    Objects.requireNonNull(window, "window");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(scope, "scope");
    if (ownerEpoch < checkpoint.ownerEpoch() || provisionalCommitSequence <= checkpoint.sequence()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event backfill sequence proposal"));
    }
    var output = new CompletableFuture<EventIndexBackfillStep>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    window.checkHistory(checkpoint, ownerEpoch, windowBudget)
        .thenCompose(ignored -> source.openLegacy(checkpoint, authorizationDigest, scope, budget))
        .whenComplete((found, failure) -> {
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
      var step = new EventIndexBackfillStep(checkpoint, found.orElseThrow(), control, window,
          scope, ownerEpoch, provisionalCommitSequence);
      if (!output.complete(step)) step.close();
    });
    return output;
  }

  /** Reads at most one event; the proposal appears only after all sealed events are verified. */
  public synchronized CompletableFuture<Optional<Prepared>> next(ManifestCatalog.ProofReadBudget budget,
      ManifestCatalog.ProofReadBudget windowBudget) {
    Objects.requireNonNull(budget, "budget");
    Objects.requireNonNull(windowBudget, "windowBudget");
    if (closed || finished || reading || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalStateException("event backfill step is not readable"));
    }
    reading = true;
    CompletableFuture<Optional<Prepared>> operation;
    if (selected.events().isEmpty()) {
      try {
        operation = window.checkHistory(before, ownerEpoch, windowBudget)
            .thenApply(ignored -> Optional.of(proposal()));
      } catch (RuntimeException failure) {
        reading = false;
        close();
        return CompletableFuture.failedFuture(failure);
      }
    } else {
      ProtectedEventCursor events = selected.events().orElseThrow();
      operation = events.next(budget).thenCompose(event -> {
        if (event.isPresent()) return CompletableFuture.completedFuture(Optional.empty());
        return events.renew().thenCompose(ignored -> budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
            () -> control.readWithSize(scope))).thenCompose(snapshot -> {
          CommitState current = snapshot.orElseThrow(
              () -> new IllegalStateException("authoritative commit control is missing")).state();
          if (current.compactRevision() != before.compactRevision()
              || current.finalizedSequence() < before.sequence() || current.ownerEpoch() != ownerEpoch
              || current.finalizedSequence() < before.upperSequence()
              || current.finalizedRevision() < before.upperRevision()) {
            throw new IllegalStateException("event backfill boundary or owner changed");
          }
          return window.checkHistory(before, ownerEpoch, windowBudget)
              .thenCompose(ignored -> {
                events.close();
                return events.releaseCompletion().thenApply(released -> Optional.of(proposal()))
                    .toCompletableFuture();
              });
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
    var next = before.advance(ownerEpoch, provisionalCommitSequence, selected.decision());
    return new Prepared(next, selected.index());
  }

  @Override public synchronized void close() {
    if (closed) return;
    closed = true;
    selected.events().ifPresent(ProtectedEventCursor::close);
    scope.close();
  }
}
