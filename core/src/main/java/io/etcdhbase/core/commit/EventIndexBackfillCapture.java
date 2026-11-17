package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Verifies the retained legacy interval while its initial event pin remains registered. */
public final class EventIndexBackfillCapture {
  private final EventHistoryPinManager pins;
  private final CommitControlStore control;
  private final SelectedDecisionReader decisions;

  public EventIndexBackfillCapture(EventHistoryPinManager pins, CommitControlStore control,
      CommitDecisionRepository decisions) {
    this.pins = Objects.requireNonNull(pins, "pins");
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = new SelectedDecisionReader(control, decisions);
  }

  /** The returned window must remain open through the initial M2 publication and its recovery. */
  public CompletableFuture<EventBackfillWindow> open(EventIndexBackfillCheckpoint checkpoint,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    var output = new CompletableFuture<EventBackfillWindow>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    EventBackfillWindow.openCapture(pins, control, checkpoint, authorizationDigest, scope, budget)
        .whenComplete((window, failure) -> {
          if (failure != null) {
            output.completeExceptionally(failure);
            return;
          }
          if (output.isDone()) {
            window.close();
            return;
          }
          try {
            verifyBoundaries(checkpoint, sequence -> read(sequence, scope, budget))
                .thenCompose(ignored -> window.checkHistory(checkpoint, checkpoint.ownerEpoch(), budget))
                .whenComplete((ignored, validationFailure) -> {
                  if (validationFailure != null) {
                    window.close();
                    output.completeExceptionally(validationFailure);
                  } else if (!output.complete(window)) {
                    window.close();
                  }
                });
          } catch (RuntimeException | Error problem) {
            window.close();
            output.completeExceptionally(problem);
            if (problem instanceof Error fatal) throw fatal;
          }
        });
    return output;
  }

  /** Shared by capture and preflight so the published window cannot omit retained history. */
  static CompletableFuture<Void> verifyBoundaries(EventIndexBackfillCheckpoint checkpoint,
      Function<Long, CompletableFuture<CommitDecision>> read) {
    if (checkpoint.upperSequence() == 0) {
      if (checkpoint.firstSequence() != 1 || checkpoint.firstBaseRevision() != checkpoint.upperRevision()) {
        return CompletableFuture.failedFuture(new IllegalStateException("invalid empty legacy boundary"));
      }
      return CompletableFuture.completedFuture(null);
    }
    if (checkpoint.firstSequence() > checkpoint.upperSequence()) {
      return CompletableFuture.failedFuture(new IllegalStateException("retained legacy history is missing"));
    }
    return read.apply(checkpoint.firstSequence()).thenCompose(first -> {
      if (first.sequence() != checkpoint.firstSequence()
          || first.baseRevision() != checkpoint.firstBaseRevision()) {
        throw new IllegalStateException("first legacy decision differs from the captured revision");
      }
      if (checkpoint.firstSequence() == 1) return CompletableFuture.completedFuture(null);
      return read.apply(checkpoint.firstSequence() - 1).thenAccept(previous -> {
        if (previous.sequence() != checkpoint.firstSequence() - 1
            || previous.revision() != first.baseRevision()
            || previous.revision() >= checkpoint.compactRevision()) {
          throw new IllegalStateException("captured interval skips retained legacy history");
        }
      });
    }).thenCompose(ignored -> read.apply(checkpoint.upperSequence()))
        .thenAccept(last -> {
          if (last.sequence() != checkpoint.upperSequence()
              || last.revision() != checkpoint.upperRevision()) {
            throw new IllegalStateException("last legacy decision differs from the captured revision");
          }
        });
  }

  private CompletableFuture<CommitDecision> read(long sequence, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    return decisions.read(sequence, scope, budget).thenApply(found -> found.orElseThrow(
        () -> new IllegalStateException("captured legacy decision is missing")));
  }
}
