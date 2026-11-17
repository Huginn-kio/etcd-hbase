package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Resolves a sequence against an authoritative control barrier and the immutable archive. */
public final class SelectedDecisionReader {
  private final CommitControlStore control;
  private final CommitDecisionRepository decisions;

  public SelectedDecisionReader(CommitControlStore control, CommitDecisionRepository decisions) {
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = Objects.requireNonNull(decisions, "decisions");
  }

  /** Empty means the requested sequence is beyond the captured barrier, never a missing archive. */
  public CompletableFuture<Optional<CommitDecision>> read(long sequence,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (sequence <= 0 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid decision sequence or budget"));
    }
    return budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(), () -> control.readWithSize(scope))
        .thenCompose(found -> {
          CommitState state = found.orElseThrow(
              () -> new IllegalStateException("authoritative commit control is missing")).state();
          if (sequence > state.publishedSequence()) return CompletableFuture.completedFuture(Optional.empty());
          return budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
              () -> decisions.readWithSize(sequence, scope))
              .thenApply(archived -> Optional.of(resolve(state, sequence, archived)));
        });
  }

  private static CommitDecision resolve(CommitState barrier, long sequence,
      Optional<CommitDecision> archived) {
    if (sequence > barrier.finalizedSequence()) {
      PublicationSlot slot = barrier.publication().orElseThrow(
          () -> new IllegalStateException("selected control slot is missing"));
      CommitDecision selected = CommitDecision.from(slot);
      if (selected.sequence() != sequence || archived.filter(value -> !value.equals(selected)).isPresent()) {
        throw new IllegalStateException("archived decision differs from selected control slot");
      }
      return selected;
    }
    CommitDecision selected = archived.orElseThrow(
        () -> new IllegalStateException("finalized commit decision is missing"));
    if (selected.sequence() != sequence || selected.revision() > barrier.finalizedRevision()) {
      throw new IllegalStateException("archived decision is outside the captured finalized barrier");
    }
    return selected;
  }
}
