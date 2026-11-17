package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Validates an archived legacy decision and recovers its index before checkpoint finalization. */
public final class EventIndexBackfillMaterializer {
  private final CommitDecisionRepository decisions;
  private final ManifestCatalog manifests;
  private final EventIndexMaterializer indexes;
  private final CommitControlStore control;

  public EventIndexBackfillMaterializer(CommitControlStore control, CommitDecisionRepository decisions,
      ManifestCatalog manifests, EventRevisionIndexRepository indexes) {
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = Objects.requireNonNull(decisions, "decisions");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.indexes = new EventIndexMaterializer(indexes);
  }

  /** Rechecks provenance before selection; does not publish an index. */
  CompletableFuture<Void> preflight(EventIndexBackfillCheckpoint checkpoint,
      OptionalLong previousRevision, RequestScope scope) {
    return process(checkpoint, previousRevision, scope, false);
  }

  /** Replays the index write after selection, before the checkpoint is materialized. */
  CompletableFuture<Void> apply(EventIndexBackfillCheckpoint checkpoint, RequestScope scope) {
    return process(checkpoint, OptionalLong.empty(), scope, true);
  }

  private CompletableFuture<Void> process(EventIndexBackfillCheckpoint checkpoint,
      OptionalLong previousRevision, RequestScope scope, boolean materialize) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(scope, "scope");
    return control.read(scope).thenCompose(found -> {
      CommitState state = found.orElseThrow(
          () -> new IllegalStateException("backfill control row is missing")).state();
      if (!state.supportsEventRevisionIndex() || state.finalizedSequence() < checkpoint.upperSequence()
          || state.finalizedRevision() < checkpoint.upperRevision()) {
        throw new IllegalStateException("backfill window exceeds the finalized control barrier");
      }
      if (materialize) {
        if (state.publishedSequence() < checkpoint.sequence()) {
          throw new IllegalStateException("backfill checkpoint has not been selected");
        }
      } else if (state.ownerEpoch() != checkpoint.ownerEpoch()
          || state.publishedSequence() != checkpoint.sequence() - 1
          || state.finalizedSequence() != state.publishedSequence()
          || state.compactRevision() != checkpoint.compactRevision()) {
        throw new IllegalStateException("backfill publication fence or compact boundary changed");
      }
      // The first checkpoint freezes the complete legacy interval captured at the upgrade
      // boundary. Later finalized commits are already governed by the event-index format and do
      // not extend this legacy interval.
      if (!materialize && previousRevision.isEmpty()
          && checkpoint.nextSequence() != checkpoint.firstSequence()) {
        throw new IllegalStateException("initial backfill checkpoint differs from its captured history boundary");
      }
      if (!materialize && previousRevision.isEmpty()) {
        return EventIndexBackfillCapture.verifyBoundaries(checkpoint, sequence ->
            decisions.read(sequence, scope).thenApply(decision -> decision.orElseThrow(
                () -> new IllegalStateException("captured legacy decision is missing"))));
      }
      return processLegacy(checkpoint, previousRevision, scope, materialize);
    });
  }

  private CompletableFuture<Void> processLegacy(EventIndexBackfillCheckpoint checkpoint,
      OptionalLong previousRevision, RequestScope scope, boolean materialize) {
    if (checkpoint.nextSequence() == checkpoint.firstSequence()) {
      return CompletableFuture.completedFuture(null);
    }
    long legacySequence = checkpoint.nextSequence() - 1;
    return decisions.read(legacySequence, scope).thenCompose(found -> {
      CommitDecision decision = found.orElseThrow(
          () -> new IllegalStateException("backfill legacy decision is missing"));
      if (decision.sequence() != legacySequence || decision.revision() != checkpoint.currentRevision()
          || previousRevision.isPresent() && decision.baseRevision() != previousRevision.getAsLong()) {
        throw new IllegalStateException("backfill checkpoint differs from its legacy decision");
      }
      return manifests.readRoot(decision.manifestRoot(), scope).thenCompose(stored -> {
        ManifestRoot root = stored.orElseThrow(
            () -> new IllegalStateException("backfill legacy manifest is missing"));
        if (!root.transactionId().equals(decision.transactionId())
            || root.baseSequence() != legacySequence - 1 || root.baseRevision() != decision.baseRevision()
            || root.containsKeyChanges() != decision.containsKeyChanges()
            || !root.systemIdentity().equals(decision.manifestRoot())
            || !root.digest().equals(decision.manifestDigest())) {
          throw new IllegalStateException("backfill manifest differs from its archived decision");
        }
        if (decision.containsKeyChanges()) EventRevisionIndex.fromSelected(decision, root);
        return materialize ? indexes.apply(decision, root, scope) : CompletableFuture.completedFuture(null);
      });
    });
  }
}
