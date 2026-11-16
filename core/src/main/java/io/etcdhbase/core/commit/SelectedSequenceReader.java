package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Reads the next selected sequence under a durable event-history pin. */
public final class SelectedSequenceReader {
  private final EventHistoryPinManager pins;
  private final CommitControlStore control;
  private final SelectedDecisionReader decisions;
  private final ManifestRepository manifests;
  private final EventRevisionIndexRepository indexes;

  public SelectedSequenceReader(EventHistoryPinManager pins, CommitControlStore control,
      CommitDecisionRepository decisions, ManifestRepository manifests,
      EventRevisionIndexRepository indexes) {
    this.pins = Objects.requireNonNull(pins, "pins");
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = new SelectedDecisionReader(control, decisions);
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.indexes = Objects.requireNonNull(indexes, "indexes");
  }

  /** A KV-changing sequence owns an event cursor; metadata-only sequences have none. */
  public record Selected(CommitDecision decision, Optional<ProtectedEventCursor> events) {
    public Selected {
      Objects.requireNonNull(decision, "decision");
      Objects.requireNonNull(events, "events");
      if (events.isPresent() != decision.containsKeyChanges()) {
        throw new IllegalArgumentException("event cursor differs from selected decision");
      }
    }
  }

  /**
   * The caller supplies the last complete revision and the next sequence from its own verified
   * checkpoint. The scope belongs exclusively to this step. Empty means no sequence was selected
   * at the captured barrier, not that an archived decision or event was missing.
   */
  public CompletableFuture<Optional<Selected>> open(long sequence, long baseRevision,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (sequence <= 0 || baseRevision <= 0 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid selected sequence request"));
    }
    var requirement = new EventHistoryPinManager.Requirement(baseRevision, sequence,
        authorizationDigest, Optional.empty(), Optional.empty());
    var output = new CompletableFuture<Optional<Selected>>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    pins.register(requirement, scope).whenComplete((pin, failure) -> {
      if (failure != null) {
        output.completeExceptionally(failure);
        return;
      }
      var protectedCursor = new ProtectedEventCursor(pin, scope);
      if (output.isDone()) {
        protectedCursor.close();
        return;
      }
      try {
        read(sequence, baseRevision, protectedCursor, scope, budget)
            .whenComplete((selected, readFailure) -> {
              if (readFailure != null) {
                protectedCursor.close();
                output.completeExceptionally(readFailure);
              } else if (!output.complete(selected)) {
                protectedCursor.close();
              }
            });
      } catch (RuntimeException | Error problem) {
        protectedCursor.close();
        output.completeExceptionally(problem);
        if (problem instanceof Error fatal) throw fatal;
      }
    });
    return output;
  }

  private CompletableFuture<Optional<Selected>> read(long sequence, long baseRevision,
      ProtectedEventCursor pin, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return pin.renew().thenCompose(ignored -> readControl(scope, budget)).thenCompose(state -> {
      validateControl(state, baseRevision);
      if (sequence > state.publishedSequence()) return closeEmpty(pin);
      return decisions.read(sequence, scope, budget).thenCompose(found -> {
        CommitDecision decision = found.orElseThrow(
            () -> new IllegalStateException("selected decision disappeared from the control barrier"));
        if (decision.sequence() != sequence || decision.baseRevision() != baseRevision) {
          throw new IllegalStateException("selected sequence is not contiguous with the cursor");
        }
        return budget.read(ManifestCatalog.maximumProofReadPageBytes(),
            () -> manifests.readRoot(decision.manifestRoot(), scope)).thenCompose(foundRoot -> {
          ManifestRoot root = foundRoot.orElseThrow(
              () -> new IllegalStateException("selected manifest root is missing"));
          validateRoot(decision, root);
          CompletableFuture<Void> index = decision.containsKeyChanges()
              ? verifyIndex(decision, root, scope, budget) : CompletableFuture.completedFuture(null);
          return index.thenCompose(ignored -> pin.renew())
              .thenCompose(ignored -> readControl(scope, budget))
              .thenCompose(latest -> {
                validateControl(latest, baseRevision);
                if (decision.sequence() > latest.publishedSequence()) {
                  throw new IllegalStateException("selected sequence moved behind the control barrier");
                }
                if (!decision.containsKeyChanges()) {
                  pin.close();
                  return pin.releaseCompletion().thenApply(released ->
                      Optional.of(new Selected(decision, Optional.empty()))).toCompletableFuture();
                }
                pin.attach(new SealedEventCursor(manifests, decision, root, scope));
                return CompletableFuture.completedFuture(Optional.of(new Selected(decision, Optional.of(pin))));
              });
        });
      });
    });
  }

  private CompletableFuture<Void> verifyIndex(CommitDecision decision, ManifestRoot root,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return readIndex(decision.revision(), scope, budget).thenCompose(index -> {
      if (index.isPresent()) {
        index.orElseThrow().verifySelected(decision, root);
        return CompletableFuture.completedFuture(null);
      }
      return readControl(scope, budget).thenCompose(state -> {
        if (state.finalizedSequence() < decision.sequence()) {
          if (state.publication().filter(decision::matches).isEmpty()) {
            throw new IllegalStateException("unfinalized event no longer matches the selected slot");
          }
          return CompletableFuture.completedFuture(null);
        }
        // The first lookup may have raced the finalizer's index write. Re-read after F.
        return readIndex(decision.revision(), scope, budget).thenAccept(afterFinalization -> {
          EventRevisionIndex stored = afterFinalization.orElseThrow(
              () -> new IllegalStateException("finalized event revision index is missing"));
          stored.verifySelected(decision, root);
        });
      });
    });
  }

  private CompletableFuture<Optional<EventRevisionIndex>> readIndex(long revision,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return budget.read(ManifestCatalog.maximumProofReadPageBytes(), () -> indexes.read(revision, scope));
  }

  private CompletableFuture<CommitState> readControl(RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return budget.read(ManifestCatalog.maximumProofReadPageBytes(), () -> control.read(scope))
        .thenApply(snapshot -> snapshot.orElseThrow(
            () -> new IllegalStateException("authoritative commit control is missing")).state());
  }

  private static void validateControl(CommitState state, long baseRevision) {
    if (!state.supportsEventRevisionIndex() || baseRevision > state.publishedRevision()) {
      throw new IllegalStateException("sequence cursor is outside the control barrier");
    }
    if (baseRevision < state.compactRevision()) {
      throw new IllegalStateException("event history has been compacted");
    }
  }

  private static void validateRoot(CommitDecision decision, ManifestRoot root) {
    if (!decision.transactionId().equals(root.transactionId())
        || decision.sequence() - 1 != root.baseSequence()
        || decision.baseRevision() != root.baseRevision()
        || !decision.manifestRoot().equals(root.systemIdentity())
        || !decision.manifestDigest().equals(root.digest())
        || decision.containsKeyChanges() != root.containsKeyChanges()
        || !decision.containsKeyChanges() && (root.eventCount() != 0 || root.eventDirectory().isPresent())) {
      throw new IllegalStateException("selected decision differs from its sealed manifest");
    }
  }

  private static CompletableFuture<Optional<Selected>> closeEmpty(ProtectedEventCursor pin) {
    pin.close();
    return pin.releaseCompletion().thenApply(ignored -> Optional.<Selected>empty()).toCompletableFuture();
  }
}
