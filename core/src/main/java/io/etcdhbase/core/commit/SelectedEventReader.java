package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Opens a selected event root only after durable registration and control-barrier verification. */
public final class SelectedEventReader {
  private final EventHistoryPinManager pins;
  private final CommitControlStore control;
  private final SelectedDecisionReader decisions;
  private final ManifestRepository manifests;

  public SelectedEventReader(EventHistoryPinManager pins, CommitControlStore control,
      CommitDecisionRepository decisions, ManifestRepository manifests) {
    this.pins = Objects.requireNonNull(pins, "pins");
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = new SelectedDecisionReader(control, decisions);
    this.manifests = Objects.requireNonNull(manifests, "manifests");
  }

  /**
   * The index is a locator, not proof of selection or coverage. The producer scope and budget
   * belong exclusively to this cursor. Callers must separately establish history coverage.
   */
  public CompletableFuture<ProtectedEventCursor> open(EventRevisionIndex index,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (!budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event budget belongs to another scope"));
    }
    CommitDecision expected = index.decision();
    var requirement = new EventHistoryPinManager.Requirement(expected.revision(), expected.sequence(),
        authorizationDigest, Optional.of(expected.transactionId()), Optional.of(index.events()));
    var result = new CompletableFuture<ProtectedEventCursor>();
    result.whenComplete((ignored, failure) -> {
      if (result.isCancelled()) scope.cancel();
    });
    pins.register(requirement, scope).whenComplete((pin, registrationFailure) -> {
      if (registrationFailure != null) {
        result.completeExceptionally(registrationFailure);
        return;
      }
      var protectedCursor = new ProtectedEventCursor(pin, scope);
      if (result.isDone()) {
        protectedCursor.close();
        return;
      }
      try {
        verify(index, expected, protectedCursor, scope, budget).whenComplete((cursor, failure) -> {
          if (failure != null) {
            protectedCursor.close();
            result.completeExceptionally(failure);
          } else if (!result.complete(cursor)) {
            protectedCursor.close();
          }
        });
      } catch (RuntimeException | Error failure) {
        protectedCursor.close();
        result.completeExceptionally(failure);
        if (failure instanceof Error fatal) throw fatal;
      }
    });
    return result;
  }

  private CompletableFuture<ProtectedEventCursor> verify(EventRevisionIndex index,
      CommitDecision expected, ProtectedEventCursor protectedCursor, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    return protectedCursor.renew()
        .thenCompose(ignored -> budget.read(ManifestCatalog.maximumProofReadPageBytes(),
            () -> control.read(scope)))
        .thenCompose(snapshot -> {
          CommitState barrier = snapshot.orElseThrow(
              () -> new IllegalStateException("authoritative commit control is missing")).state();
          if (!barrier.supportsEventRevisionIndex()
              || expected.sequence() > barrier.publishedSequence()
              || expected.revision() > barrier.publishedRevision()) {
            throw new IllegalStateException("event locator is outside the captured control barrier");
          }
          if (expected.revision() < barrier.compactRevision()) {
            throw new IllegalStateException("event history has been compacted");
          }
          return decisions.read(expected.sequence(), scope, budget);
        })
        .thenCompose(selected -> {
          CommitDecision decision = selected.orElseThrow(
              () -> new IllegalStateException("event locator is not selected"));
          if (!expected.equals(decision)) {
            throw new IllegalStateException("event locator differs from selected decision");
          }
          return budget.read(ManifestCatalog.maximumProofReadPageBytes(),
              () -> manifests.readRoot(decision.manifestRoot(), scope))
              .thenApply(root -> {
                ManifestRoot manifest = root.orElseThrow(
                    () -> new IllegalStateException("selected manifest root is missing"));
                index.verifySelected(decision, manifest);
                return manifest;
              });
        })
        .thenCompose(root -> protectedCursor.renew().thenApply(ignored -> {
          protectedCursor.attach(new SealedEventCursor(manifests, expected, root, scope));
          return protectedCursor;
        }));
  }
}
