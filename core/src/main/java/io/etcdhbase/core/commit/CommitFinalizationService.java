package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Archives and idempotently materializes a selected transaction before advancing finalized state. */
public final class CommitFinalizationService implements CommitFinalizer {
  private static final int MAX_ARCHIVE_ATTEMPTS = 32;
  private static final int MAX_FINALIZE_ATTEMPTS = 32;

  private final CommitControlStore control;
  private final CommitOutcomeResolver outcomes;
  private final CommitDecisionRepository decisions;
  private final ManifestCatalog manifests;
  private final MaterializationStore materialization;

  public CommitFinalizationService(
      CommitControlStore control,
      CommitOutcomeResolver outcomes,
      CommitDecisionRepository decisions,
      ManifestCatalog manifests,
      MaterializationStore materialization) {
    this.control = Objects.requireNonNull(control, "control");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.decisions = Objects.requireNonNull(decisions, "decisions");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.materialization = Objects.requireNonNull(materialization, "materialization");
  }

  @Override
  public CompletableFuture<CommitOutcomeResolver.Resolution> finish(
      InvocationBinding invocation,
      CommitOutcomeResolver.Resolution publication,
      RequestScope serviceScope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(publication, "publication");
    Objects.requireNonNull(serviceScope, "serviceScope");
    if (publication.state() != CommitOutcomeResolver.State.SELECTED_PENDING) {
      return CompletableFuture.completedFuture(publication);
    }
    return outcomes.resolve(invocation, serviceScope).thenCompose(current -> {
      if (current.state() != CommitOutcomeResolver.State.SELECTED_PENDING) {
        return CompletableFuture.completedFuture(current);
      }
      CommitDecision decision = current.decision().orElseThrow();
      if (!decision.equals(publication.decision().orElseThrow())) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("selected decision changed during finalization"));
      }
      validateBinding(invocation, decision);
      return archiveDecision(decision, serviceScope, 0)
          .thenCompose(ignored -> readAndValidateManifest(invocation, decision, serviceScope))
          .thenCompose(root -> applyCandidates(invocation, decision, root, serviceScope))
          .thenCompose(ignored -> finalizeControl(invocation, decision, serviceScope, 0));
    });
  }

  private CompletableFuture<Void> archiveDecision(
      CommitDecision decision, RequestScope scope, int attempt) {
    if (attempt >= MAX_ARCHIVE_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    return decisions.createIfAbsent(decision, scope)
        .handle((result, failure) -> new ArchiveAttempt(result, failure))
        .thenCompose(result -> {
          if (result.failure() != null && !isUnknown(result.failure())) {
            return CompletableFuture.failedFuture(unwrap(result.failure()));
          }
          if (result.failure() == null && (result.result() == null
              || result.result() == SystemRecordStore.CreateResult.CONFLICT)) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("commit sequence has a conflicting durable decision"));
          }
          return decisions.read(decision.sequence(), scope).thenCompose(stored -> {
            if (stored.isPresent()) {
              if (!stored.orElseThrow().equals(decision)) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("archived decision differs from the selected control slot"));
              }
              return CompletableFuture.completedFuture(null);
            }
            if (result.failure() != null) {
              return archiveDecision(decision, scope, attempt + 1);
            }
            return CompletableFuture.failedFuture(
                new IllegalStateException("successful decision creation is not strongly readable"));
          });
        });
  }

  private CompletableFuture<ManifestRoot> readAndValidateManifest(
      InvocationBinding invocation, CommitDecision decision, RequestScope scope) {
    return manifests.readRoot(decision.manifestRoot(), scope).thenApply(stored -> {
      ManifestRoot root = stored.orElseThrow(
          () -> new IllegalStateException("selected sealed manifest root is missing"));
      if (!root.transactionId().equals(decision.transactionId())
          || root.ownerEpoch() != invocation.preparedOwnerEpoch()
          || root.baseSequence() != invocation.baseSequence()
          || root.baseRevision() != decision.baseRevision()
          || (root.candidateCount() > 0) != decision.containsKeyChanges()
          || !root.systemIdentity().equals(decision.manifestRoot())
          || !root.digest().equals(decision.manifestDigest())) {
        throw new IllegalStateException("selected decision differs from its sealed manifest root");
      }
      return root;
    });
  }

  private CompletableFuture<Void> applyCandidates(
      InvocationBinding invocation, CommitDecision decision, ManifestRoot root, RequestScope scope) {
    return manifests.forEachCandidate(root, scope, candidate ->
        materialization.applySelected(decision, candidate, scope).thenCompose(result -> {
          if (result != MaterializationStore.ApplyResult.CURRENT_SUPERSEDED) {
            return CompletableFuture.completedFuture(null);
          }
          return outcomes.resolve(invocation, scope).thenCompose(current ->
              current.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
                  ? CompletableFuture.completedFuture(null)
                  : CompletableFuture.failedFuture(new IllegalStateException(
                      "current row was superseded before this commit was finalized")));
        })).thenApply(ignored -> null);
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> finalizeControl(
      InvocationBinding invocation, CommitDecision decision, RequestScope scope, int attempt) {
    if (attempt >= MAX_FINALIZE_ATTEMPTS) return outcomes.resolve(invocation, scope);
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("authoritative M2 control state is missing"));
      }
      ControlSnapshot observed = snapshot.orElseThrow();
      CommitState state = observed.state();
      if (state.finalizedSequence() >= decision.sequence()) return outcomes.resolve(invocation, scope);
      if (state.publication().isEmpty()
          || !CommitDecision.from(state.publication().orElseThrow()).equals(decision)) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("pending selected decision is missing from the control slot"));
      }
      CommitState finalized = state.finalizePublication(decision.transactionId());
      return control.compareAndSet(observed, finalized, scope)
          .handle((updated, failure) -> new FinalizeAttempt(updated, failure))
          .thenCompose(result -> {
            if (result.failure() != null) {
              Throwable failure = unwrap(result.failure());
              if (!isUnknown(failure)) return CompletableFuture.failedFuture(failure);
              return resolveAndRetry(invocation, decision, scope, attempt);
            }
            if (Boolean.TRUE.equals(result.updated())) return outcomes.resolve(invocation, scope);
            if (result.updated() == null) {
              return resolveAndRetry(invocation, decision, scope, attempt);
            }
            return finalizeControl(invocation, decision, scope, attempt + 1);
          });
    });
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> resolveAndRetry(
      InvocationBinding invocation, CommitDecision decision, RequestScope scope, int attempt) {
    return outcomes.resolve(invocation, scope).thenCompose(current -> {
      if (current.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED) {
        return CompletableFuture.completedFuture(current);
      }
      if (current.state() == CommitOutcomeResolver.State.SELECTED_PENDING
          && current.decision().filter(decision::equals).isPresent()) {
        return finalizeControl(invocation, decision, scope, attempt + 1);
      }
      return CompletableFuture.completedFuture(current);
    });
  }

  private static void validateBinding(InvocationBinding invocation, CommitDecision decision) {
    if (!decision.transactionId().equals(invocation.transactionId())
        || !decision.invocationId().equals(invocation.invocationId())
        || decision.sequence() != Math.addExact(invocation.baseSequence(), 1)
        || decision.baseRevision() != invocation.baseRevision()
        || invocation.manifestRoot().filter(decision.manifestRoot()::equals).isEmpty()) {
      throw new IllegalStateException("selected decision conflicts with its invocation binding");
    }
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = unwrap(failure);
    return cause instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record ArchiveAttempt(SystemRecordStore.CreateResult result, Throwable failure) {}
  private record FinalizeAttempt(Boolean updated, Throwable failure) {}
}
