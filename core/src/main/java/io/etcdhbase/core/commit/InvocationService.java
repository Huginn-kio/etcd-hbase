package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Resolves concurrent first binds and persists progress without rebinding an invocation ID. */
public final class InvocationService {
  private static final int MAX_CAS_ATTEMPTS = 16;
  private final InvocationRepository repository;

  public InvocationService(InvocationRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  /**
   * Returns the durable winner. A competing proposal may contain a different freshly generated
   * txnId; matching request and authorization digests join the already-bound attempt.
   */
  public CompletableFuture<InvocationBinding> bindOrJoin(
      InvocationBinding proposed, RequestScope scope) {
    Objects.requireNonNull(proposed, "proposed");
    Objects.requireNonNull(scope, "scope");
    if (proposed.state() != InvocationBinding.State.PREPARING) {
      throw new IllegalArgumentException("first invocation binding must start in PREPARING");
    }
    return bindOrJoin(proposed, scope, 0, null);
  }

  private CompletableFuture<InvocationBinding> bindOrJoin(
      InvocationBinding proposed, RequestScope scope, int attempt, Throwable priorUnknown) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(
          BackendFailure.Category.UNKNOWN, priorUnknown));
    }
    CompletableFuture<SystemRecordStore.CreateResult> creation;
    try {
      creation = repository.createIfAbsent(proposed, scope);
    } catch (RuntimeException failure) {
      creation = CompletableFuture.failedFuture(failure);
    }
    if (creation == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("invocation repository returned no create future"));
    }
    return creation.handle((result, failure) -> new CreateAttempt(result, failure))
        .thenCompose(result -> {
          if (result.failure() == null) {
            if (result.created() == null) {
              return CompletableFuture.failedFuture(
                  new IllegalStateException("invocation repository returned no create result"));
            }
            return readAndJoin(proposed, scope, result.created());
          }
          Throwable cause = CompletionFailures.unwrapCompletion(result.failure());
          if (!isUnknown(cause)) return CompletableFuture.failedFuture(cause);
          return resolveUnknownCreate(proposed, scope, attempt, cause);
        });
  }

  private CompletableFuture<InvocationBinding> resolveUnknownCreate(
      InvocationBinding proposed, RequestScope scope, int attempt, Throwable unknownFailure) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(new BackendFailure(
          scope.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED,
          unknownFailure));
    }
    return repository.read(proposed.invocationId(), scope)
        .handle((stored, readFailure) -> new ReadResult(stored, readFailure))
        .thenCompose(result -> {
          if (result.failure() != null) {
            Throwable cause = CompletionFailures.unwrapCompletion(unknownFailure);
            cause.addSuppressed(CompletionFailures.unwrapCompletion(result.failure()));
            return CompletableFuture.failedFuture(new BackendFailure(
                BackendFailure.Category.UNKNOWN, cause));
          }
          if (result.binding().isPresent()) {
            return joinMatchingRequest(proposed, result.binding().orElseThrow());
          }
          if (scope.isClosed() || scope.isDeadlineExceeded()) {
            return CompletableFuture.failedFuture(new BackendFailure(
                scope.isDeadlineExceeded()
                    ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED,
                unknownFailure));
          }
          return bindOrJoin(proposed, scope, attempt + 1, unknownFailure);
        });
  }

  private CompletableFuture<InvocationBinding> readAndJoin(
      InvocationBinding proposed, RequestScope scope, SystemRecordStore.CreateResult createResult) {
    return repository.read(proposed.invocationId(), scope).thenCompose(stored -> {
      if (stored.isEmpty()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      InvocationBinding binding = stored.orElseThrow();
      if (createResult == SystemRecordStore.CreateResult.CREATED
          && !binding.sameAttempt(proposed)) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "created invocation row differs from the proposed immutable binding"));
      }
      return joinMatchingRequest(proposed, binding);
    });
  }

  private static CompletableFuture<InvocationBinding> joinMatchingRequest(
      InvocationBinding proposed, InvocationBinding binding) {
    if (!binding.invocationId().equals(proposed.invocationId())
        || !binding.requestDigest().equals(proposed.requestDigest())
        || !binding.authorizationScopeDigest().equals(proposed.authorizationScopeDigest())) {
      return CompletableFuture.failedFuture(new IllegalStateException(
          "invocation ID is bound to a different request or authorization scope"));
    }
    // A competing owner may have proposed another txnId or base. The durable winner is
    // authoritative; a matching request joins it instead of rebinding the invocation.
    return CompletableFuture.completedFuture(binding);
  }

  /** Persists a state transition with exact-value CAS and durable reread after an unknown result. */
  public CompletableFuture<InvocationBinding> transition(
      InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
    validateTransition(expected, replacement);
    if (replacement.state() == InvocationBinding.State.NO_CHANGES) {
      throw new IllegalArgumentException("no-change completion requires durable result verification");
    }
    return transition(expected, replacement, scope, 0);
  }

  /**
   * Verifies an immutable result and the read protection before competing with seal on the same
   * durable invocation CAS. The caller retains page/IO capacity until all producers drain.
   */
  public CompletableFuture<InvocationBinding> completeWithoutChanges(
      InvocationBinding expected, ResultReference reference, ResultCatalog results,
      Supplier<CompletableFuture<Void>> finishRead, RequestScope scope) {
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(reference, "reference");
    Objects.requireNonNull(results, "results");
    Objects.requireNonNull(finishRead, "finishRead");
    Objects.requireNonNull(scope, "scope");
    if (expected.state() != InvocationBinding.State.PREPARING
        || !reference.identity().matches(expected)) {
      throw new IllegalArgumentException("result does not belong to the preparing invocation");
    }
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    InvocationBinding completed = expected.completeWithoutChanges(reference.token());
    validateTransition(expected, completed);
    return results.verify(reference, scope).thenCompose(ignored -> {
      if (scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.<Void>failedFuture(inactive(scope));
      }
      return Objects.requireNonNull(finishRead.get(), "read protection result");
    }).thenCompose(ignored -> {
      if (scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive(scope));
      }
      return transition(expected, completed, scope, 0);
    });
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private CompletableFuture<InvocationBinding> transition(
      InvocationBinding expected, InvocationBinding replacement, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(new IllegalStateException("invocation CAS retry limit exceeded"));
    }
    return repository.compareAndSet(expected, replacement, scope)
        .handle((updated, failure) -> new CasResult(updated, failure))
        .thenCompose(result -> {
          if (result.failure() != null && !isUnknown(CompletionFailures.unwrapCompletion(result.failure()))) {
            return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(result.failure()));
          }
          if (result.failure() == null && Boolean.TRUE.equals(result.updated())) {
            return CompletableFuture.completedFuture(replacement);
          }
          return repository.read(expected.invocationId(), scope).thenCompose(observed -> {
            if (observed.isEmpty()) {
              return CompletableFuture.failedFuture(new IllegalStateException("invocation binding disappeared"));
            }
            InvocationBinding current = observed.orElseThrow();
            if (!current.sameAttempt(expected)) {
              return CompletableFuture.failedFuture(new IllegalStateException("invocation immutable binding changed"));
            }
            if (sameProgress(current, replacement)) return CompletableFuture.completedFuture(current);
            if (!sameProgress(current, expected)) {
              return CompletableFuture.failedFuture(new IllegalStateException("invocation progressed concurrently"));
            }
            if (result.failure() != null) {
              return CompletableFuture.failedFuture(
                  CompletionFailures.unwrapCompletion(result.failure()));
            }
            if (result.updated() == null) {
              return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
            }
            return transition(current, replacement, scope, attempt + 1);
          });
        });
  }

  private static void validateTransition(InvocationBinding expected, InvocationBinding replacement) {
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(replacement, "replacement");
    if (!expected.sameAttempt(replacement) || !legalProgress(expected, replacement)) {
      throw new IllegalArgumentException("invocation transition changes immutable identity or regresses state");
    }
  }

  private static boolean legalProgress(InvocationBinding before, InvocationBinding after) {
    return switch (before.state()) {
      case PREPARING -> after.state() == InvocationBinding.State.SEALED
          || after.state() == InvocationBinding.State.NOT_SELECTED
          || after.state() == InvocationBinding.State.NO_CHANGES;
      case SEALED -> after.state() == InvocationBinding.State.SELECTED
          || after.state() == InvocationBinding.State.NOT_SELECTED;
      case SELECTED, NOT_SELECTED, NO_CHANGES -> after.state() == InvocationBinding.State.WINDOW_CLOSED;
      case WINDOW_CLOSED -> false;
    };
  }

  private static boolean sameProgress(InvocationBinding left, InvocationBinding right) {
    return left.state() == right.state() && left.manifestRoot().equals(right.manifestRoot())
        && left.resultReference().equals(right.resultReference());
  }

  private static boolean isUnknown(Throwable failure) {
    if (!(failure instanceof BackendFailure backend)) return false;
    return switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record CreateAttempt(SystemRecordStore.CreateResult created, Throwable failure) {}
  private record ReadResult(java.util.Optional<InvocationBinding> binding, Throwable failure) {}
  private record CasResult(Boolean updated, Throwable failure) {}
}
