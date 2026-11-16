package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Writes the index derived from a selected decision and verifies its durable value. */
final class EventIndexMaterializer {
  private static final int MAX_ATTEMPTS = 32;
  private final EventRevisionIndexRepository indexes;

  EventIndexMaterializer(EventRevisionIndexRepository indexes) {
    this.indexes = Objects.requireNonNull(indexes, "indexes");
  }

  CompletableFuture<Void> apply(CommitDecision decision, ManifestRoot root, RequestScope scope) {
    if (!decision.containsKeyChanges()) return CompletableFuture.completedFuture(null);
    return create(EventRevisionIndex.fromSelected(decision, root), scope, 0);
  }

  private CompletableFuture<Void> create(EventRevisionIndex expected, RequestScope scope, int attempt) {
    if (attempt >= MAX_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    return indexes.createIfAbsent(expected, scope)
        .handle((result, failure) -> new Attempt(result, failure))
        .thenCompose(result -> verify(expected, scope, attempt, result));
  }

  private CompletableFuture<Void> verify(EventRevisionIndex expected, RequestScope scope,
      int attempt, Attempt result) {
    if (result.failure() != null && !isUnknown(result.failure())) {
      return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(result.failure()));
    }
    if (result.failure() == null && (result.result() == null
        || result.result() == SystemRecordStore.CreateResult.CONFLICT)) {
      return CompletableFuture.failedFuture(new IllegalStateException("conflicting event revision index"));
    }
    return indexes.read(expected.decision().revision(), scope).thenCompose(stored -> {
      if (stored.isPresent()) {
        return stored.orElseThrow().equals(expected)
            ? CompletableFuture.completedFuture(null)
            : CompletableFuture.failedFuture(new IllegalStateException("event revision index differs from selected decision"));
      }
      if (result.failure() != null) return create(expected, scope, attempt + 1);
      return CompletableFuture.failedFuture(new IllegalStateException("created event revision index is not readable"));
    });
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    return cause instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record Attempt(SystemRecordStore.CreateResult result, Throwable failure) {}
}
