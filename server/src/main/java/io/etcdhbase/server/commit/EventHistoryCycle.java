package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Runs owner-only legacy backfill before the read-only shared tailer. */
final class EventHistoryCycle {
  private final EventBackfillCycle backfill;
  private final EventTailerCycle tailer;
  private final RequestMetrics metrics;
  private final AtomicBoolean backfillComplete = new AtomicBoolean();

  EventHistoryCycle(EventBackfillCycle backfill, EventTailerCycle tailer, RequestMetrics metrics) {
    this.backfill = Objects.requireNonNull(backfill, "backfill");
    this.tailer = Objects.requireNonNull(tailer, "tailer");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  CompletableFuture<EventHistoryWorker.CycleResult> run(
      Optional<CommitOwnerService.OwnerToken> owner, RequestScope scope) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    if (backfillComplete.get() || owner.isEmpty()) return tailer.run(scope);
    return backfill.run(owner.orElseThrow(), scope).handle((result, failure) -> {
      if (failure != null) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof java.util.concurrent.ExecutionException)
            && cause.getCause() != null) cause = cause.getCause();
        metrics.recordEventBackfillFailure(cause instanceof BackendFailure backend
            ? backend.category() : BackendFailure.Category.UNKNOWN);
        throw new CompletionException(cause);
      }
      if (result.progressed()) metrics.recordEventBackfillStep(result.complete());
      if (result.complete()) backfillComplete.set(true);
      return result;
    }).thenCompose(result -> {
      if (!result.complete()) {
        return CompletableFuture.completedFuture(
            new EventHistoryWorker.CycleResult(result.progressed(), false));
      }
      return tailer.run(scope);
    });
  }
}
