package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Owns a sealed event cursor and its durable protection for a single producer scope. */
public final class ProtectedEventCursor implements AutoCloseable {
  private final SealedEventCursor cursor;
  private final ReadViewPinRegistry.Pin pin;
  private final RequestScope scope;
  private final CompletableFuture<Void> release = new CompletableFuture<>();
  private boolean reading;
  private boolean closed;

  /**
   * The caller registers the pin before reading the control barrier and verifies the selected
   * decision and compact boundary after registration. The scope must belong solely to this cursor.
   */
  public ProtectedEventCursor(SealedEventCursor cursor, ReadViewPinRegistry.Pin pin,
      RequestScope scope) {
    this.cursor = Objects.requireNonNull(cursor, "cursor");
    this.pin = Objects.requireNonNull(pin, "pin");
    this.scope = Objects.requireNonNull(scope, "scope");
    scope.registerCleanup(this);
  }

  /** Renew both before I/O and before delivery; lost protection makes the cursor terminal. */
  public synchronized CompletableFuture<Optional<PreparedEvent>> next(
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(budget, "budget");
    if (!budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event budget belongs to another scope"));
    }
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(inactive());
    }
    if (reading) return CompletableFuture.failedFuture(new IllegalStateException("event read already in flight"));
    reading = true;
    CompletableFuture<Optional<PreparedEvent>> result;
    try {
      result = pin.renew(scope).thenCompose(ignored -> cursor.next(budget))
          .thenCompose(event -> pin.renew(scope).thenApply(ignored -> deliver(event)));
    } catch (RuntimeException failure) {
      reading = false;
      close();
      return CompletableFuture.failedFuture(failure);
    }
    return result.whenComplete((ignored, failure) -> finished(failure));
  }

  private synchronized Optional<PreparedEvent> deliver(Optional<PreparedEvent> event) {
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new java.util.concurrent.CompletionException(inactive());
    }
    return event;
  }

  private void finished(Throwable failure) {
    synchronized (this) { reading = false; }
    if (failure != null) close();
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  /** Completes after physical backend producers stop and durable pin release finishes. */
  public CompletionStage<Void> releaseCompletion() { return release.minimalCompletionStage(); }

  @Override public void close() {
    synchronized (this) {
      if (closed) return;
      closed = true;
      cursor.close();
    }
    scope.close();
    scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) -> {
      if (producerFailure != null) {
        release.completeExceptionally(producerFailure);
        return;
      }
      try {
        pin.close();
        pin.releaseCompletion().whenComplete((released, releaseFailure) -> {
          if (releaseFailure != null) release.completeExceptionally(releaseFailure);
          else release.complete(null);
        });
      } catch (RuntimeException | Error failure) {
        release.completeExceptionally(failure);
        if (failure instanceof Error fatal) throw fatal;
      }
    });
  }
}
