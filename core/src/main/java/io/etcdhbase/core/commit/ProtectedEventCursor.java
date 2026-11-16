package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Owns a sealed event cursor and its durable protection for a single producer scope. */
public final class ProtectedEventCursor implements AutoCloseable {
  private SealedEventCursor cursor;
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
    this(pin, scope);
    attach(cursor);
  }

  ProtectedEventCursor(ReadViewPinRegistry.Pin pin, RequestScope scope) {
    this.pin = Objects.requireNonNull(pin, "pin");
    this.scope = Objects.requireNonNull(scope, "scope");
    // Scope cleanup must include the durable release, not only the backend producer drain.
    scope.registerScopedCleanup(this, release);
  }

  synchronized void attach(SealedEventCursor cursor) {
    Objects.requireNonNull(cursor, "cursor");
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      cursor.close();
      throw new IllegalStateException("event producer scope is closed");
    }
    if (this.cursor != null) throw new IllegalStateException("event cursor is already attached");
    this.cursor = cursor;
  }

  synchronized CompletableFuture<Void> renew() {
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(inactive());
    }
    return pin.renew(scope);
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
    if (cursor == null) return CompletableFuture.failedFuture(new IllegalStateException("event cursor is not attached"));
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
    var response = new CompletableFuture<Optional<PreparedEvent>>();
    response.whenComplete((ignored, failure) -> {
      if (response.isCancelled()) close();
    });
    result.whenComplete((event, failure) -> {
      finished(failure);
      if (failure != null) response.completeExceptionally(failure);
      else response.complete(event);
    });
    return response;
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
      if (cursor != null) cursor.close();
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
