package io.etcdhbase.core.kv;

import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Visits independent bucket views until one has a record; the caller owns the sources. */
final class SequentialViewCursor implements AsyncCursor<StoredRecord>, AutoCloseable {
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();

  private final List<AsyncCursor<StoredRecord>> sources;
  private final RequestScope scope;
  private int index;
  private boolean reading;
  private boolean closed;
  private boolean failed;

  SequentialViewCursor(List<AsyncCursor<StoredRecord>> sources, RequestScope scope) {
    this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
    this.scope = Objects.requireNonNull(scope, "scope");
    if (this.sources.isEmpty() || this.sources.size() > 4096) {
      throw new IllegalArgumentException("invalid sequential view sources");
    }
  }

  @Override public CompletableFuture<Optional<StoredRecord>> next() {
    synchronized (this) {
      if (closed || failed || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive());
      }
      if (reading) return CompletableFuture.failedFuture(new IllegalStateException("concurrent sequential view reads"));
      reading = true;
    }
    CompletableFuture<Optional<StoredRecord>> operation;
    try {
      operation = advance();
    } catch (RuntimeException failure) {
      synchronized (this) { reading = false; failed = true; }
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      synchronized (this) { reading = false; failed = true; }
      throw fatal;
    }
    return operation.whenComplete((ignored, failure) -> {
      synchronized (SequentialViewCursor.this) {
        reading = false;
        if (failure != null) failed = true;
        if (failure == null && (closed || scope.isClosed() || scope.isDeadlineExceeded())) {
          throw new CompletionException(inactive());
        }
      }
    }).copy();
  }

  private CompletableFuture<Optional<StoredRecord>> advance() {
    synchronized (this) {
      if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive());
      }
      if (index == sources.size()) return CompletableFuture.completedFuture(Optional.empty());
    }
    return Objects.requireNonNull(sources.get(index).next(), "sequential source future")
        .thenComposeAsync(found -> {
          Objects.requireNonNull(found, "sequential source result");
          synchronized (SequentialViewCursor.this) {
            if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
              return CompletableFuture.failedFuture(inactive());
            }
            if (found.isPresent()) return CompletableFuture.completedFuture(found);
            index++;
          }
          return advance();
        }, CONTINUE);
  }

  @Override public synchronized void close() { closed = true; }

  private BackendFailure inactive() {
    if (scope.isDeadlineExceeded()) return new BackendFailure(BackendFailure.Category.TIMEOUT);
    if (closed || scope.isClosed()) return new BackendFailure(BackendFailure.Category.CANCELLED);
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
