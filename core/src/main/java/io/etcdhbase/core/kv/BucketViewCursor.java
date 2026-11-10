package io.etcdhbase.core.kv;

import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BucketCursor;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Holds a charged single-row head without holding a physical scanner between reads. */
final class BucketViewCursor implements AsyncCursor<StoredRecord>, AutoCloseable {
  private final BucketCursor cursor;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private BudgetManager.Lease delivered;
  private boolean reading;
  private boolean closed;

  BucketViewCursor(ScanSource source, ScanRequest request, BudgetManager.Account account, RequestScope scope) {
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (!account.belongsTo(scope) || request.maximumRows() != 1) {
      throw new IllegalArgumentException("bucket view requires one-row pages and the same request budget");
    }
    cursor = new BucketCursor(source, request, scope, account);
  }

  @Override public CompletableFuture<Optional<StoredRecord>> next() {
    synchronized (this) {
      if (closed || scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(inactive());
      if (reading) return CompletableFuture.failedFuture(new IllegalStateException("concurrent bucket view reads"));
      if (delivered != null) {
        delivered.close();
        delivered = null;
      }
      if (cursor.state() == BucketCursor.State.EXHAUSTED) return CompletableFuture.completedFuture(Optional.empty());
      reading = true;
    }
    CompletableFuture<Optional<StoredRecord>> operation = cursor.fetch().thenApply(this::receivePage);
    return operation.whenComplete(this::finishRead).copy();
  }

  private Optional<StoredRecord> receivePage(BucketCursor.Page page) {
    try (page) {
      Optional<StoredRecord> next = page.next();
      if (next.isEmpty() && !page.exhausted()) throw new IllegalStateException("bucket page made no progress");
      if (next.isEmpty()) return Optional.empty();
      retainHead(next.orElseThrow());
      return next;
    }
  }

  private void retainHead(StoredRecord record) {
    BudgetManager.Lease lease;
    try {
      lease = account.tryRetain(BudgetManager.Memory.HEAD, record.retainedBytes())
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    } catch (BackendFailure failure) {
      throw new java.util.concurrent.CompletionException(failure);
    }
    synchronized (this) {
      if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
        lease.close();
        throw new java.util.concurrent.CompletionException(inactive());
      }
      delivered = lease;
    }
  }

  private synchronized void finishRead(Optional<StoredRecord> ignored, Throwable failure) {
    reading = false;
    if (failure != null || closed) release();
    if (failure == null && closed) throw new java.util.concurrent.CompletionException(inactive());
  }

  @Override public synchronized void close() {
    closed = true;
    cursor.close();
    if (!reading) release();
  }

  private void release() {
    if (delivered != null) {
      delivered.close();
      delivered = null;
    }
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT
        : BackendFailure.Category.CANCELLED);
  }
}
