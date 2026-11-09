package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.PointResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;

/** Ordered fixed-base plus completed-workset view for one delete operation. */
final class DeleteViewCursor implements AsyncCursor<StoredRecord>, AutoCloseable {
  private final List<BucketViewCursor> bases;
  private final OrderedViewCursor ordered;
  private final DirectRecordReader values;
  private final long revision;
  private final RequestScope scope;
  private PointResult deliveredValue;
  private boolean reading;
  private boolean closed;

  DeleteViewCursor(TxnReadSession session, TxnWorkingSet working, KeyRange range,
      ToIntFunction<ByteKey> bucketSelector, CompareReadExecutor comparisons,
      BudgetManager.Account account, RequestScope scope, long pointBytes) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(working, "working");
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(bucketSelector, "bucketSelector");
    Objects.requireNonNull(comparisons, "comparisons");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || !comparisons.belongsTo(session, account, scope)) {
      throw new IllegalArgumentException("delete view inputs belong to different requests");
    }
    this.revision = session.currentRevision();
    this.values = new DirectRecordReader(session.reader(0), account, pointBytes);
    var limits = comparisons.limits();
    ScanSource source = comparisons.scanSource();
    var opened = new ArrayList<BucketViewCursor>();
    var merged = new ArrayList<AsyncCursor<StoredRecord>>();
    try {
      int first = 0;
      int end = limits.buckets();
      if (range.isEmpty()) end = 0;
      else if (range.kind() == KeyRange.Kind.POINT) {
        first = bucketSelector.applyAsInt(range.lowerInclusive().orElseThrow());
        if (first < 0 || first >= limits.buckets()) throw new IllegalArgumentException("invalid delete bucket");
        end = first + 1;
      }
      for (int bucket = first; bucket < end; bucket++) {
        var request = new ScanRequest(bucket, range, Optional.empty(), revision, ReadProjection.METADATA,
            1, limits.pageBytes(), limits.physicalRowsPerPage());
        var base = new BucketViewCursor(source, request, account, scope);
        opened.add(base);
        merged.add(working.merge(bucket, range, base));
      }
      if (merged.isEmpty()) merged.add(AsyncCursor.fromIterator(List.<StoredRecord>of().iterator()));
      this.ordered = new OrderedViewCursor(merged, account, scope);
      this.bases = List.copyOf(opened);
    } catch (RuntimeException | Error failure) {
      closeAll(opened, failure);
      throw failure;
    }
  }

  @Override public CompletableFuture<Optional<StoredRecord>> next() {
    synchronized (this) {
      if (closed || scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(inactive());
      if (reading) return CompletableFuture.failedFuture(new IllegalStateException("concurrent delete view reads"));
      releaseValue();
      reading = true;
    }
    CompletableFuture<Optional<StoredRecord>> operation = ordered.next().thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
      StoredRecord selected = found.orElseThrow();
      if (selected.metadata().state() == StorageMetadata.State.TOMBSTONE || selected.hasValue()) {
        return CompletableFuture.completedFuture(found);
      }
      return values.readForPreparation(selected.key(), revision, ReadProjection.FULL).thenApply(point -> {
        try {
          StoredRecord value = point.record().orElseThrow(() -> new IllegalStateException("delete value is missing"));
          if (!value.hasValue() || !value.key().equals(selected.key()) || !value.metadata().equals(selected.metadata())) {
            throw new IllegalStateException("delete value differs from its selected metadata");
          }
          synchronized (DeleteViewCursor.this) {
            if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
              throw new java.util.concurrent.CompletionException(inactive());
            }
            deliveredValue = point;
          }
          return Optional.of(value);
        } catch (RuntimeException | Error failure) {
          try { point.close(); }
          catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          throw failure;
        }
      });
    });
    return operation.whenComplete((ignored, failure) -> {
      synchronized (DeleteViewCursor.this) {
        reading = false;
        if (failure != null || closed) releaseValue();
        if (failure == null && closed) throw new java.util.concurrent.CompletionException(inactive());
      }
    }).copy();
  }

  @Override public synchronized void close() {
    if (closed) return;
    closed = true;
    Throwable failure = null;
    try { ordered.close(); }
    catch (RuntimeException | Error problem) { failure = problem; }
    failure = closeAll(bases, failure);
    if (!reading) {
      try { releaseValue(); }
      catch (RuntimeException | Error problem) {
        if (failure == null) failure = problem;
        else if (failure != problem) failure.addSuppressed(problem);
      }
    }
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException problem) throw problem;
  }

  private static Throwable closeAll(List<BucketViewCursor> cursors, Throwable failure) {
    for (BucketViewCursor cursor : cursors) {
      try { cursor.close(); }
      catch (RuntimeException | Error problem) {
        if (failure == null) failure = problem;
        else if (failure != problem) failure.addSuppressed(problem);
      }
    }
    return failure;
  }

  private void releaseValue() {
    if (deliveredValue != null) {
      deliveredValue.close();
      deliveredValue = null;
    }
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT
        : BackendFailure.Category.CANCELLED);
  }
}
