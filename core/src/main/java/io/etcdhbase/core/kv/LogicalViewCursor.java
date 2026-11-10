package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;

/** Merges metadata from one fixed read view and an optional completed transaction workset. */
final class LogicalViewCursor implements AsyncCursor<StoredRecord>, AutoCloseable {
  enum Traversal { KEY_ASCENDING, ANY }

  private final List<BucketViewCursor> bases;
  private final OrderedViewCursor ordered;
  private final SequentialViewCursor sequential;
  private boolean closed;

  LogicalViewCursor(ScanSource source, Optional<TxnWorkingSet> working, ScanRequest template,
      int bucketCount, ToIntFunction<ByteKey> bucketSelector,
      BudgetManager.Account account, RequestScope scope) {
    this(source, working, template, bucketCount, bucketSelector, account, scope, Traversal.KEY_ASCENDING);
  }

  LogicalViewCursor(ScanSource source, Optional<TxnWorkingSet> working, ScanRequest template,
      int bucketCount, ToIntFunction<ByteKey> bucketSelector,
      BudgetManager.Account account, RequestScope scope, Traversal traversal) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(working, "working");
    Objects.requireNonNull(template, "template");
    Objects.requireNonNull(bucketSelector, "bucketSelector");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(traversal, "traversal");
    if (bucketCount < 1 || bucketCount > 4096 || !account.belongsTo(scope)
        || template.projection() != ReadProjection.METADATA || template.maximumRows() != 1
        || template.after().isPresent()) {
      throw new IllegalArgumentException("invalid logical view inputs");
    }
    var opened = new ArrayList<BucketViewCursor>();
    var merged = new ArrayList<AsyncCursor<StoredRecord>>();
    try {
      int first = 0;
      int end = bucketCount;
      if (template.range().isEmpty()) end = 0;
      else if (template.range().kind() == KeyRange.Kind.POINT) {
        first = bucketSelector.applyAsInt(template.range().lowerInclusive().orElseThrow());
        if (first < 0 || first >= bucketCount) throw new IllegalArgumentException("invalid point bucket");
        end = first + 1;
      }
      for (int bucket = first; bucket < end; bucket++) {
        var request = new ScanRequest(bucket, template.range(), template.after(), template.revisionUpperBound(),
            ReadProjection.METADATA, 1, template.maximumBytes(), template.maximumPhysicalRows());
        var base = new BucketViewCursor(source, request, account, scope);
        opened.add(base);
        merged.add(working.isPresent() ? working.orElseThrow().merge(bucket, template.range(), base) : base);
      }
      if (merged.isEmpty()) merged.add(AsyncCursor.fromIterator(List.<StoredRecord>of().iterator()));
      this.ordered = traversal == Traversal.KEY_ASCENDING ? new OrderedViewCursor(merged, account, scope) : null;
      this.sequential = traversal == Traversal.ANY ? new SequentialViewCursor(merged, scope) : null;
      this.bases = List.copyOf(opened);
    } catch (RuntimeException | Error failure) {
      closeAll(opened, failure);
      throw failure;
    }
  }

  @Override public CompletableFuture<Optional<StoredRecord>> next() {
    return ordered == null ? sequential.next() : ordered.next();
  }

  @Override public synchronized void close() {
    if (closed) return;
    closed = true;
    Throwable failure = null;
    try {
      if (ordered == null) sequential.close();
      else ordered.close();
    }
    catch (RuntimeException | Error problem) { failure = problem; }
    failure = closeAll(bases, failure);
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
}
