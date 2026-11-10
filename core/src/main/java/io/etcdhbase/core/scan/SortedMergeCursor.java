package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.core.storage.ScanDirection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Frozen-data merge with one head per bucket, global Limit and one extra visible result for More. */
public final class SortedMergeCursor implements AutoCloseable {
  private final List<BucketCursor> buckets;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final RequestScope.ScopedCleanup registration;
  private final RecordStore completionStore;
  private final PriorityQueue<Head> heap = new PriorityQueue<>(Comparator.comparing(head -> head.record.key()));
  private final Head[] heads;
  private final boolean[] exhausted;
  private final List<StoredRecord> output = new ArrayList<>();
  private final List<BudgetManager.Lease> outputLeases = new ArrayList<>();
  private final CompletableFuture<RangePage> result = new CompletableFuture<>();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopRequested = new AtomicBoolean();
  private final AtomicInteger draining = new AtomicInteger();
  private final AtomicReference<Completion> completion = new AtomicReference<>();
  private final AtomicReference<ValueCompletion> valueCompletion = new AtomicReference<>();
  private final RecordStore valueStore;
  // The drain loop alone mutates merge state; callbacks publish one bounded completion slot.
  private int initializing;
  private int refill = -1;
  private boolean waiting;
  private volatile boolean terminal;
  private int limit;
  private long pagePointReads;
  private ByteKey previous;
  private Head selected;
  private StoragePage.Cost cost = new StoragePage.Cost(0, 0, 0);

  public SortedMergeCursor(ScanSource source, ScanRequest template, int bucketCount,
      BudgetManager.Account account, RequestScope scope) {
    this(source, template, bucketCount, account, scope, null,
        source instanceof RecordStore recordStore ? recordStore : null);
  }

  public static SortedMergeCursor values(RecordStore store, ScanSource source, ScanRequest template,
      int bucketCount, BudgetManager.Account account, RequestScope scope) {
    return new SortedMergeCursor(source, template, bucketCount, account, scope,
        java.util.Objects.requireNonNull(store, "store"), store);
  }

  /** Metadata-only merge that still completes the same pinned view before returning its page. */
  public static SortedMergeCursor metadata(RecordStore store, ScanSource source,
      ScanRequest template, int bucketCount, BudgetManager.Account account, RequestScope scope) {
    return new SortedMergeCursor(source, template, bucketCount, account, scope,
        null, java.util.Objects.requireNonNull(store, "store"));
  }

  private SortedMergeCursor(ScanSource source, ScanRequest template, int bucketCount,
      BudgetManager.Account account, RequestScope scope, RecordStore valueStore,
      RecordStore completionStore) {
    if (template.direction() != ScanDirection.ASCENDING) {
      throw new UnsupportedOperationException("descending merge is not implemented");
    }
    if (bucketCount < 1 || bucketCount > 4096
        || account.reservation().heads() / bucketCount < template.maximumBytes()) {
      throw new IllegalArgumentException("all bucket heads require reserved memory before scanning");
    }
    if (account.scope() != scope) throw new IllegalArgumentException("query scope must own its budget");
    this.account = account;
    this.scope = scope;
    this.valueStore = valueStore;
    this.completionStore = completionStore;
    if (valueStore == null && template.projection() != ReadProjection.METADATA) {
      throw new IllegalArgumentException("full values require a pinned value reader");
    }
    heads = new Head[bucketCount];
    exhausted = new boolean[bucketCount];
    List<BucketCursor> opened = new ArrayList<>();
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      opened.add(new BucketCursor(source, new ScanRequest(bucket, template.range(), template.after(),
          template.revisionUpperBound(), ReadProjection.METADATA, 1, template.maximumBytes(), template.maximumPhysicalRows()), scope, account));
    }
    buckets = List.copyOf(opened);
    registration = scope.registerScopedCleanup(this);
  }

  public CompletableFuture<RangePage> read(int limit) {
    if (limit < 0) return CompletableFuture.failedFuture(new IllegalArgumentException("limit must not be negative"));
    if (!started.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("merge already started"));
    this.limit = limit;
    var delivery = new CompletableFuture<RangePage>();
    result.whenComplete((response, failure) -> {
      if (failure != null) delivery.completeExceptionally(failure);
      else if (!delivery.complete(response)) response.close();
    });
    drain();
    return delivery;
  }

  private void drain() {
    if (draining.getAndIncrement() != 0) return;
    int missed = 1;
    do {
      try {
        while (step()) { /* finite CPU steps until the next asynchronous page */ }
      } catch (Exception failure) {
        finish(false, failure);
      } catch (Error fatal) {
        // Drain every cursor and settle the public result before propagating the fatal Error.
        finish(false, fatal);
        throw fatal;
      }
      missed = draining.addAndGet(-missed);
    } while (missed != 0);
  }

  private boolean step() throws BackendFailure {
    Completion arrived = completion.getAndSet(null);
    ValueCompletion value = valueCompletion.getAndSet(null);
    if (terminal) {
      if (arrived != null && arrived.page != null) arrived.page.close();
      return false;
    }
    if (stopRequested.get() || scope.isClosed() || scope.isDeadlineExceeded()) {
      if (arrived != null && arrived.page != null) arrived.page.close();
      finish(false, new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      return false;
    }
    if (arrived != null) accept(arrived);
    if (value != null) acceptValue(value);
    if (waiting) return false;
    if (refill >= 0) {
      int bucket = refill;
      refill = -1;
      fetch(bucket);
      return true;
    }
    if (initializing < buckets.size()) {
      fetch(initializing++);
      return true;
    }
    if (heap.isEmpty()) { finish(false, null); return false; }
    Head head = heap.remove();
    if (previous != null && previous.compareTo(head.record.key()) >= 0) throw new IllegalStateException("duplicate or unordered bucket result");
    if (limit > 0 && output.size() == limit) { finish(true, null); return false; }
    long bytes = head.record.retainedBytes() + (valueStore == null ? 0 : head.record.metadata().valueLength());
    var lease = account.tryRetain(BudgetManager.Memory.OUTPUT, bytes).orElseThrow(SortedMergeCursor::resourceLimit);
    outputLeases.add(lease);
    if (valueStore == null) append(head, head.record);
    else {
      selected = head;
      waiting = true;
      PointRead.get(valueStore, head.record.key(), head.record.metadata().modificationRevision(),
          ReadProjection.FULL, account, false)
          .whenComplete((record, failure) -> { valueCompletion.set(new ValueCompletion(record, failure)); drain(); });
    }
    return true;
  }

  private void acceptValue(ValueCompletion arrived) throws BackendFailure {
    waiting = false;
    if (arrived.failure != null) throw unwrap(arrived.failure);
    StoredRecord record = arrived.record.orElseThrow(() -> new IllegalStateException("selected value version is missing"));
    if (!record.key().equals(selected.record.key()) || !record.metadata().equals(selected.record.metadata()) || !record.hasValue()) {
      throw new IllegalStateException("value does not match selected metadata version");
    }
    pagePointReads++;
    append(selected, record);
    selected = null;
  }

  private void append(Head head, StoredRecord record) {
    output.add(record);
    previous = record.key();
    heads[head.bucket] = null;
    head.lease.close();
    if (!exhausted[head.bucket]) refill = head.bucket;
  }

  private void fetch(int bucket) {
    waiting = true;
    buckets.get(bucket).fetch().whenComplete((page, failure) -> {
      completion.set(new Completion(bucket, page, failure));
      drain();
    });
  }

  private void accept(Completion arrived) throws BackendFailure {
    waiting = false;
    if (arrived.failure != null) throw unwrap(arrived.failure);
    try (var page = arrived.page) {
      cost = cost.plus(page.cost());
      Optional<StoredRecord> candidate = page.next();
      exhausted[arrived.bucket] = page.exhausted();
      if (candidate.isEmpty()) return;
      StoredRecord record = candidate.get();
      if (record.metadata().state() == StorageMetadata.State.TOMBSTONE) {
        if (!exhausted[arrived.bucket]) refill = arrived.bucket;
        return;
      }
      var lease = account.tryRetain(BudgetManager.Memory.HEAD, record.retainedBytes()).orElseThrow(SortedMergeCursor::resourceLimit);
      var head = new Head(arrived.bucket, record, lease);
      heads[arrived.bucket] = head;
      heap.add(head);
    }
  }

  private void finish(boolean more, Throwable failure) {
    if (terminal) return;
    terminal = true;
    Throwable terminalFailure = closeBuckets(failure);
    terminalFailure = releaseHeads(terminalFailure);
    heap.clear();
    selected = null;
    previous = null;
    ResponseBuild built = buildResponse(more, terminalFailure);
    terminalFailure = built.failure();
    if (built.response() == null) terminalFailure = releaseOutputLeases(terminalFailure);
    output.clear();
    outputLeases.clear();
    terminalFailure = closeRegistration(terminalFailure);
    terminalFailure = scopeFailure(terminalFailure);
    deliver(built.response(), terminalFailure);
  }

  private Throwable closeBuckets(Throwable failure) {
    for (var bucket : buckets) {
      try {
        bucket.close();
      } catch (RuntimeException | Error cleanup) {
        // Keep closing all buckets; finish settles the result before rethrowing a fatal Error.
        failure = combine(failure, cleanup);
      }
    }
    return failure;
  }

  private Throwable releaseHeads(Throwable failure) {
    for (Head head : heads) {
      if (head == null) continue;
      try {
        head.lease.close();
      } catch (RuntimeException | Error cleanup) {
        // Keep releasing all retained heads; the result observes the combined failure.
        failure = combine(failure, cleanup);
      }
    }
    java.util.Arrays.fill(heads, null);
    return failure;
  }

  private ResponseBuild buildResponse(boolean more, Throwable failure) {
    if (failure != null) return new ResponseBuild(null, failure);
    try {
      return new ResponseBuild(new RangePage(output, outputLeases, more, cost,
          pagePointReads, scope), null);
    } catch (RuntimeException | Error allocationFailure) {
      // Until construction succeeds, this cursor still owns the output leases and must release them.
      return new ResponseBuild(null, allocationFailure);
    }
  }

  private Throwable releaseOutputLeases(Throwable failure) {
    for (BudgetManager.Lease lease : outputLeases) {
      try {
        lease.close();
      } catch (RuntimeException | Error cleanup) {
        // Keep releasing all output leases; the result observes the combined failure.
        failure = combine(failure, cleanup);
      }
    }
    return failure;
  }

  private Throwable closeRegistration(Throwable failure) {
    try {
      if (registration != null) registration.close();
    } catch (Exception | Error cleanup) {
      // Finish records this cleanup failure before settling the public result.
      failure = combine(failure, cleanup);
    }
    return failure;
  }

  private Throwable scopeFailure(Throwable failure) {
    if (failure != null) return failure;
    boolean deadlineExceeded = scope.isDeadlineExceeded();
    if (scope.isClosed() || deadlineExceeded) {
      return new BackendFailure(deadlineExceeded
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    }
    return null;
  }

  private void deliver(RangePage response, Throwable failure) {
    if (failure != null) {
      failResponse(response, failure);
      return;
    }
    if (completionStore == null) {
      result.complete(response);
      return;
    }
    CompletableFuture<Void> pinCompletion;
    try {
      pinCompletion = completionStore.finishRead();
    } catch (RuntimeException | Error pinFailure) {
      // Settle the caller and release the response lease before a synchronous fatal error escapes.
      failResponse(response, pinFailure);
      return;
    }
    pinCompletion.whenComplete((ignored, pinFailure) -> {
      Throwable terminal = pinFailure == null ? null : unwrapTerminal(pinFailure);
      if (terminal == null && (scope.isClosed() || scope.isDeadlineExceeded())) {
        terminal = new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
      }
      if (terminal != null) {
        failResponse(response, terminal);
      } else {
        result.complete(response);
      }
    });
  }

  private void failResponse(RangePage response, Throwable failure) {
    Throwable terminal = failure;
    Error fatal = failure instanceof Error error ? error : null;
    if (response != null) {
      try {
        response.close();
      } catch (RuntimeException | Error cleanup) {
        // Settle the read future before rethrowing a fatal response-cleanup Error.
        terminal = combine(terminal, cleanup);
        if (fatal == null && cleanup instanceof Error error) fatal = error;
      }
    }
    result.completeExceptionally(terminal);
    if (fatal != null) throw fatal;
  }

  private static Throwable combine(Throwable primary, Throwable cleanup) {
    if (primary == null) return cleanup;
    if (primary != cleanup) primary.addSuppressed(cleanup);
    return primary;
  }

  private static Throwable unwrapTerminal(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof Error || cause instanceof BackendFailure) return cause;
    return new BackendFailure(BackendFailure.Category.UNKNOWN, cause);
  }

  @Override public void close() { stopRequested.set(true); drain(); }

  private static BackendFailure unwrap(Throwable failure) {
    failure = CompletionFailures.unwrapCompletion(failure);
    if (failure instanceof Error fatal) throw fatal;
    return failure instanceof BackendFailure backend ? backend
        : new BackendFailure(BackendFailure.Category.UNKNOWN, failure);
  }
  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
  private record Head(int bucket, StoredRecord record, BudgetManager.Lease lease) {}
  private record Completion(int bucket, BucketCursor.Page page, Throwable failure) {}
  private record ValueCompletion(Optional<StoredRecord> record, Throwable failure) {}
  private record ResponseBuild(RangePage response, Throwable failure) {}
}
