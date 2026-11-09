package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Merges ordered bucket views while retaining at most one charged head per input. */
final class OrderedViewCursor implements AsyncCursor<StoredRecord>, AutoCloseable {
  private static final ThreadLocal<ArrayDeque<Runnable>> CONTINUATIONS = new ThreadLocal<>();
  private static final Executor CONTINUE = command -> {
    ArrayDeque<Runnable> queue = CONTINUATIONS.get();
    if (queue != null) {
      queue.addLast(command);
      return;
    }
    queue = new ArrayDeque<>();
    CONTINUATIONS.set(queue);
    try {
      queue.addLast(command);
      Runnable next;
      while ((next = queue.pollFirst()) != null) next.run();
    } finally {
      queue.clear();
      CONTINUATIONS.remove();
    }
  };

  private record Head(int source, StoredRecord record, BudgetManager.Lease lease) {}

  private final List<AsyncCursor<StoredRecord>> sources;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final PriorityQueue<Head> heads = new PriorityQueue<>(Comparator.comparing(head -> head.record().key()));
  private int initialized;
  private Head delivered;
  private ByteKey previous;
  private boolean reading;
  private boolean ended;
  private boolean closed;
  private boolean failed;

  OrderedViewCursor(List<AsyncCursor<StoredRecord>> sources, BudgetManager.Account account, RequestScope scope) {
    this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (this.sources.isEmpty() || this.sources.size() > 4096 || !account.belongsTo(scope)) {
      throw new IllegalArgumentException("invalid ordered view sources or request budget");
    }
  }

  /** The previous item remains owned until the next call or close; sources remain caller-owned. */
  @Override public CompletableFuture<Optional<StoredRecord>> next() {
    int refill = -1;
    try {
      synchronized (this) {
        if (closed || failed) return CompletableFuture.failedFuture(inactive());
        if (reading) return CompletableFuture.failedFuture(new IllegalStateException("concurrent ordered view reads"));
        if (ended) return CompletableFuture.completedFuture(Optional.empty());
        reading = true;
        if (delivered != null) {
          refill = delivered.source();
          delivered.lease().close();
          delivered = null;
        }
      }
    } catch (RuntimeException failure) {
      failAndRelease(failure);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      failAndRelease(fatal);
      throw fatal;
    }
    CompletableFuture<Optional<StoredRecord>> operation;
    try {
      CompletableFuture<Void> first = refill < 0 ? CompletableFuture.completedFuture(null) : load(refill);
      operation = first.thenComposeAsync(ignored -> initialize(), CONTINUE).thenApply(ignored -> take());
    } catch (RuntimeException failure) {
      failAndRelease(failure);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      failAndRelease(fatal);
      throw fatal;
    }
    return operation.whenComplete((ignored, failure) -> {
      synchronized (OrderedViewCursor.this) {
        reading = false;
        if (failure != null) failed = true;
        if (failed || closed) {
          try { releaseHeads(); }
          catch (RuntimeException | Error cleanup) {
            if (failure == null) throw cleanup;
            if (cleanup != failure) failure.addSuppressed(cleanup);
          }
        }
        if (failure == null && closed) throw new java.util.concurrent.CompletionException(inactive());
      }
    }).copy();
  }

  private CompletableFuture<Void> initialize() {
    requireActive();
    if (initialized == sources.size()) return CompletableFuture.completedFuture(null);
    int source = initialized++;
    return load(source).thenComposeAsync(ignored -> initialize(), CONTINUE);
  }

  private CompletableFuture<Void> load(int source) {
    requireActive();
    return Objects.requireNonNull(sources.get(source).next(), "ordered source future").thenAccept(found -> {
      requireActive();
      Objects.requireNonNull(found, "ordered source result").ifPresent(record -> {
        try {
          BudgetManager.Lease lease = account.tryRetain(BudgetManager.Memory.HEAD, record.retainedBytes())
              .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
          try { heads.add(new Head(source, record, lease)); }
          catch (RuntimeException | Error failure) {
            try { lease.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
          }
        } catch (BackendFailure failure) {
          throw new java.util.concurrent.CompletionException(failure);
        }
      });
    });
  }

  private synchronized Optional<StoredRecord> take() {
    requireActive();
    Head head = heads.poll();
    if (head == null) {
      ended = true;
      return Optional.empty();
    }
    if (previous != null && previous.compareTo(head.record().key()) >= 0) {
      head.lease().close();
      throw new IllegalStateException("bucket views contain duplicate or unordered keys");
    }
    previous = head.record().key();
    delivered = head;
    return Optional.of(head.record());
  }

  private synchronized void requireActive() {
    if (closed || failed || scope.isClosed() || scope.isDeadlineExceeded() || !account.isActive()) {
      throw new java.util.concurrent.CompletionException(inactive());
    }
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT
        : scope.isClosed() ? BackendFailure.Category.CANCELLED : BackendFailure.Category.RESOURCE_LIMIT);
  }

  @Override public synchronized void close() {
    closed = true;
    if (!reading) releaseHeads();
  }

  private synchronized void failAndRelease(Throwable failure) {
    reading = false;
    failed = true;
    try { releaseHeads(); }
    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
  }

  private void releaseHeads() {
    Throwable failure = null;
    if (delivered != null) {
      try { delivered.lease().close(); }
      catch (RuntimeException | Error problem) { failure = problem; }
      delivered = null;
    }
    Head head;
    while ((head = heads.poll()) != null) {
      try { head.lease().close(); }
      catch (RuntimeException | Error problem) {
        if (failure == null) failure = problem;
        else if (failure != problem) failure.addSuppressed(problem);
      }
    }
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException problem) throw problem;
  }
}
