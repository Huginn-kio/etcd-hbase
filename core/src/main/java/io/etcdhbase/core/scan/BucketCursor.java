package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.core.storage.ScanDirection;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** One logical bucket cursor; physical page scanners belong to the request-bound store. */
public final class BucketCursor implements AutoCloseable {
  private static final ThreadLocal<ArrayDeque<Runnable>> CONTINUATIONS = new ThreadLocal<>();
  private static final Executor CONTINUE = task -> {
    ArrayDeque<Runnable> queue = CONTINUATIONS.get();
    if (queue != null) { queue.addLast(task); return; }
    queue = new ArrayDeque<>();
    CONTINUATIONS.set(queue);
    try {
      queue.addLast(task);
      Runnable next;
      while ((next = queue.pollFirst()) != null) next.run();
    } finally {
      queue.clear();
      CONTINUATIONS.remove();
    }
  };
  public enum State { IDLE, FETCHING, PAGE_READY, PAUSED, EXHAUSTED, FAILED, CLOSED }

  private final ScanSource store;
  private final ScanRequest template;
  private final RequestScope scope;
  private final BudgetManager.Account account;
  private final RequestScope.ScopedCleanup registration;
  private Optional<ByteKey> checkpoint;
  private BudgetManager.Lease checkpointLease;
  private State state = State.IDLE;
  private boolean pauseRequested;
  private ScanOperation activeRead;
  private Throwable failure;
  private CompletableFuture<Page> pending;
  private Page outstanding;

  public BucketCursor(ScanSource store, ScanRequest template, RequestScope scope) {
    this(store, template, scope, null);
  }

  public BucketCursor(ScanSource store, ScanRequest template, RequestScope scope, BudgetManager.Account account) {
    this.store = Objects.requireNonNull(store, "store");
    this.template = Objects.requireNonNull(template, "template");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (account != null && !account.belongsTo(scope)) throw new IllegalArgumentException("scan account belongs to another scope");
    this.account = account;
    this.checkpoint = template.after();
    this.registration = scope.registerScopedCleanup(this);
  }

  public synchronized State state() { return state; }

  public CompletableFuture<Page> fetch() {
    CompletableFuture<Page> result;
    ScanRequest request;
    synchronized (this) {
      if (state == State.FAILED) return CompletableFuture.failedFuture(failure);
      if (state == State.CLOSED || scope.isClosed()) return CompletableFuture.failedFuture(cancelled());
      if (scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.TIMEOUT));
      if (state != State.IDLE) return CompletableFuture.failedFuture(new IllegalStateException("cursor cannot fetch in this state"));
      state = State.FETCHING;
      result = new CompletableFuture<>();
      pending = result;
      request = template.resumeAfter(checkpoint);
    }
    startRead(result, request, null, new StoragePage.Cost(0, 0, 0));
    return result.copy();
  }

  private void startRead(CompletableFuture<Page> result, ScanRequest request, BudgetManager.Lease progressLease,
      StoragePage.Cost priorCost) {
    try {
      synchronized (this) {
        if (state != State.FETCHING || scope.isClosed()) {
          if (progressLease != null) progressLease.close();
          releaseCheckpoint();
          result.completeExceptionally(cancelled());
          return;
        }
      }
      ScanOperation read = Objects.requireNonNull(store.startScan(request), "page operation");
      boolean stop;
      synchronized (this) {
        activeRead = read;
        stop = state == State.CLOSED || scope.isClosed();
      }
      read.completion().whenComplete((page, problem) -> {
        try {
          if (progressLease != null) progressLease.close();
          synchronized (this) {
            if (activeRead == read) activeRead = null;
            if (state == State.CLOSED || scope.isClosed()) releaseCheckpoint();
          }
          if (problem == null && page != null && page.physicalProgress().isPresent()) {
            continuePhysical(result, request, read, page, priorCost);
          } else if (problem == null && page != null) {
            receive(result, request, read, new StoragePage(page.records(), page.exhausted(),
                priorCost.plus(page.cost())), null);
          } else {
            receive(result, request, read, page, problem);
          }
        } catch (RuntimeException failure) {
          failRead(result, read, failure);
        } catch (Error fatal) {
          failRead(result, read, fatal);
          throw fatal;
        }
      });
      if (stop) read.close();
    } catch (RuntimeException problem) {
      if (progressLease != null) progressLease.close();
      fail(result, problem);
    } catch (Error fatal) {
      if (progressLease != null) progressLease.close();
      fail(result, fatal);
      throw fatal;
    }
  }

  private void continuePhysical(CompletableFuture<Page> result, ScanRequest request, ScanOperation read,
      StoragePage page, StoragePage.Cost priorCost) {
    BudgetManager.Lease retained = null;
    try {
      var progress = page.physicalProgress().orElseThrow();
      var next = request.resumePhysical(progress);
      if (request.direction() != ScanDirection.DESCENDING
          || request.physicalProgress().map(previous -> request.direction().compare(
              previous.lastPhysicalRow(), progress.lastPhysicalRow()) >= 0).orElse(false)) {
        throw new IllegalStateException("physical scan did not advance");
      }
      long bytes = Math.addExact(128L, progress.lastPhysicalRow().length());
      if (account == null || bytes > request.maximumBytes()) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      retained = account.tryRetain(BudgetManager.Memory.HEAD, bytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      StoragePage.Cost cost = priorCost.plus(page.cost());
      read.close();
      synchronized (this) {
        if (state == State.CLOSED || scope.isClosed()) {
          retained.close();
          releaseCheckpoint();
          return;
        }
      }
      BudgetManager.Lease lease = retained;
      CONTINUE.execute(() -> startRead(result, next, lease, cost));
    } catch (BackendFailure | RuntimeException failure) {
      if (retained != null) retained.close();
      failRead(result, read, failure);
    } catch (Error fatal) {
      if (retained != null) retained.close();
      failRead(result, read, fatal);
      throw fatal;
    }
  }

  public synchronized void pause() {
    pauseRequested = true;
    if (state == State.IDLE) state = State.PAUSED;
  }

  public synchronized void resume() {
    pauseRequested = false;
    if (state == State.PAUSED) state = State.IDLE;
  }

  private void receive(CompletableFuture<Page> result, ScanRequest request, ScanOperation read, StoragePage source, Throwable problem) {
    if (problem != null) {
      failRead(result, read, problem);
      return;
    }
    try {
      validate(source, request);
    } catch (RuntimeException invalid) {
      failRead(result, read, invalid);
      return;
    }
    Page page = null;
    synchronized (this) {
      if (state != State.CLOSED && !scope.isClosed()) {
        page = new Page(source, read);
        outstanding = page;
        state = State.PAGE_READY;
      }
    }
    if (page == null) {
      try {
        read.close();
      } finally {
        result.completeExceptionally(cancelled());
      }
      return;
    }
    if (!result.complete(page)) page.close();
    synchronized (this) {
      if (pending == result) pending = null;
    }
  }

  private void failRead(CompletableFuture<Page> result, ScanOperation read, Throwable problem) {
    try {
      read.close();
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != problem) problem.addSuppressed(cleanupFailure);
      if (cleanupFailure instanceof Error fatal) {
        fail(result, problem);
        throw fatal;
      }
    }
    fail(result, problem);
  }

  private static void validate(StoragePage page, ScanRequest request) {
    Objects.requireNonNull(page, "page");
    if (page.records().isEmpty() && !page.exhausted()) throw new IllegalStateException("page made no logical progress");
    if (page.records().size() > request.maximumRows()) throw new IllegalStateException("page exceeded its row bound");
    long retained = 0;
    Optional<ByteKey> previous = request.after();
    for (StoredRecord record : page.records()) {
      if (!request.range().contains(record.key())
          || previous.map(key -> request.direction().compare(key, record.key()) >= 0).orElse(false)) {
        throw new IllegalStateException("page violates its ordered logical range");
      }
      retained = Math.addExact(retained, record.retainedBytes());
      previous = Optional.of(record.key());
    }
    if (retained > request.maximumBytes()) throw new IllegalStateException("page exceeded its byte bound");
  }

  private void fail(CompletableFuture<Page> result, Throwable problem) {
    synchronized (this) {
      if (state == State.CLOSED) {
        releaseCheckpoint();
        return;
      }
      failure = problem;
      state = State.FAILED;
      releaseCheckpoint();
      if (pending == result) pending = null;
    }
    result.completeExceptionally(problem);
    releaseRegistration();
  }

  private void release(Page page, Optional<ByteKey> consumed, boolean exhausted) {
    BackendFailure limit = null;
    synchronized (this) {
      if (outstanding != page) return;
      outstanding = null;
      if (state == State.CLOSED || state == State.FAILED) return;
      if (!exhausted && consumed.isPresent() && account != null) {
        try {
          long bytes = Math.addExact(128L, consumed.orElseThrow().length());
          BudgetManager.Lease next = account.tryRetain(BudgetManager.Memory.HEAD, bytes)
              .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
          releaseCheckpoint();
          checkpointLease = next;
        } catch (BackendFailure problem) {
          limit = problem;
          failure = problem;
          state = State.FAILED;
          releaseCheckpoint();
        }
      }
      if (limit == null) {
        if (consumed.isPresent()) checkpoint = consumed;
        if (exhausted) {
          state = State.EXHAUSTED;
          releaseCheckpoint();
        } else state = pauseRequested ? State.PAUSED : State.IDLE;
      }
      if (pending != null && pending.isDone()) pending = null;
    }
    if (exhausted || limit != null) releaseRegistration();
    if (limit != null) throw new CompletionException(limit);
  }

  private void releaseCheckpoint() {
    if (checkpointLease != null) {
      checkpointLease.close();
      checkpointLease = null;
    }
    if (state == State.CLOSED || state == State.EXHAUSTED || state == State.FAILED || scope.isClosed()) {
      checkpoint = Optional.empty();
    }
  }

  @Override
  public void close() {
    Page page;
    CompletableFuture<Page> operation;
    ScanOperation read;
    synchronized (this) {
      if (state == State.CLOSED) return;
      boolean fetching = state == State.FETCHING;
      if (state != State.EXHAUSTED && state != State.FAILED) state = State.CLOSED;
      if (!fetching) releaseCheckpoint();
      page = outstanding;
      outstanding = null;
      operation = pending;
      pending = null;
      read = activeRead;
      activeRead = null;
    }
    try {
      if (read != null) read.close();
    } finally {
      try {
        if (page != null) page.close();
      } finally {
        if (operation != null) operation.completeExceptionally(cancelled());
        releaseRegistration();
      }
    }
  }

  private void releaseRegistration() {
    if (registration == null) return; // A scope already closed during constructor ownership transfer.
    try {
      registration.close();
    } catch (Exception cleanupFailure) {
      throw new IllegalStateException("cursor cleanup failed", cleanupFailure);
    }
  }

  private static BackendFailure cancelled() {
    return new BackendFailure(BackendFailure.Category.CANCELLED);
  }

  /** Closing discards unread records and advances only through records actually handed out. */
  public final class Page implements AutoCloseable {
    private List<StoredRecord> records;
    private final boolean sourceExhausted;
    private final StoragePage.Cost cost;
    private ScanOperation read;
    private int position;
    private boolean closed;

    private Page(StoragePage source, ScanOperation read) {
      records = source.records();
      sourceExhausted = source.exhausted();
      cost = source.cost();
      this.read = read;
    }

    public synchronized Optional<StoredRecord> next() {
      requireOpen();
      if (scope.isClosed()) throw new CancellationException();
      return position == records.size() ? Optional.empty() : Optional.of(records.get(position++));
    }

    public synchronized boolean exhausted() {
      requireOpen();
      return sourceExhausted && position == records.size();
    }

    public StoragePage.Cost cost() { return cost; }

    @Override
    public void close() {
      Optional<ByteKey> consumed;
      boolean exhausted;
      ScanOperation ownedRead;
      synchronized (this) {
        if (closed) return;
        closed = true;
        consumed = position == 0 ? Optional.empty() : Optional.of(records.get(position - 1).key());
        exhausted = sourceExhausted && position == records.size();
        records = List.of();
        ownedRead = read;
        read = null;
      }
      try {
        release(this, consumed, exhausted);
      } finally {
        ownedRead.close();
      }
    }

    private void requireOpen() {
      if (closed) throw new IllegalStateException("cursor page is closed");
    }
  }
}
