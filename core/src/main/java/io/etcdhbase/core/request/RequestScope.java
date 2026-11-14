package io.etcdhbase.core.request;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/** Owns request resources; stopping new work and completing cleanup are separate transitions. */
public final class RequestScope implements AutoCloseable {
  /** Logical backend-call cost measured at the storage adapter boundary. */
  public record StorageCostSnapshot(long logicalOperations, long logicalRequestPayloadBytes) {
    public StorageCostSnapshot {
      if (logicalOperations < 0 || logicalRequestPayloadBytes < 0) {
        throw new IllegalArgumentException("backend cost counters must not be negative");
      }
    }

    public StorageCostSnapshot minus(StorageCostSnapshot earlier) {
      Objects.requireNonNull(earlier, "earlier");
      if (logicalOperations < earlier.logicalOperations
          || logicalRequestPayloadBytes < earlier.logicalRequestPayloadBytes) {
        throw new IllegalArgumentException("backend cost snapshot moved backwards");
      }
      return new StorageCostSnapshot(logicalOperations - earlier.logicalOperations,
          logicalRequestPayloadBytes - earlier.logicalRequestPayloadBytes);
    }
  }

  private static final class StorageCostLedger {
    private final LongAdder logicalOperations = new LongAdder();
    private final LongAdder logicalRequestPayloadBytes = new LongAdder();

    private void record(long requestPayloadBytes) {
      logicalOperations.increment();
      logicalRequestPayloadBytes.add(requestPayloadBytes);
    }

    private StorageCostSnapshot snapshot() {
      return new StorageCostSnapshot(logicalOperations.sum(), logicalRequestPayloadBytes.sum());
    }
  }

  private final boolean hasDeadline;
  private final long deadlineNanos;
  private final LongSupplier nanoTime;
  private final Object lock = new Object();
  private final Deque<AutoCloseable> cleanupOrder = new ArrayDeque<>();
  private final Set<AutoCloseable> registrations =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final AtomicInteger cleanupFailureCount = new AtomicInteger();
  private final StorageCostLedger storageCostLedger;
  private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
  private final CompletableFuture<Void> physicalProducersCompletion = new CompletableFuture<>();
  private boolean cancelled;
  private boolean closed;
  private boolean cleanupFinished;
  private int pendingCleanup;
  private int physicalProducers;
  private Throwable physicalProducerFailure;

  public static RequestScope withTimeout(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
    long nanos;
    try {
      nanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      nanos = Long.MAX_VALUE;
    }
    return withTimeoutNanos(nanos, System::nanoTime);
  }

  /** Creates a deadline scope using a monotonic clock, primarily for deterministic tests. */
  public static RequestScope withTimeoutNanos(long timeoutNanos, LongSupplier nanoTime) {
    return new RequestScope(timeoutNanos, nanoTime);
  }

  public static RequestScope withoutDeadline() {
    return withoutDeadline(System::nanoTime);
  }

  /** Creates a no-deadline scope whose child scopes can share a deterministic monotonic clock. */
  public static RequestScope withoutDeadline(LongSupplier nanoTime) {
    return new RequestScope(false, 0, nanoTime);
  }

  /** Creates one child budget, capped by this scope's remaining deadline and sharing its clock. */
  public RequestScope childWithTimeoutNanos(long maximumTimeoutNanos) {
    if (maximumTimeoutNanos < 0) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long now = nanoTime.getAsLong();
    long parentRemaining = hasDeadline ? Math.max(0, deadlineNanos - now) : Long.MAX_VALUE;
    return new RequestScope(true, now + Math.min(maximumTimeoutNanos, parentRemaining),
        nanoTime, storageCostLedger);
  }

  /**
   * Creates an independently cancellable service continuation with a fresh deadline while
   * retaining this operation's storage-cost accounting.
   */
  public RequestScope forkWithTimeout(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
    long timeoutNanos;
    try {
      timeoutNanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      timeoutNanos = Long.MAX_VALUE;
    }
    return new RequestScope(true, deadline(timeoutNanos, nanoTime), nanoTime, storageCostLedger);
  }

  RequestScope(long timeoutNanos, LongSupplier nanoTime) {
    this(true, deadline(timeoutNanos, nanoTime), nanoTime);
  }

  private RequestScope(boolean hasDeadline, long deadlineNanos, LongSupplier nanoTime) {
    this(hasDeadline, deadlineNanos, nanoTime, new StorageCostLedger());
  }

  private RequestScope(boolean hasDeadline, long deadlineNanos, LongSupplier nanoTime,
      StorageCostLedger storageCostLedger) {
    this.hasDeadline = hasDeadline;
    this.deadlineNanos = deadlineNanos;
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.storageCostLedger = Objects.requireNonNull(storageCostLedger, "storageCostLedger");
  }

  public long remainingDeadlineNanos() {
    return hasDeadline ? Math.max(0, deadlineNanos - nanoTime.getAsLong()) : Long.MAX_VALUE;
  }

  public boolean isDeadlineExceeded() {
    return hasDeadline && remainingDeadlineNanos() == 0;
  }

  public boolean hasDeadline() {
    return hasDeadline;
  }

  public boolean isCancelled() {
    synchronized (lock) {
      return cancelled;
    }
  }

  /** True once new work is stopped, including while another caller is still closing resources. */
  public boolean isClosed() {
    synchronized (lock) {
      return closed;
    }
  }

  /**
   * Starts a nonblocking operation while cancellation and deadline closure are excluded.
   * The starter must return promptly; its invocation is the ownership handoff point.
   */
  public boolean startIfOpen(Runnable starter) {
    Objects.requireNonNull(starter, "starter");
    synchronized (lock) {
      if (closed || isDeadlineExceeded()) return false;
      starter.run();
      return true;
    }
  }

  public int cleanupFailureCount() {
    return cleanupFailureCount.get();
  }

  /** Records one adapter-level operation and its counted row/cell/range payload bytes. */
  public void recordStorageOperation(long logicalRequestPayloadBytes) {
    if (logicalRequestPayloadBytes < 0) {
      throw new IllegalArgumentException("storage request payload bytes must not be negative");
    }
    storageCostLedger.record(logicalRequestPayloadBytes);
  }

  public StorageCostSnapshot storageCostSnapshot() {
    return storageCostLedger.snapshot();
  }

  /** Read-only completion of all resources registered before or during the closing batch. */
  public CompletionStage<Void> cleanupCompletion() {
    return cleanupCompletion.minimalCompletionStage();
  }

  /**
   * Tracks a backend producer separately from its cancellable request result. Register it before
   * starting the producer; the stage must settle only after that producer has actually stopped.
   */
  public void trackPhysicalProducer(CompletionStage<?> producerCompletion) {
    Objects.requireNonNull(producerCompletion, "producerCompletion");
    synchronized (lock) {
      if (closed) return;
      physicalProducers = Math.addExact(physicalProducers, 1);
    }
    producerCompletion.whenComplete((ignored, failure) -> physicalProducerFinished(failure));
  }

  /** Completes after the scope is closed and every producer registered before closure has stopped. */
  public CompletionStage<Void> physicalProducersCompletion() {
    return physicalProducersCompletion.minimalCompletionStage();
  }

  public boolean cancel() {
    return finish(true, Runnable::run);
  }

  @Override
  public void close() {
    finish(false, Runnable::run);
  }

  public boolean cancelAsync(Executor cleanupExecutor) {
    return finish(true, cleanupExecutor);
  }

  public boolean closeAsync(Executor cleanupExecutor) {
    return finish(false, cleanupExecutor);
  }

  /** Tracks request-owned work, never the producer future of a shared connection. */
  public <T> CompletableFuture<T> track(CompletableFuture<T> operation) {
    Objects.requireNonNull(operation, "operation");
    AutoCloseable cancellation = () -> operation.cancel(true);
    registerCleanup(cancellation);
    operation.whenComplete(
        (value, failure) -> {
          synchronized (lock) {
            cleanupOrder.remove(cancellation);
            registrations.remove(cancellation);
          }
        });
    return operation;
  }

  /** Identity-deduplicated, reverse-order cleanup; a late resource is closed synchronously. */
  public void registerCleanup(AutoCloseable resource) {
    Objects.requireNonNull(resource, "resource");
    boolean joinsClosingBatch;
    synchronized (lock) {
      if (!registrations.add(resource)) return;
      if (!closed) {
        cleanupOrder.push(resource);
        return;
      }
      joinsClosingBatch = !cleanupFinished;
      if (joinsClosingBatch) pendingCleanup++;
    }
    closeResource(resource, joinsClosingBatch);
  }

  /** Transfers one resource into a releasable owner; finished pages leave no retained registration. */
  public ScopedCleanup registerScopedCleanup(AutoCloseable resource) {
    return registerScopedCleanup(resource, CompletableFuture.completedFuture(null));
  }

  /** The resource initiates cleanup; actualCompletion must settle after all physical cleanup ends. */
  public ScopedCleanup registerScopedCleanup(AutoCloseable resource, CompletionStage<Void> actualCompletion) {
    var registration = new ScopedCleanup(Objects.requireNonNull(resource, "resource"),
        Objects.requireNonNull(actualCompletion, "actualCompletion"));
    registerCleanup(registration);
    return registration;
  }

  int registeredResourceCount() {
    synchronized (lock) {
      return registrations.size();
    }
  }

  public final class ScopedCleanup implements AutoCloseable {
    private final AutoCloseable resource;
    private final CompletionStage<Void> actualCompletion;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();

    private ScopedCleanup(AutoCloseable resource, CompletionStage<Void> actualCompletion) {
      this.resource = resource;
      this.actualCompletion = actualCompletion;
    }

    @Override
    public void close() throws Exception {
      if (!closeStarted.compareAndSet(false, true)) return;
      Throwable failure = null;
      try {
        resource.close();
      } catch (Exception | Error problem) {
        failure = problem;
        throw problem;
      } finally {
        Throwable synchronousFailure = failure;
        actualCompletion.whenComplete((ignored, completionFailure) ->
            completeClose(synchronousFailure == null ? completionFailure : synchronousFailure));
      }
    }

    private void completeClose(Throwable failure) {
      synchronized (lock) {
        cleanupOrder.remove(this);
        registrations.remove(this);
      }
      // Duplicate closes never wait, including reentrant scope cleanup. Only the physical
      // completion detaches this registration, and callbacks run outside the state lock.
      if (failure == null) closeCompletion.complete(null);
      else closeCompletion.completeExceptionally(failure);
    }
  }

  /** Root-operation completion stops the request without replacing its original failure. */
  public <T> CompletionStage<T> own(CompletionStage<T> operation) {
    return Objects.requireNonNull(operation, "operation")
        .whenComplete((ignored, failure) -> close());
  }

  private boolean finish(boolean cancellation, Executor executor) {
    Objects.requireNonNull(executor, "cleanupExecutor");
    List<AutoCloseable> resources;
    Throwable stoppedFailure = null;
    boolean producersStopped;
    synchronized (lock) {
      if (closed) return false;
      cancelled = cancellation;
      closed = true;
      resources = new ArrayList<>(cleanupOrder);
      cleanupOrder.clear();
      // The batch sentinel keeps completion pending until the cleanup task itself has run.
      pendingCleanup = resources.size() + 1;
      producersStopped = physicalProducers == 0;
      if (producersStopped) stoppedFailure = physicalProducerFailure;
    }
    if (producersStopped) completePhysicalProducers(stoppedFailure);
    Runnable cleanup = () -> closeBatch(resources);
    try {
      executor.execute(cleanup);
    } catch (java.util.concurrent.RejectedExecutionException rejected) {
      // Generic callers retain cleanup responsibility when their executor refuses ownership.
      cleanup.run();
      throw rejected;
    }
    return true;
  }

  private void physicalProducerFinished(Throwable failure) {
    Throwable stoppedFailure = null;
    boolean producersStopped;
    synchronized (lock) {
      if (physicalProducers <= 0) throw new IllegalStateException("physical producer count underflow");
      if (failure != null) {
        if (physicalProducerFailure == null) physicalProducerFailure = failure;
        else if (physicalProducerFailure != failure) physicalProducerFailure.addSuppressed(failure);
      }
      physicalProducers--;
      producersStopped = closed && physicalProducers == 0;
      if (producersStopped) stoppedFailure = physicalProducerFailure;
    }
    if (producersStopped) completePhysicalProducers(stoppedFailure);
  }

  private void completePhysicalProducers(Throwable failure) {
    if (failure == null) physicalProducersCompletion.complete(null);
    else physicalProducersCompletion.completeExceptionally(failure);
  }

  private void closeBatch(List<AutoCloseable> resources) {
    Error fatal = null;
    try {
      for (var resource : resources) {
        try {
          closeResource(resource, true);
        } catch (Error failure) {
          if (fatal == null) fatal = failure;
        }
      }
    } finally {
      resourceClosed();
    }
    if (fatal != null) throw fatal;
  }

  private void closeResource(AutoCloseable resource, boolean joinsClosingBatch) {
    var actualCloseCompletion =
        resource instanceof ScopedCleanup registration ? registration.closeCompletion : null;
    try {
      resource.close();
    } catch (Exception failure) {
      if (actualCloseCompletion == null) cleanupFailureCount.incrementAndGet();
    } catch (Error failure) {
      if (actualCloseCompletion == null) cleanupFailureCount.incrementAndGet();
      throw failure;
    } finally {
      if (actualCloseCompletion == null) {
        if (joinsClosingBatch) resourceClosed();
      } else {
        // Another caller may own a still-running close. The closing batch observes that
        // attempt's outcome rather than treating the duplicate close's return as completion.
        actualCloseCompletion.whenComplete((ignored, failure) -> {
          if (failure != null) cleanupFailureCount.incrementAndGet();
          if (joinsClosingBatch) resourceClosed();
        });
      }
    }
  }

  private void resourceClosed() {
    boolean complete;
    synchronized (lock) {
      pendingCleanup--;
      complete = pendingCleanup == 0;
      if (complete) cleanupFinished = true;
    }
    // Completion callbacks can re-enter this scope; never run them under its state lock.
    if (complete) cleanupCompletion.complete(null);
  }

  private static long deadline(long timeoutNanos, LongSupplier nanoTime) {
    if (timeoutNanos < 0) throw new IllegalArgumentException("timeout must not be negative");
    return Objects.requireNonNull(nanoTime, "nanoTime").getAsLong() + timeoutNanos;
  }
}
