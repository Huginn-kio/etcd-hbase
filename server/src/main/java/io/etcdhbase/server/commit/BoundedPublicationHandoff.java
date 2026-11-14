package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PublicationHandoff;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Bounded service-owned lane for attempts that may have reached the publication CAS. */
public final class BoundedPublicationHandoff implements PublicationHandoff, AutoCloseable {
  private static final Executor TIMEOUT_CLEANUP =
      task -> Thread.ofVirtual().name("commit-publish-timeout").start(task);
  private final ThreadPoolExecutor workers;
  private final ScheduledThreadPoolExecutor deadlines;
  private final Semaphore capacity;
  private final Duration serviceDeadline;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicInteger active = new AtomicInteger();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  public BoundedPublicationHandoff(int workerCount, int queueCapacity, Duration serviceDeadline) {
    this(workerCount, queueCapacity, serviceDeadline,
        Thread.ofPlatform().daemon().name("commit-publish-", 0).factory());
  }

  BoundedPublicationHandoff(
      int workerCount,
      int queueCapacity,
      Duration serviceDeadline,
      ThreadFactory threads) {
    if (workerCount < 1 || workerCount > 256 || queueCapacity < 1 || queueCapacity > 65_536) {
      throw new IllegalArgumentException("invalid bounded publication capacity");
    }
    this.serviceDeadline = Objects.requireNonNull(serviceDeadline, "serviceDeadline");
    Objects.requireNonNull(threads, "threads");
    if (serviceDeadline.isZero() || serviceDeadline.isNegative()) {
      throw new IllegalArgumentException("service deadline must be positive");
    }
    int slots = Math.addExact(workerCount, queueCapacity);
    capacity = new Semaphore(slots);
    workers = new ThreadPoolExecutor(workerCount, workerCount, 0, TimeUnit.NANOSECONDS,
        new ArrayBlockingQueue<>(queueCapacity), threads, new ThreadPoolExecutor.AbortPolicy());
    deadlines = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("commit-publish-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  @Override
  public <T> CompletableFuture<T> transfer(
      InvocationBinding invocation,
      RequestScope requestScope,
      Function<RequestScope, CompletableFuture<T>> serviceWork) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(requestScope, "requestScope");
    Objects.requireNonNull(serviceWork, "serviceWork");
    if (closing.get()) return CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    if (!capacity.tryAcquire()) return CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    active.incrementAndGet();
    var task = new HandoffTask<T>(requestScope, serviceWork);
    try {
      task.registration = requestScope.registerScopedCleanup(task, task.physicalDrain);
      if (!task.accept()) {
        task.releaseCapacity();
        return task.waitingResult.copy();
      }
    } catch (RuntimeException failure) {
      task.rejectAdmission(failure);
      return task.waitingResult.copy();
    } catch (Error fatal) {
      task.rejectAdmission(fatal);
      throw fatal;
    }
    try {
      task.armDeadline();
      workers.execute(task::run);
    } catch (RuntimeException rejected) {
      task.reject(rejected);
    } catch (Error fatal) {
      // Thread creation may fail after execute has queued this task. Reject it before returning
      // capacity so a later worker cannot invoke serviceWork for an unaccepted attempt.
      task.rejectFatal(fatal);
      throw fatal;
    }
    return task.waitingResult.copy();
  }

  /** Completes after all accepted service work has completed and its owned resources have drained. */
  public CompletableFuture<Void> drained() {
    return drained.copy();
  }

  /** Waits for the owned worker and deadline threads after accepted work has drained. */
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("negative termination timeout");
    long nanos = durationNanos(timeout);
    long start = System.nanoTime();
    if (!workers.awaitTermination(nanos, TimeUnit.NANOSECONDS)) return false;
    long elapsed = Math.max(0, System.nanoTime() - start);
    return deadlines.awaitTermination(Math.max(0, nanos - elapsed), TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {
    if (closing.compareAndSet(false, true)) workers.shutdown();
    completeIfDrained();
  }

  private void release() {
    active.decrementAndGet();
    capacity.release();
    completeIfDrained();
  }

  private void completeIfDrained() {
    if (closing.get() && active.get() == 0) {
      deadlines.shutdown();
      drained.complete(null);
    }
  }

  private static long durationNanos(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }

  private final class HandoffTask<T> implements AutoCloseable {
    private enum State { WAITING, TRANSFERRED, RUNNING, CANCELLED, REJECTED, FINISHED }

    private final Object lock = new Object();
    private final RequestScope requestScope;
    private final Function<RequestScope, CompletableFuture<T>> serviceWork;
    private final CompletableFuture<T> waitingResult = new CompletableFuture<>();
    private final CompletableFuture<Void> physicalDrain = new CompletableFuture<>();
    private RequestScope.ScopedCleanup registration;
    private volatile ScheduledFuture<?> deadline;
    private RequestScope runningScope;
    private State state = State.WAITING;
    private boolean capacityReleased;
    private boolean timedOut;
    private boolean timeoutRunning;
    private boolean releaseAfterTimeout;
    private Throwable deferredCleanupFailure;

    private HandoffTask(RequestScope requestScope,
        Function<RequestScope, CompletableFuture<T>> serviceWork) {
      this.requestScope = requestScope;
      this.serviceWork = serviceWork;
    }

    private boolean accept() {
      boolean requestOpen = requestScope.startIfOpen(() -> {
        synchronized (lock) {
          if (state != State.WAITING) return;
          if (closing.get()) {
            state = State.CANCELLED;
            return;
          }
          state = State.TRANSFERRED;
          // Parent cleanup and its memory owners must include the independent service producer.
          requestScope.trackPhysicalProducer(physicalDrain);
        }
      });
      if (!requestOpen) {
        synchronized (lock) {
          if (state == State.WAITING) state = State.CANCELLED;
        }
      }
      boolean accepted;
      synchronized (lock) {
        accepted = state == State.TRANSFERRED;
      }
      if (!accepted) {
        waitingResult.completeExceptionally(new BackendFailure(
            requestScope.isDeadlineExceeded()
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      }
      return accepted;
    }

    private void armDeadline() {
      deadline = deadlines.schedule(this::timeout, durationNanos(serviceDeadline), TimeUnit.NANOSECONDS);
    }

    private void run() {
      RequestScope serviceScope;
      try {
        serviceScope = requestScope.forkWithTimeout(serviceDeadline);
      } catch (RuntimeException failure) {
        reject(failure);
        return;
      } catch (Error fatal) {
        rejectFatal(fatal);
        throw fatal;
      }
      boolean expired;
      synchronized (lock) {
        if (state != State.TRANSFERRED) {
          serviceScope.close();
          return;
        }
        expired = timedOut;
        if (expired) state = State.FINISHED;
        else {
          state = State.RUNNING;
          runningScope = serviceScope;
        }
      }
      if (expired) {
        serviceScope.close();
        releaseCapacity();
        return;
      }
      CompletableFuture<T> serviceResult;
      try {
        serviceResult = Objects.requireNonNull(serviceWork.apply(serviceScope), "serviceWork result");
      } catch (RuntimeException failure) {
        serviceResult = CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        // This task already owns capacity and may have registered service resources. Preserve
        // the fatal result, drain that scope, and release capacity only after physical cleanup.
        cancelDeadline();
        failFatal(fatal, serviceScope);
        throw fatal;
      }
      serviceResult.whenComplete((value, failure) -> {
        cancelDeadline();
        finishServiceWork(serviceScope, value, failure);
      });
    }

    private void finishServiceWork(RequestScope serviceScope, T value, Throwable failure) {
      if (!markFinished()) return;
      Throwable serviceFailure = failure == null
          ? null : CompletionFailures.unwrapCompletion(failure);
      if ((timedOut || serviceScope != null && serviceScope.isDeadlineExceeded())
          && !(serviceFailure instanceof Error)) {
        serviceFailure = new BackendFailure(BackendFailure.Category.TIMEOUT, serviceFailure);
      }
      Error cleanupFatal = closeServiceScope(serviceScope);
      releaseCapacityAfterCleanup(serviceScope);
      publishServiceResult(value, serviceFailure, cleanupFatal);
    }

    private void timeout() {
      RequestScope serviceScope;
      synchronized (lock) {
        if (state != State.TRANSFERRED && state != State.RUNNING) return;
        timedOut = true;
        timeoutRunning = true;
        serviceScope = runningScope;
      }
      try {
        TIMEOUT_CLEANUP.execute(() -> expire(serviceScope));
      } catch (RuntimeException | Error failure) {
        // If a thread cannot be started, retain ownership and perform the cleanup here.
        try { expire(serviceScope); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    private void expire(RequestScope serviceScope) {
      try {
        if (serviceScope != null) serviceScope.cancelAsync(TIMEOUT_CLEANUP);
      } finally {
        try {
          // After transfer, timeout is an unknown outcome, never proof that the write failed.
          waitingResult.completeExceptionally(new BackendFailure(BackendFailure.Category.TIMEOUT));
        } finally {
          Throwable cleanupFailure;
          boolean release;
          synchronized (lock) {
            timeoutRunning = false;
            release = releaseAfterTimeout;
            cleanupFailure = deferredCleanupFailure;
            deferredCleanupFailure = null;
          }
          if (release) releaseCapacity(cleanupFailure);
        }
      }
    }

    private void cancelDeadline() {
      ScheduledFuture<?> scheduled = deadline;
      if (scheduled != null) scheduled.cancel(false);
    }

    private boolean markFinished() {
      synchronized (lock) {
        if (state != State.RUNNING) return false;
        state = State.FINISHED;
        return true;
      }
    }

    private Error closeServiceScope(RequestScope serviceScope) {
      if (serviceScope == null) return null;
      try {
        serviceScope.close();
        return null;
      } catch (Error fatal) {
        return fatal;
      }
    }

    private void publishServiceResult(T value, Throwable serviceFailure, Error cleanupFatal) {
      if (cleanupFatal != null) {
        Throwable propagated = cleanupFatal;
        if (serviceFailure instanceof Error serviceFatal) {
          propagated = serviceFatal;
          suppressDistinct(serviceFatal, cleanupFatal);
        } else {
          suppressDistinct(cleanupFatal, serviceFailure);
        }
        waitingResult.completeExceptionally(propagated);
      } else if (serviceFailure == null) {
        waitingResult.complete(value);
      } else {
        waitingResult.completeExceptionally(serviceFailure);
      }
    }

    private void failFatal(Error fatal, RequestScope serviceScope) {
      if (!markFinished()) return;
      if (serviceScope == null) {
        waitingResult.completeExceptionally(fatal);
        releaseCapacity();
        return;
      }
      suppressDistinct(fatal, closeServiceScope(serviceScope));
      releaseCapacityAfterCleanup(serviceScope);
      waitingResult.completeExceptionally(fatal);
    }

    private void reject(RuntimeException failure) {
      if (!markRejected()) return;
      waitingResult.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
      releaseCapacity();
    }

    private void rejectAdmission(Throwable failure) {
      synchronized (lock) {
        if (state == State.WAITING || state == State.TRANSFERRED) state = State.REJECTED;
      }
      waitingResult.completeExceptionally(failure);
      releaseCapacity();
    }

    private void rejectFatal(Error failure) {
      if (!markRejected()) return;
      waitingResult.completeExceptionally(failure);
      releaseCapacity();
    }

    private boolean markRejected() {
      synchronized (lock) {
        if (state != State.TRANSFERRED) return false;
        state = State.REJECTED;
        return true;
      }
    }

    private void releaseCapacityAfterCleanup(RequestScope serviceScope) {
      if (serviceScope == null) {
        releaseCapacity();
      } else {
        CompletableFuture.allOf(serviceScope.cleanupCompletion().toCompletableFuture(),
            serviceScope.physicalProducersCompletion().toCompletableFuture())
            .whenComplete((ignored, cleanupFailure) -> releaseCapacity(cleanupFailure));
      }
    }

    private void releaseCapacity() {
      releaseCapacity(null);
    }

    private void releaseCapacity(Throwable cleanupFailure) {
      synchronized (lock) {
        if (capacityReleased) return;
        if (timeoutRunning) {
          releaseAfterTimeout = true;
          if (deferredCleanupFailure == null) deferredCleanupFailure = cleanupFailure;
          else if (cleanupFailure != null && deferredCleanupFailure != cleanupFailure) {
            deferredCleanupFailure.addSuppressed(cleanupFailure);
          }
          return;
        }
        capacityReleased = true;
      }
      cancelDeadline();
      BoundedPublicationHandoff.this.release();
      if (cleanupFailure == null) physicalDrain.complete(null);
      else physicalDrain.completeExceptionally(CompletionFailures.unwrapCompletion(cleanupFailure));
      try {
        if (registration != null) registration.close();
      } catch (Exception failure) {
        throw new IllegalStateException("publication request registration did not close", failure);
      }
    }

    private void suppressDistinct(Throwable primary, Throwable secondary) {
      if (secondary != null && primary != secondary) primary.addSuppressed(secondary);
    }

    @Override
    public void close() {
      synchronized (lock) {
        if (state != State.WAITING) return;
        state = State.CANCELLED;
      }
      waitingResult.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
    }
  }

}
