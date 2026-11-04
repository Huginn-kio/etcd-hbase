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
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Bounded service-owned lane for attempts that may have reached the publication CAS. */
public final class BoundedPublicationHandoff implements PublicationHandoff, AutoCloseable {
  private final ThreadPoolExecutor workers;
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
    requestScope.registerCleanup(task);
    if (!task.accept()) {
      task.releaseCapacity();
      return task.waitingResult.copy();
    }
    try {
      workers.execute(task::run);
    } catch (java.util.concurrent.RejectedExecutionException rejected) {
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
    if (closing.get() && active.get() == 0) drained.complete(null);
  }

  private final class HandoffTask<T> implements AutoCloseable {
    private enum State { WAITING, TRANSFERRED, RUNNING, CANCELLED, REJECTED, FINISHED }

    private final Object lock = new Object();
    private final RequestScope requestScope;
    private final Function<RequestScope, CompletableFuture<T>> serviceWork;
    private final CompletableFuture<T> waitingResult = new CompletableFuture<>();
    private State state = State.WAITING;
    private boolean capacityReleased;

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

    private void run() {
      synchronized (lock) {
        if (state != State.TRANSFERRED) return;
        state = State.RUNNING;
      }
      RequestScope serviceScope = null;
      CompletableFuture<T> serviceResult;
      try {
        serviceScope = RequestScope.withTimeout(serviceDeadline);
        serviceResult = Objects.requireNonNull(serviceWork.apply(serviceScope), "serviceWork result");
      } catch (RuntimeException failure) {
        serviceResult = CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        // This task already owns capacity and may have registered service resources. Preserve
        // the fatal result, drain that scope, and release capacity only after physical cleanup.
        failFatal(fatal, serviceScope);
        throw fatal;
      }
      RequestScope ownedServiceScope = serviceScope;
      serviceResult.whenComplete((value, failure) -> {
        synchronized (lock) {
          if (state != State.RUNNING) return;
          state = State.FINISHED;
        }
        Throwable serviceFailure = failure == null
            ? null : CompletionFailures.unwrapCompletion(failure);
        Error cleanupFatal = null;
        try {
          if (ownedServiceScope != null) ownedServiceScope.close();
        } catch (Error fatal) {
          cleanupFatal = fatal;
        }
        if (ownedServiceScope == null) releaseCapacityAfterCleanup(null);
        else releaseCapacityAfterCleanup(ownedServiceScope);

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
      });
    }

    private void failFatal(Error fatal, RequestScope serviceScope) {
      synchronized (lock) {
        if (state != State.RUNNING) return;
        state = State.FINISHED;
      }
      if (serviceScope == null) {
        waitingResult.completeExceptionally(fatal);
        releaseCapacity();
        return;
      }
      try {
        serviceScope.close();
      } catch (Error cleanupFailure) {
        suppressDistinct(fatal, cleanupFailure);
      }
      releaseCapacityAfterCleanup(serviceScope);
      waitingResult.completeExceptionally(fatal);
    }

    private void reject(RuntimeException failure) {
      if (!markRejected()) return;
      waitingResult.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
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
        serviceScope.cleanupCompletion().whenComplete((ignored, cleanupFailure) -> releaseCapacity());
      }
    }

    private void releaseCapacity() {
      synchronized (lock) {
        if (capacityReleased) return;
        capacityReleased = true;
      }
      BoundedPublicationHandoff.this.release();
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
