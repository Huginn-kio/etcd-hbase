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
    if (workerCount < 1 || workerCount > 256 || queueCapacity < 1 || queueCapacity > 65_536) {
      throw new IllegalArgumentException("invalid bounded publication capacity");
    }
    this.serviceDeadline = Objects.requireNonNull(serviceDeadline, "serviceDeadline");
    if (serviceDeadline.isZero() || serviceDeadline.isNegative()) {
      throw new IllegalArgumentException("service deadline must be positive");
    }
    int slots = Math.addExact(workerCount, queueCapacity);
    capacity = new Semaphore(slots);
    ThreadFactory threads = Thread.ofPlatform().daemon().name("commit-publish-", 0).factory();
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
      release();
      return task.waitingResult.copy();
    }
    try {
      workers.execute(task::run);
    } catch (java.util.concurrent.RejectedExecutionException rejected) {
      task.reject(rejected);
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
    private enum State { WAITING, TRANSFERRED, CANCELLED, REJECTED, FINISHED }

    private final Object lock = new Object();
    private final RequestScope requestScope;
    private final Function<RequestScope, CompletableFuture<T>> serviceWork;
    private final CompletableFuture<T> waitingResult = new CompletableFuture<>();
    private State state = State.WAITING;

    private HandoffTask(RequestScope requestScope,
        Function<RequestScope, CompletableFuture<T>> serviceWork) {
      this.requestScope = requestScope;
      this.serviceWork = serviceWork;
    }

    private boolean accept() {
      synchronized (lock) {
        if (state != State.WAITING || requestScope.isClosed() || closing.get()) {
          state = State.CANCELLED;
          waitingResult.completeExceptionally(new BackendFailure(
              requestScope.isDeadlineExceeded()
                  ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
          return false;
        }
        state = State.TRANSFERRED;
        return true;
      }
    }

    private void run() {
      RequestScope serviceScope = RequestScope.withTimeout(serviceDeadline);
      CompletableFuture<T> serviceResult;
      try {
        serviceResult = Objects.requireNonNull(serviceWork.apply(serviceScope), "serviceWork result");
      } catch (RuntimeException failure) {
        serviceResult = CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        // This task already owns capacity and may have registered service resources. Preserve
        // the fatal result, drain that scope, and release capacity only after physical cleanup.
        failFatal(fatal, serviceScope);
        throw fatal;
      }
      serviceResult.whenComplete((value, failure) -> {
        synchronized (lock) {
          state = State.FINISHED;
        }
        if (failure == null) waitingResult.complete(value);
        else waitingResult.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
        try {
          serviceScope.close();
        } finally {
          serviceScope.cleanupCompletion().whenComplete((ignored, cleanupFailure) -> release());
        }
      });
    }

    private void failFatal(Error fatal, RequestScope serviceScope) {
      synchronized (lock) {
        state = State.FINISHED;
      }
      waitingResult.completeExceptionally(fatal);
      try {
        serviceScope.close();
      } catch (Error cleanupFailure) {
        if (cleanupFailure != fatal) fatal.addSuppressed(cleanupFailure);
      } finally {
        serviceScope.cleanupCompletion().whenComplete((ignored, cleanupFailure) -> release());
      }
    }

    private void reject(RuntimeException failure) {
      synchronized (lock) {
        if (state != State.TRANSFERRED) return;
        state = State.REJECTED;
      }
      waitingResult.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
      release();
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
