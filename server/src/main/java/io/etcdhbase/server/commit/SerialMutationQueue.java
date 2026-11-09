package io.etcdhbase.server.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Bounded per-table-group lane: one dispatched mutation owns the lane until physical cleanup. */
public final class SerialMutationQueue implements AutoCloseable {
  public record Snapshot(int waiting, boolean active, long retainedBytes, boolean closing) {}

  private final Object lock = new Object();
  private final ArrayDeque<Task<?>> waiting = new ArrayDeque<>();
  private final int maxQueued;
  private final long maxBytes;
  private final Duration serviceDeadline;
  private final ThreadPoolExecutor worker;
  private final ScheduledThreadPoolExecutor deadlines;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private Task<?> active;
  private long retainedBytes;
  private boolean closing;

  public SerialMutationQueue(int maxQueued, long maxBytes, Duration serviceDeadline) {
    this.serviceDeadline = Objects.requireNonNull(serviceDeadline, "serviceDeadline");
    if (maxQueued < 0 || maxQueued > 65_536 || maxBytes < 1 || serviceDeadline.isZero()
        || serviceDeadline.isNegative() || serviceDeadline.compareTo(Duration.ofMinutes(10)) > 0) {
      throw new IllegalArgumentException("invalid mutation queue limits");
    }
    this.maxQueued = maxQueued;
    this.maxBytes = maxBytes;
    this.worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new java.util.concurrent.ArrayBlockingQueue<>(1),
        Thread.ofPlatform().daemon().name("mutation-owner-", 0).factory());
    this.deadlines = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("mutation-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  /**
   * Before dispatch, request cancellation removes the queued task. After dispatch only the
   * waiter is cancelled; work continues in a service scope. Returned values must be resource-free
   * receipts; output bytes are resolved and retained separately under the request's budget.
   * Work must settle its future when its service scope is cancelled.
   */
  public <T> CompletableFuture<T> submit(long requestBytes, RequestScope requestScope,
      Function<RequestScope, CompletableFuture<T>> work) {
    Objects.requireNonNull(requestScope, "requestScope");
    Objects.requireNonNull(work, "work");
    if (requestBytes <= 0 || requestBytes > maxBytes) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    Task<T> task = new Task<>(requestBytes, requestScope, work);
    task.registration = requestScope.registerScopedCleanup(task);
    boolean open = requestScope.startIfOpen(() -> {
      synchronized (lock) {
        if (task.state != State.NEW) return;
        if (closing) {
          task.state = State.REJECTED;
          task.rejection = BackendFailure.Category.UNAVAILABLE;
        } else if ((active == null ? 0 : 1) + waiting.size() >= (long) maxQueued + 1
            || requestBytes > maxBytes - retainedBytes) {
          task.state = State.REJECTED;
          task.rejection = BackendFailure.Category.RESOURCE_LIMIT;
        } else {
          task.state = State.WAITING;
          waiting.addLast(task);
          retainedBytes += requestBytes;
        }
      }
    });
    if (!open) {
      task.close();
      task.detach();
    }
    if (task.rejection != null) {
      task.response.completeExceptionally(new BackendFailure(task.rejection));
      task.detach();
    } else {
      scheduleNext();
    }
    return task.response.copy();
  }

  public Snapshot snapshot() {
    synchronized (lock) { return new Snapshot(waiting.size(), active != null, retainedBytes, closing); }
  }

  /** Completes after accepted work and its physical producers drain, once closing begins. */
  public CompletableFuture<Void> drained() { return drained.copy(); }

  /** Waits separately for the queue's worker and timer threads to terminate after close. */
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("negative termination timeout");
    long nanos;
    try { nanos = timeout.toNanos(); } catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
    long start = System.nanoTime();
    if (!worker.awaitTermination(nanos, TimeUnit.NANOSECONDS)) return false;
    long elapsed = Math.max(0, System.nanoTime() - start);
    return deadlines.awaitTermination(Math.max(0, nanos - elapsed), TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {
    List<Task<?>> rejected;
    synchronized (lock) {
      if (closing) return;
      closing = true;
      rejected = new ArrayList<>(waiting);
      waiting.clear();
      for (Task<?> task : rejected) {
        task.state = State.REJECTED;
        retainedBytes -= task.bytes;
      }
    }
    for (Task<?> task : rejected) {
      task.response.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      task.detach();
    }
    completeIfDrained();
  }

  private void scheduleNext() {
    while (true) {
      Task<?> next;
      synchronized (lock) {
        if (active != null || waiting.isEmpty() || closing) return;
        next = waiting.peekFirst();
      }
      boolean[] dispatched = {false};
      // Keep the same scope-then-queue lock order as admission. Deadline and cancellation must
      // be checked at the ownership handoff, not only when the request first entered the queue.
      boolean open = next.requestScope.startIfOpen(() -> {
        synchronized (lock) {
          if (active == null && !closing && waiting.peekFirst() == next
              && next.state == State.WAITING) {
            waiting.removeFirst();
            active = next;
            next.state = State.ACTIVE;
            dispatched[0] = true;
          }
        }
      });
      if (!open) {
        next.close();
        next.detach();
        continue;
      }
      if (!dispatched[0]) continue;
      try {
        worker.execute(next::start);
      } catch (RuntimeException | Error failure) {
        next.rejectBeforeStart(failure);
        if (failure instanceof Error fatal) throw fatal;
      }
      return;
    }
  }

  private void finish(Task<?> task) {
    synchronized (lock) {
      if (active != task || task.state != State.ACTIVE) return;
      task.state = State.FINISHED;
      active = null;
      retainedBytes -= task.bytes;
    }
    task.detach();
    scheduleNext();
    completeIfDrained();
  }

  private void completeIfDrained() {
    boolean complete;
    synchronized (lock) { complete = closing && active == null && waiting.isEmpty(); }
    if (complete) {
      worker.shutdown();
      deadlines.shutdown();
      drained.complete(null);
    }
  }

  private enum State { NEW, WAITING, ACTIVE, FINISHED, CANCELLED, REJECTED }

  private final class Task<T> implements AutoCloseable {
    private final long bytes;
    private final RequestScope requestScope;
    private final Function<RequestScope, CompletableFuture<T>> work;
    private final CompletableFuture<T> response = new CompletableFuture<>();
    private RequestScope.ScopedCleanup registration;
    private State state = State.NEW;
    private BackendFailure.Category rejection;
    private boolean started;
    private final AtomicBoolean settled = new AtomicBoolean();

    private Task(long bytes, RequestScope requestScope, Function<RequestScope, CompletableFuture<T>> work) {
      this.bytes = bytes;
      this.requestScope = requestScope;
      this.work = work;
    }

    private void start() {
      synchronized (lock) {
        if (state != State.ACTIVE || started) return;
        started = true;
      }
      RequestScope serviceScope;
      try {
        serviceScope = RequestScope.withTimeout(serviceDeadline);
      } catch (RuntimeException | Error failure) {
        finish(this);
        response.completeExceptionally(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      java.util.concurrent.ScheduledFuture<?> timeout;
      try {
        timeout = deadlines.schedule(serviceScope::cancel, serviceDeadline.toNanos(), TimeUnit.NANOSECONDS);
      } catch (RuntimeException | Error failure) {
        settle(serviceScope, null, failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      CompletableFuture<T> processing;
      try {
        processing = Objects.requireNonNull(work.apply(serviceScope), "mutation work future");
      } catch (RuntimeException failure) {
        timeout.cancel(false);
        settle(serviceScope, null, failure);
        return;
      } catch (Error fatal) {
        timeout.cancel(false);
        settle(serviceScope, null, fatal);
        throw fatal;
      }
      try {
        processing.whenComplete((value, failure) -> {
          timeout.cancel(false);
          settle(serviceScope, value, failure);
        });
      } catch (RuntimeException | Error failure) {
        timeout.cancel(false);
        settle(serviceScope, null, failure);
        if (failure instanceof Error fatal) throw fatal;
      }
    }

    private void settle(RequestScope serviceScope, T value, Throwable failure) {
      if (!settled.compareAndSet(false, true)) return;
      Throwable primary = failure == null ? null : CompletionFailures.unwrapCompletion(failure);
      if (primary == null && (serviceScope.isCancelled() || serviceScope.isDeadlineExceeded())) {
        primary = new BackendFailure(serviceScope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
      }
      try {
        serviceScope.close();
      } catch (RuntimeException | Error cleanup) {
        if (primary == null) primary = cleanup;
        else if (primary != cleanup) primary.addSuppressed(cleanup);
      }
      Throwable completionFailure = primary;
      CompletableFuture.allOf(serviceScope.cleanupCompletion().toCompletableFuture(),
          serviceScope.physicalProducersCompletion().toCompletableFuture()).whenComplete((ignored, drainFailure) -> {
            Throwable finalFailure = completionFailure;
            if (drainFailure != null) {
              Throwable cause = CompletionFailures.unwrapCompletion(drainFailure);
              if (finalFailure == null) finalFailure = cause;
              else if (finalFailure != cause) finalFailure.addSuppressed(cause);
            }
            if (serviceScope.cleanupFailureCount() != 0 && finalFailure == null) {
              finalFailure = new IllegalStateException("mutation service cleanup failed");
            }
            finish(this);
            if (finalFailure == null) response.complete(value);
            else response.completeExceptionally(finalFailure);
          });
    }

    private void rejectBeforeStart(Throwable failure) {
      synchronized (lock) {
        if (state != State.ACTIVE || started) return;
        state = State.FINISHED;
        active = null;
        retainedBytes -= bytes;
      }
      detach();
      scheduleNext();
      completeIfDrained();
      response.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
    }

    private void detach() {
      try {
        registration.close();
      } catch (Exception failure) {
        response.completeExceptionally(new IllegalStateException("mutation request registration did not close", failure));
      }
    }

    @Override public void close() {
      boolean cancel = false;
      synchronized (lock) {
        if (state == State.NEW) {
          state = State.CANCELLED;
          cancel = true;
        } else if (state == State.WAITING && waiting.remove(this)) {
          state = State.CANCELLED;
          retainedBytes -= bytes;
          cancel = true;
        } else if (state == State.ACTIVE) {
          cancel = true;
        }
      }
      if (cancel) response.completeExceptionally(new BackendFailure(requestScope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      completeIfDrained();
    }
  }
}
