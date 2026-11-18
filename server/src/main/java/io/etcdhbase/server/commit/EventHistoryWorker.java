package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Runs one bounded event-history cycle away from the owner lane at a time. */
final class EventHistoryWorker {
  enum State { WAITING, RUNNING, COMPLETE, RETRYING, STOPPED }

  record CycleResult(boolean progressed, boolean finished) {}

  private static final long PROGRESS_DELAY_MILLIS = 20;
  private static final long IDLE_DELAY_MILLIS = 250;
  private static final long RETRY_DELAY_MILLIS = 1000;
  private static final java.util.concurrent.Executor CLEANUP =
      task -> Thread.ofVirtual().name("event-history-cleanup").start(task);
  private final Object lock = new Object();
  private final ThreadPoolExecutor cycles;
  private final ScheduledExecutorService timer;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final BiFunction<Optional<CommitOwnerService.OwnerToken>, RequestScope,
      CompletableFuture<CycleResult>> cycle;
  private final RequestMetrics metrics;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private final CompletableFuture<Void> complete = new CompletableFuture<>();
  private final AtomicBoolean drainStarted = new AtomicBoolean();
  private ScheduledFuture<?> scheduled;
  private RequestScope service;
  private State state = State.WAITING;
  private Optional<BackendFailure.Category> lastFailure = Optional.empty();
  private Throwable drainFailure;
  private boolean started;
  private boolean running;
  private boolean outstanding;

  EventHistoryWorker(ScheduledExecutorService timer,
      Supplier<Optional<CommitOwnerService.OwnerToken>> owner,
      BiFunction<Optional<CommitOwnerService.OwnerToken>, RequestScope,
          CompletableFuture<CycleResult>> cycle,
      RequestMetrics metrics) {
    this.timer = Objects.requireNonNull(timer, "timer");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.cycle = Objects.requireNonNull(cycle, "cycle");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    cycles = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1),
        Thread.ofPlatform().daemon().name("event-history-cycle-", 0).factory(),
        new ThreadPoolExecutor.AbortPolicy());
  }

  void start() {
    synchronized (lock) {
      if (started) throw new IllegalStateException("event history worker already started");
      started = true;
      running = true;
      schedule(0);
    }
  }

  State state() { synchronized (lock) { return state; } }
  Optional<BackendFailure.Category> lastFailure() { synchronized (lock) { return lastFailure; } }
  CompletableFuture<Void> drained() { return drained.copy(); }
  CompletableFuture<Void> completion() { return complete.copy(); }

  void stop() {
    RequestScope cancel;
    boolean idle;
    synchronized (lock) {
      if (!running && started) return;
      started = true;
      running = false;
      state = State.STOPPED;
      if (scheduled != null) scheduled.cancel(false);
      scheduled = null;
      cancel = service;
      idle = !outstanding;
    }
    complete.completeExceptionally(new CancellationException("event history worker stopped"));
    cycles.shutdown();
    if (cancel != null) cancel.cancelAsync(CLEANUP);
    if (idle) drainAfterExecutorStops();
  }

  private void schedule(long delayMillis) {
    if (!running || outstanding || scheduled != null) return;
    try {
      scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException failure) {
      running = false;
      state = State.STOPPED;
      drainFailure = failure;
      lastFailure = Optional.of(BackendFailure.Category.UNAVAILABLE);
      metrics.recordEventHistoryFailure(BackendFailure.Category.UNAVAILABLE);
      complete.completeExceptionally(failure);
      cycles.shutdown();
      drainAfterExecutorStops();
    }
  }

  private void dispatch() {
    RequestScope request;
    synchronized (lock) {
      scheduled = null;
      if (!running || outstanding) return;
      outstanding = true;
      state = State.RUNNING;
      request = RequestScope.withoutDeadline();
      service = request;
    }
    try {
      cycles.execute(() -> runCycle(request));
    } catch (RuntimeException | Error failure) {
      settle(request, null, new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void runCycle(RequestScope scope) {
    synchronized (lock) {
      if (!running) {
        settle(scope, null, new BackendFailure(BackendFailure.Category.CANCELLED));
        return;
      }
    }
    CompletableFuture<CycleResult> result;
    try {
      result = Objects.requireNonNull(cycle.apply(owner.get(), scope), "event history cycle future");
    } catch (RuntimeException | Error failure) {
      settle(scope, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    result.whenComplete((cycleResult, failure) -> settle(scope, cycleResult, failure));
  }

  private void settle(RequestScope request, CycleResult result, Throwable failure) {
    try {
      request.closeAsync(CLEANUP);
    } catch (RuntimeException | Error cleanup) {
      if (failure == null) failure = cleanup;
      else if (failure != cleanup) failure.addSuppressed(cleanup);
    }
    Throwable primaryFailure = failure;
    var cleanupFailure = request.cleanupOutcomeCompletion().handle((ignored, cleanupProblem) ->
        cleanupProblem == null ? null
            : CompletionFailures.unwrapCompletionPreservingSuppressed(cleanupProblem)).toCompletableFuture();
    var producerFailure = request.physicalProducersCompletion().handle((ignored, producerProblem) ->
        producerProblem == null ? null
            : CompletionFailures.unwrapCompletionPreservingSuppressed(producerProblem)).toCompletableFuture();
    CompletableFuture.allOf(cleanupFailure, producerFailure).whenComplete((ignored, outcomeFailure) -> {
      Throwable settledFailure = primaryFailure == null
          ? null : CompletionFailures.unwrapCompletionPreservingSuppressed(primaryFailure);
      Throwable cleanupCause = cleanupFailure.join();
      Throwable producerCause = producerFailure.join();
      settledFailure = appendFailure(settledFailure, cleanupCause);
      settledFailure = appendFailure(settledFailure, producerCause);

      Throwable drainFailure = appendFailure(cleanupCause, producerCause);
      if (request.cleanupFailureCount() != 0 && cleanupCause == null) {
        Throwable unreportedFailure = new IllegalStateException("event history cycle cleanup failed");
        settledFailure = appendFailure(settledFailure, unreportedFailure);
        drainFailure = appendFailure(drainFailure, unreportedFailure);
      }
      if (outcomeFailure != null) {
        Throwable cause = CompletionFailures.unwrapCompletionPreservingSuppressed(outcomeFailure);
        settledFailure = appendFailure(settledFailure, cause);
        drainFailure = appendFailure(drainFailure, cause);
      }
      finish(request, result, settledFailure, drainFailure);
    });
  }

  private void finish(RequestScope request, CycleResult result, Throwable failure, Throwable drainFailure) {
    boolean stopped = false;
    boolean finished = false;
    synchronized (lock) {
      if (service != request || !outstanding) return;
      service = null;
      outstanding = false;
      if (!running) {
        stopped = true;
        this.drainFailure = appendFailure(this.drainFailure, drainFailure);
      } else if (failure != null) {
        Throwable cause = CompletionFailures.unwrapCompletionPreservingSuppressed(failure);
        lastFailure = Optional.of(cause instanceof BackendFailure backend
            ? backend.category() : BackendFailure.Category.UNKNOWN);
        metrics.recordEventHistoryFailure(lastFailure.orElseThrow());
        state = State.RETRYING;
        schedule(RETRY_DELAY_MILLIS);
      } else if (result.finished()) {
        lastFailure = Optional.empty();
        state = State.COMPLETE;
        finished = true;
      } else {
        lastFailure = Optional.empty();
        state = State.WAITING;
        schedule(result.progressed() ? PROGRESS_DELAY_MILLIS : IDLE_DELAY_MILLIS);
      }
    }
    if (stopped) drainAfterExecutorStops();
    if (finished) complete.complete(null);
  }

  private static Throwable appendFailure(Throwable primary, Throwable additional) {
    if (additional == null) return primary;
    if (primary == null) return additional;
    if (primary != additional) primary.addSuppressed(additional);
    return primary;
  }

  private void drainAfterExecutorStops() {
    if (!drainStarted.compareAndSet(false, true)) return;
    CLEANUP.execute(() -> {
      boolean interrupted = false;
      try {
        while (true) {
          try {
            if (cycles.awaitTermination(30, TimeUnit.SECONDS)) break;
          } catch (InterruptedException failure) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
      Throwable failure;
      synchronized (lock) { failure = drainFailure; }
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    });
  }
}
