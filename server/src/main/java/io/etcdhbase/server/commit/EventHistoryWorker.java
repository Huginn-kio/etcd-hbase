package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Schedules one bounded backfill or live-tail step at a time on the owner mutation lane. */
final class EventHistoryWorker {
  enum State { WAITING, RUNNING, COMPLETE, RETRYING, STOPPED }

  record CycleResult(boolean progressed, boolean finished) {}

  private static final long PROGRESS_DELAY_MILLIS = 20;
  private static final long IDLE_DELAY_MILLIS = 250;
  private static final long RETRY_DELAY_MILLIS = 1000;
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("event-history-cleanup").start(task);
  private final Object lock = new Object();
  private final SerialMutationQueue lane;
  private final ScheduledExecutorService timer;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final BiFunction<Optional<CommitOwnerService.OwnerToken>, RequestScope,
      CompletableFuture<CycleResult>> cycle;
  private final RequestMetrics metrics;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private final CompletableFuture<Void> complete = new CompletableFuture<>();
  private ScheduledFuture<?> scheduled;
  private RequestScope waiter;
  private RequestScope service;
  private State state = State.WAITING;
  private Optional<BackendFailure.Category> lastFailure = Optional.empty();
  private boolean started;
  private boolean running;
  private boolean outstanding;

  EventHistoryWorker(SerialMutationQueue lane, ScheduledExecutorService timer,
      Supplier<Optional<CommitOwnerService.OwnerToken>> owner,
      BiFunction<Optional<CommitOwnerService.OwnerToken>, RequestScope,
          CompletableFuture<CycleResult>> cycle,
      RequestMetrics metrics) {
    this.lane = Objects.requireNonNull(lane, "lane");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.cycle = Objects.requireNonNull(cycle, "cycle");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
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
      if (!running) return;
      running = false;
      state = State.STOPPED;
      if (scheduled != null) scheduled.cancel(false);
      scheduled = null;
      cancel = service == null ? waiter : service;
      idle = !outstanding;
    }
    complete.completeExceptionally(new CancellationException("event history worker stopped"));
    if (idle) drained.complete(null);
    if (cancel != null) cancel.cancelAsync(CLEANUP);
  }

  private void schedule(long delayMillis) {
    if (!running || outstanding || scheduled != null) return;
    try {
      scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException failure) {
      running = false;
      state = State.STOPPED;
      lastFailure = Optional.of(BackendFailure.Category.UNAVAILABLE);
      metrics.recordEventHistoryFailure(BackendFailure.Category.UNAVAILABLE);
      complete.completeExceptionally(failure);
      drained.completeExceptionally(failure);
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
      waiter = request;
    }
    try {
      CompletableFuture<CycleResult> result = lane.submitBackground(request, this::runOnLane);
      result.whenComplete((cycleResult, failure) -> finish(request, cycleResult, failure));
    } catch (RuntimeException | Error failure) {
      finish(request, null, failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private CompletableFuture<CycleResult> runOnLane(RequestScope scope) {
    synchronized (lock) {
      if (!running) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      service = scope;
    }
    return Objects.requireNonNull(cycle.apply(owner.get(), scope), "event history cycle future");
  }

  private void finish(RequestScope request, CycleResult result, Throwable failure) {
    request.close();
    boolean stopped = false;
    boolean finished = false;
    synchronized (lock) {
      waiter = null;
      service = null;
      outstanding = false;
      if (!running) {
        stopped = true;
      } else if (failure != null) {
        Throwable cause = CompletionFailures.unwrapCompletion(failure);
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
    if (stopped) drained.complete(null);
    if (finished) complete.complete(null);
  }
}
