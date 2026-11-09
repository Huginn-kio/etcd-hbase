package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Recovers a group before enabling its owner lane and fences it on uncertain renewal. */
final class M3OwnerLease implements AutoCloseable {
  private static final Executor CLEANUP = task ->
      Thread.ofVirtual().name("m3-owner-cleanup").start(task);

  private final Object lock = new Object();
  private final CommitOwnerService owners;
  private final CommitOwnerService.OwnerToken token;
  private final long leaseMillis;
  private final long heartbeatTimeoutMillis;
  private final Clock clock;
  private final Runnable onLost;
  private final ScheduledThreadPoolExecutor timer;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean ready = true;
  private boolean stopping;
  private boolean stopCallbackFinished;
  private RequestScope inFlight;
  private Throwable stopFailure;
  private long verifiedDeadlineMillis;

  private M3OwnerLease(CommitOwnerService owners, CommitOwnerService.OwnerToken token,
      long leaseMillis, long heartbeatMillis, long heartbeatTimeoutMillis, long verifiedDeadlineMillis,
      Runnable onLost, Clock clock) {
    this.owners = owners;
    this.token = token;
    this.leaseMillis = leaseMillis;
    this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    this.verifiedDeadlineMillis = verifiedDeadlineMillis;
    this.clock = clock;
    this.onLost = onLost;
    timer = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("m3-owner-heartbeat-", 0).factory());
    timer.setRemoveOnCancelPolicy(true);
    timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    try {
      timer.scheduleWithFixedDelay(this::heartbeat, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException | Error failure) {
      timer.shutdownNow();
      throw failure;
    }
  }

  static CompletableFuture<M3OwnerLease> acquire(RecoveryManager recovery, CommitOwnerService owners,
      String instance, Duration lease, Duration heartbeat, Duration heartbeatTimeout,
      RequestScope startup, Runnable onLost) {
    return acquire(recovery, owners, instance, lease, heartbeat, heartbeatTimeout, startup, onLost,
        Clock.systemUTC());
  }

  static CompletableFuture<M3OwnerLease> acquire(RecoveryManager recovery, CommitOwnerService owners,
      String instance, Duration lease, Duration heartbeat, Duration heartbeatTimeout,
      RequestScope startup, Runnable onLost, Clock clock) {
    Objects.requireNonNull(recovery, "recovery");
    Objects.requireNonNull(owners, "owners");
    Objects.requireNonNull(instance, "instance");
    Objects.requireNonNull(startup, "startup");
    Objects.requireNonNull(onLost, "onLost");
    Objects.requireNonNull(clock, "clock");
    long leaseMillis = millis(lease);
    long heartbeatMillis = millis(heartbeat);
    long timeoutMillis = millis(heartbeatTimeout);
    if (heartbeatMillis > leaseMillis / 3 || timeoutMillis >= leaseMillis) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid owner renewal schedule"));
    }
    long now = clock.millis();
    long deadline = Math.addExact(now, leaseMillis);
    return recovery.recover(instance, now, deadline, startup).thenCompose(result -> {
      if (result.state() == RecoveryManager.State.OWNER_ACTIVE) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (clock.millis() >= deadline) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      var token = result.owner().orElseThrow();
      long verifiedDeadline = Math.addExact(clock.millis(), leaseMillis);
      return owners.heartbeat(token, verifiedDeadline, startup).thenApply(verified -> {
        if (!Boolean.TRUE.equals(verified) || clock.millis() >= deadline || clock.millis() >= verifiedDeadline
            || startup.isClosed() || startup.isDeadlineExceeded()) {
          throw new CompletionException(new BackendFailure(startup.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : startup.isClosed()
                  ? BackendFailure.Category.CANCELLED : BackendFailure.Category.UNAVAILABLE));
        }
        return new M3OwnerLease(owners, token, leaseMillis, heartbeatMillis, timeoutMillis,
            verifiedDeadline, onLost, clock);
      });
    });
  }

  CommitOwnerService.OwnerToken token() { return token; }
  boolean isReady() { synchronized (lock) { return ready && clock.millis() < verifiedDeadlineMillis; } }
  CompletableFuture<Void> drained() { return drained.copy(); }

  boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("negative termination timeout");
    long nanos;
    try { nanos = timeout.toNanos(); } catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
    return timer.awaitTermination(nanos, TimeUnit.NANOSECONDS);
  }

  @Override public void close() { stop(false); }

  private void heartbeat() {
    RequestScope scope;
    long previousDeadline;
    boolean expired;
    synchronized (lock) {
      if (stopping || inFlight != null) return;
      previousDeadline = verifiedDeadlineMillis;
      expired = clock.millis() >= previousDeadline;
      scope = expired ? null : RequestScope.withTimeout(Duration.ofMillis(heartbeatTimeoutMillis));
      if (!expired) inFlight = scope;
    }
    if (expired) { stop(true); return; }
    ScheduledFuture<?> timeout;
    try {
      timeout = timer.schedule(() -> {
        stop(true);
        scope.cancelAsync(CLEANUP);
      }, heartbeatTimeoutMillis, TimeUnit.MILLISECONDS);
      long deadline = Math.addExact(clock.millis(), leaseMillis);
      owners.heartbeat(token, deadline, scope).whenComplete((renewed, failure) -> {
        timeout.cancel(false);
        if (failure != null || !Boolean.TRUE.equals(renewed)
            || clock.millis() >= previousDeadline || clock.millis() >= deadline) {
          stop(true);
        } else {
          synchronized (lock) {
            if (!stopping) verifiedDeadlineMillis = Math.max(verifiedDeadlineMillis, deadline);
          }
        }
        finishHeartbeat(scope);
      });
    } catch (RuntimeException failure) {
      stop(true);
      finishHeartbeat(scope);
    } catch (Error fatal) {
      stop(true);
      finishHeartbeat(scope);
      throw fatal;
    }
  }

  private void finishHeartbeat(RequestScope scope) {
    try {
      scope.closeAsync(CLEANUP);
    } catch (RuntimeException | Error failure) {
      recordFailure(failure);
      stop(true);
    } finally {
      CompletableFuture.allOf(scope.cleanupCompletion().toCompletableFuture(),
          scope.physicalProducersCompletion().toCompletableFuture()).whenComplete((ignored, failure) -> {
            if (failure != null) {
              recordFailure(failure);
              stop(true);
            }
            DrainStatus status;
            synchronized (lock) {
              if (inFlight == scope) inFlight = null;
              status = drainStatus();
            }
            completeDrain(status);
          });
    }
  }

  private void stop(boolean lost) {
    RequestScope cancel;
    synchronized (lock) {
      if (stopping) return;
      stopping = true;
      ready = false;
      cancel = inFlight;
    }
    timer.shutdown();
    try {
      if (lost) onLost.run();
    } catch (RuntimeException | Error failure) {
      recordFailure(failure);
    } finally {
      try {
        if (cancel != null) cancel.cancelAsync(CLEANUP);
      } catch (RuntimeException | Error failure) {
        recordFailure(failure);
      } finally {
        DrainStatus status;
        synchronized (lock) {
          stopCallbackFinished = true;
          status = drainStatus();
        }
        completeDrain(status);
      }
    }
  }

  private void recordFailure(Throwable failure) {
    synchronized (lock) {
      if (stopFailure == null) stopFailure = failure;
      else if (stopFailure != failure) stopFailure.addSuppressed(failure);
    }
  }

  /** Called under lock; no owned heartbeat may still retain a physical producer. */
  private DrainStatus drainStatus() {
    return stopping && stopCallbackFinished && inFlight == null ? new DrainStatus(stopFailure) : null;
  }

  private void completeDrain(DrainStatus status) {
    if (status == null) return;
    if (status.failure() == null) drained.complete(null);
    else drained.completeExceptionally(status.failure());
  }

  private record DrainStatus(Throwable failure) {}

  private static long millis(Duration value) {
    Objects.requireNonNull(value, "duration");
    try {
      long millis = value.toMillis();
      if (millis < 1 || millis > 600_000 || !Duration.ofMillis(millis).equals(value)) {
        throw new IllegalArgumentException("invalid owner duration");
      }
      return millis;
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("invalid owner duration", overflow);
    }
  }
}
