package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class EventHistoryWorkerTest {
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 1);

  @Test
  void rejectedSchedulingStopsAndReportsTheFailure() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    timer.shutdown();
    var lane = new SerialMutationQueue(0, 1024, 1, 1024, Duration.ofSeconds(5));
    var metrics = new RequestMetrics();
    var worker = new EventHistoryWorker(timer, () -> Optional.of(OWNER),
        (owner, scope) -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false)), metrics);
    try {
      worker.start();
      assertThrows(ExecutionException.class, () -> worker.completion().get(5, TimeUnit.SECONDS));
      assertThrows(ExecutionException.class, () -> worker.drained().get(5, TimeUnit.SECONDS));
      assertEquals(EventHistoryWorker.State.STOPPED, worker.state());
      assertEquals(Optional.of(BackendFailure.Category.UNAVAILABLE), worker.lastFailure());
      assertEquals(new RequestMetrics.EventHistoryFailureSnapshot(1, 0), metrics.eventHistoryFailures());
    } finally {
      worker.stop();
      lane.close();
      lane.drained().get(5, TimeUnit.SECONDS);
      assertTrue(lane.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void retriesResourceLimitAndOnlyCompletesAfterSuccessfulProgress() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    var lane = new SerialMutationQueue(0, 1024, 1, 1024, Duration.ofSeconds(5));
    var calls = new AtomicInteger();
    var metrics = new RequestMetrics();
    var observedFailure = new AtomicReference<Optional<BackendFailure.Category>>();
    var workerReference = new AtomicReference<EventHistoryWorker>();
    var worker = new EventHistoryWorker(timer, () -> Optional.of(OWNER), (owner, scope) -> {
      int attempt = calls.incrementAndGet();
      if (attempt == 1) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (attempt == 2) observedFailure.set(workerReference.get().lastFailure());
      return CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, attempt >= 3));
    }, metrics);
    workerReference.set(worker);
    try {
      worker.start();
      worker.completion().get(10, TimeUnit.SECONDS);
      assertEquals(3, calls.get());
      assertEquals(Optional.of(BackendFailure.Category.RESOURCE_LIMIT), observedFailure.get());
      assertEquals(EventHistoryWorker.State.COMPLETE, worker.state());
      assertTrue(worker.lastFailure().isEmpty());
      assertEquals(new RequestMetrics.EventHistoryFailureSnapshot(1, 1), metrics.eventHistoryFailures());
    } finally {
      worker.stop();
      worker.drained().get(10, TimeUnit.SECONDS);
      lane.close();
      lane.drained().get(10, TimeUnit.SECONDS);
      timer.shutdownNow();
      assertTrue(timer.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(lane.awaitTermination(Duration.ofSeconds(10)));
    }
  }

  @Test
  void readinessRequiresSuccessfulCycleAndIsRevokedAfterWorkerFailure() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    var attempts = new AtomicInteger();
    var failureStarted = new CountDownLatch(1);
    var worker = new EventHistoryWorker(timer, () -> Optional.of(OWNER), (owner, scope) -> {
      if (attempts.incrementAndGet() == 1) {
        return CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(false, false));
      }
      failureStarted.countDown();
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }, new RequestMetrics());
    try {
      assertFalse(worker.isReady());
      worker.start();
      worker.initialReadiness().get(5, TimeUnit.SECONDS);
      assertTrue(worker.isReady(), "the first successful event-history cycle establishes readiness");

      assertTrue(failureStarted.await(5, TimeUnit.SECONDS));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (worker.state() != EventHistoryWorker.State.RETRYING && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(1);
      }
      assertEquals(EventHistoryWorker.State.RETRYING, worker.state());
      assertFalse(worker.isReady(), "a failed background worker must revoke readiness while it retries");
    } finally {
      worker.stop();
      worker.drained().get(10, TimeUnit.SECONDS);
      timer.shutdownNow();
      assertTrue(timer.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void stopCancelsActiveCycleAndWaitsForLaneCleanup() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    var lane = new SerialMutationQueue(0, 1024, 1, 1024, Duration.ofSeconds(5));
    var started = new CountDownLatch(1);
    var physical = new CompletableFuture<Void>();
    var worker = new EventHistoryWorker(timer, () -> Optional.of(OWNER), (owner, scope) -> {
      var operation = new CompletableFuture<EventHistoryWorker.CycleResult>();
      scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
      scope.registerCleanup(() -> operation.completeExceptionally(
          new BackendFailure(BackendFailure.Category.CANCELLED)));
      started.countDown();
      return operation;
    }, new RequestMetrics());
    try {
      worker.start();
      assertTrue(started.await(5, TimeUnit.SECONDS));
      worker.stop();
      assertFalse(worker.drained().isDone());
      physical.complete(null);
      worker.drained().get(10, TimeUnit.SECONDS);
      assertEquals(EventHistoryWorker.State.STOPPED, worker.state());
      assertFalse(lane.snapshot().active());
    } finally {
      physical.complete(null);
      worker.stop();
      lane.close();
      lane.drained().get(10, TimeUnit.SECONDS);
      timer.shutdownNow();
      assertTrue(timer.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(lane.awaitTermination(Duration.ofSeconds(10)));
    }
  }

  @Test
  void slowBackfillReadDoesNotBlockHeartbeatAndPublicationUsesOwnerLane() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    var lane = new SerialMutationQueue(1, 1024, 1, 1024, Duration.ofSeconds(5));
    var readStarted = new CountDownLatch(1);
    var releaseRead = new CountDownLatch(1);
    var producerDrained = new CompletableFuture<Void>();
    var publicationUsedOwnerLane = new AtomicReference<Boolean>();
    var worker = new EventHistoryWorker(timer, () -> Optional.of(OWNER), (owner, scope) -> {
      scope.startIfOpen(() -> scope.trackPhysicalProducer(producerDrained));
      readStarted.countDown();
      try {
        assertTrue(releaseRead.await(5, TimeUnit.SECONDS), "test read should be released");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(interrupted);
      }
      var publicationScope = RequestScope.withoutDeadline();
      return lane.submitBackground(publicationScope, laneScope -> {
        publicationUsedOwnerLane.set(lane.snapshot().active());
        return CompletableFuture.completedFuture(null);
      }).whenComplete((ignored, failure) -> {
        publicationScope.close();
        producerDrained.complete(null);
      }).thenApply(ignored -> new EventHistoryWorker.CycleResult(true, true));
    }, new RequestMetrics());
    try {
      worker.start();
      assertTrue(readStarted.await(5, TimeUnit.SECONDS));

      var heartbeatScope = RequestScope.withTimeout(Duration.ofSeconds(2));
      var heartbeat = lane.submit(1, heartbeatScope,
          ignored -> CompletableFuture.completedFuture(Boolean.TRUE));
      assertTrue(heartbeat.get(1, TimeUnit.SECONDS),
          "a slow event backfill read must leave the serial owner lane available");
      heartbeatScope.close();

      releaseRead.countDown();
      worker.completion().get(5, TimeUnit.SECONDS);
      assertEquals(Boolean.TRUE, publicationUsedOwnerLane.get(),
          "the backfill publication must still be dispatched through the owner lane");
    } finally {
      releaseRead.countDown();
      producerDrained.complete(null);
      worker.stop();
      worker.drained().get(5, TimeUnit.SECONDS);
      lane.close();
      lane.drained().get(5, TimeUnit.SECONDS);
      timer.shutdownNow();
      assertTrue(timer.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(lane.awaitTermination(Duration.ofSeconds(5)));
    }
  }
}
