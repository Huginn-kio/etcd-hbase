package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.error.BackendFailure;
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

class EventBackfillWorkerTest {
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 1);

  @Test
  void rejectedSchedulingStopsAndReportsTheFailure() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    timer.shutdown();
    var lane = new SerialMutationQueue(0, 1024, 1, 1024, Duration.ofSeconds(5));
    var metrics = new RequestMetrics();
    var worker = new EventBackfillWorker(lane, timer, () -> Optional.of(OWNER),
        (owner, scope) -> CompletableFuture.completedFuture(true), metrics);
    try {
      worker.start();
      assertThrows(ExecutionException.class, () -> worker.completion().get(5, TimeUnit.SECONDS));
      assertThrows(ExecutionException.class, () -> worker.drained().get(5, TimeUnit.SECONDS));
      assertEquals(EventBackfillWorker.State.STOPPED, worker.state());
      assertEquals(Optional.of(BackendFailure.Category.UNAVAILABLE), worker.lastFailure());
      assertEquals(new RequestMetrics.EventBackfillSnapshot(0, 0, 0, 1), metrics.eventBackfill());
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
    var workerReference = new AtomicReference<EventBackfillWorker>();
    var worker = new EventBackfillWorker(lane, timer, () -> Optional.of(OWNER), (owner, scope) -> {
      int attempt = calls.incrementAndGet();
      if (attempt == 1) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (attempt == 2) observedFailure.set(workerReference.get().lastFailure());
      return CompletableFuture.completedFuture(attempt >= 3);
    }, metrics);
    workerReference.set(worker);
    try {
      worker.start();
      worker.completion().get(10, TimeUnit.SECONDS);
      assertEquals(3, calls.get());
      assertEquals(Optional.of(BackendFailure.Category.RESOURCE_LIMIT), observedFailure.get());
      assertEquals(EventBackfillWorker.State.COMPLETE, worker.state());
      assertTrue(worker.lastFailure().isEmpty());
      assertEquals(new RequestMetrics.EventBackfillSnapshot(2, 1, 1, 0), metrics.eventBackfill());
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
  void stopCancelsActiveCycleAndWaitsForLaneCleanup() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    var lane = new SerialMutationQueue(0, 1024, 1, 1024, Duration.ofSeconds(5));
    var started = new CountDownLatch(1);
    var physical = new CompletableFuture<Void>();
    var worker = new EventBackfillWorker(lane, timer, () -> Optional.of(OWNER), (owner, scope) -> {
      var operation = new CompletableFuture<Boolean>();
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
      assertEquals(EventBackfillWorker.State.STOPPED, worker.state());
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
}
