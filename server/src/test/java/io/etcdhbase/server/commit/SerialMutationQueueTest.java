package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SerialMutationQueueTest {
  @Test
  void elapsedQueuedDeadlinePreventsDispatchBeforeCancellationCallback() throws Exception {
    var queue = new SerialMutationQueue(1, 100, Duration.ofSeconds(10));
    var firstScope = RequestScope.withoutDeadline();
    var clock = new AtomicLong();
    var waitingScope = RequestScope.withTimeoutNanos(100, clock::get);
    var firstWork = new CompletableFuture<String>();
    var firstStarted = new CountDownLatch(1);
    var waitingStarts = new AtomicInteger();
    try {
      var first = queue.submit(10, firstScope, ignored -> {
        firstStarted.countDown();
        return firstWork;
      });
      assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
      var waiting = queue.submit(10, waitingScope, ignored -> {
        waitingStarts.incrementAndGet();
        return CompletableFuture.completedFuture("unexpected");
      });
      assertEquals(1, queue.snapshot().waiting());
      clock.set(100);
      firstWork.complete("first");
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      assertCategory(BackendFailure.Category.TIMEOUT, waiting);
      assertEquals(0, waitingStarts.get());
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      firstWork.complete("finish");
      firstScope.close();
      waitingScope.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void queuedCancellationRemovesOnlyTheWaitingRequest() throws Exception {
    var queue = new SerialMutationQueue(1, 100, Duration.ofSeconds(10));
    var firstScope = RequestScope.withoutDeadline();
    var waitingScope = RequestScope.withoutDeadline();
    var nextScope = RequestScope.withoutDeadline();
    var firstWork = new CompletableFuture<String>();
    var firstStarted = new CountDownLatch(1);
    var nextStarted = new CountDownLatch(1);
    try {
      var first = queue.submit(10, firstScope, ignored -> {
        firstStarted.countDown();
        return firstWork;
      });
      assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
      var waiting = queue.submit(20, waitingScope,
          ignored -> CompletableFuture.failedFuture(new AssertionError("cancelled work ran")));
      assertEquals(1, queue.snapshot().waiting());
      waitingScope.cancel();
      assertCategory(BackendFailure.Category.CANCELLED, waiting);
      assertEquals(0, queue.snapshot().waiting());
      assertEquals(10, queue.snapshot().retainedBytes());
      var next = queue.submit(30, nextScope, ignored -> {
        nextStarted.countDown();
        return CompletableFuture.completedFuture("next");
      });
      firstWork.complete("first");
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      assertTrue(nextStarted.await(5, TimeUnit.SECONDS));
      assertEquals("next", next.get(5, TimeUnit.SECONDS));
    } finally {
      firstWork.complete("finish");
      firstScope.close();
      waitingScope.close();
      nextScope.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void rejectsCountAndByteOvercommitBeforeDispatch() throws Exception {
    var queue = new SerialMutationQueue(1, 10, Duration.ofSeconds(10));
    var firstScope = RequestScope.withoutDeadline();
    var secondScope = RequestScope.withoutDeadline();
    var rejectedScope = RequestScope.withoutDeadline();
    var firstWork = new CompletableFuture<String>();
    var started = new CountDownLatch(1);
    try {
      var first = queue.submit(6, firstScope, ignored -> {
        started.countDown();
        return firstWork;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submit(5, rejectedScope, ignored -> CompletableFuture.completedFuture("bad")));
      var second = queue.submit(4, secondScope, ignored -> CompletableFuture.completedFuture("second"));
      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submit(1, rejectedScope, ignored -> CompletableFuture.completedFuture("bad")));
      assertEquals(10, queue.snapshot().retainedBytes());
      firstWork.complete("first");
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      assertEquals("second", second.get(5, TimeUnit.SECONDS));
    } finally {
      firstWork.complete("finish");
      firstScope.close();
      secondScope.close();
      rejectedScope.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void dispatchedCancellationDoesNotReleaseLaneUntilProducerDrains() throws Exception {
    var queue = new SerialMutationQueue(1, 100, Duration.ofSeconds(10));
    var firstScope = RequestScope.withoutDeadline();
    var secondScope = RequestScope.withoutDeadline();
    var processing = new CompletableFuture<String>();
    var physical = new CompletableFuture<Void>();
    var started = new CountDownLatch(1);
    var cleanupStarted = new CountDownLatch(1);
    var nextStarted = new CountDownLatch(1);
    try {
      var first = queue.submit(10, firstScope, service -> {
        service.trackPhysicalProducer(physical);
        service.registerScopedCleanup(cleanupStarted::countDown, physical);
        started.countDown();
        return processing;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      var second = queue.submit(10, secondScope, ignored -> {
        nextStarted.countDown();
        return CompletableFuture.completedFuture("second");
      });
      firstScope.cancel();
      assertCategory(BackendFailure.Category.CANCELLED, first);
      processing.complete("may-have-committed");
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      assertFalse(nextStarted.await(100, TimeUnit.MILLISECONDS));
      assertTrue(queue.snapshot().active());
      physical.complete(null);
      assertTrue(nextStarted.await(5, TimeUnit.SECONDS));
      assertEquals("second", second.get(5, TimeUnit.SECONDS));
    } finally {
      processing.complete("finish");
      physical.complete(null);
      firstScope.close();
      secondScope.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void closeRejectsWaitingWorkAndDrainsTheAlreadyDispatchedTask() throws Exception {
    var queue = new SerialMutationQueue(1, 100, Duration.ofSeconds(10));
    var firstScope = RequestScope.withoutDeadline();
    var secondScope = RequestScope.withoutDeadline();
    var work = new CompletableFuture<String>();
    var started = new CountDownLatch(1);
    try {
      var first = queue.submit(10, firstScope, ignored -> {
        started.countDown();
        return work;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      var second = queue.submit(10, secondScope, ignored -> CompletableFuture.completedFuture("bad"));
      queue.close();
      assertCategory(BackendFailure.Category.UNAVAILABLE, second);
      assertFalse(queue.drained().isDone());
      work.complete("first");
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      queue.drained().get(5, TimeUnit.SECONDS);
      assertCategory(BackendFailure.Category.UNAVAILABLE,
          queue.submit(1, secondScope, ignored -> CompletableFuture.completedFuture("bad")));
    } finally {
      work.complete("finish");
      firstScope.close();
      secondScope.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  @Test
  void serviceDeadlineCancelsDispatchedWorkAndDrainsIt() throws Exception {
    var queue = new SerialMutationQueue(0, 100, Duration.ofMillis(200));
    var request = RequestScope.withoutDeadline();
    try {
      var result = queue.submit(10, request, service -> {
        var processing = new CompletableFuture<String>();
        service.registerCleanup(() -> processing.complete("late"));
        return processing;
      });
      assertCategory(BackendFailure.Category.TIMEOUT, result);
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      request.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
    }
  }

  private static void assertCategory(BackendFailure.Category category, CompletableFuture<?> result) {
    var failure = assertThrows(CompletionException.class, () -> result.orTimeout(5, TimeUnit.SECONDS).join());
    assertEquals(category, ((BackendFailure) failure.getCause()).category());
  }
}
