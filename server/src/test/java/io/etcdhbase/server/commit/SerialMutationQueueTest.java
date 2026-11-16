package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
      closeAndAssertDrained(queue);
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
      closeAndAssertDrained(queue);
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
      closeAndAssertDrained(queue);
    }
  }

  @Test
  void zeroQueuedCapacityRejectsConcurrentRequestsWithoutDisplacingTheActiveMutation() throws Exception {
    var queue = new SerialMutationQueue(0, 100, Duration.ofSeconds(10));
    var activeScope = RequestScope.withoutDeadline();
    var rejectedScope = RequestScope.withoutDeadline();
    var activeWork = new CompletableFuture<String>();
    var started = new CountDownLatch(1);
    var rejectedWorkStarts = new AtomicInteger();
    try {
      var active = queue.submit(10, activeScope, ignored -> {
        started.countDown();
        return activeWork;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));

      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submit(10, rejectedScope, ignored -> {
            rejectedWorkStarts.incrementAndGet();
            return CompletableFuture.completedFuture("must not run");
          }));
      assertTrue(queue.snapshot().active());
      assertEquals(0, queue.snapshot().waiting());
      assertEquals(10, queue.snapshot().retainedBytes());
      assertEquals(0, rejectedWorkStarts.get());

      activeWork.complete("active");
      assertEquals("active", active.get(5, TimeUnit.SECONDS));
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      activeWork.complete("finish");
      activeScope.close();
      rejectedScope.close();
      closeAndAssertDrained(queue);
    }
  }

  @Test
  void reservedControlSlotAcceptsRevocationBehindAnActiveMutation() throws Exception {
    var queue = new SerialMutationQueue(0, 100, 1, 100, Duration.ofSeconds(10));
    var activeScope = RequestScope.withoutDeadline();
    var reservedScope = RequestScope.withoutDeadline();
    var fullReservedScope = RequestScope.withoutDeadline();
    var regularScope = RequestScope.withoutDeadline();
    var activeWork = new CompletableFuture<String>();
    var activeStarted = new CountDownLatch(1);
    var reservedStarted = new CountDownLatch(1);
    try {
      var active = queue.submit(10, activeScope, ignored -> {
        activeStarted.countDown();
        return activeWork;
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submit(10, regularScope, ignored -> CompletableFuture.completedFuture("rejected")));
      var reserved = queue.submitReserved(10, reservedScope, ignored -> {
        reservedStarted.countDown();
        return CompletableFuture.completedFuture("revoke");
      });
      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submitReserved(10, fullReservedScope,
              ignored -> CompletableFuture.completedFuture("over-reserved")));
      assertEquals(1, queue.snapshot().waiting());
      assertEquals(20, queue.snapshot().retainedBytes());

      activeWork.complete("mutation");
      assertEquals("mutation", active.get(5, TimeUnit.SECONDS));
      assertTrue(reservedStarted.await(5, TimeUnit.SECONDS));
      assertEquals("revoke", reserved.get(5, TimeUnit.SECONDS));
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      activeWork.complete("finish");
      activeScope.close();
      reservedScope.close();
      fullReservedScope.close();
      regularScope.close();
      closeAndAssertDrained(queue);
    }
  }

  @Test
  void controlAndRegularWorkAlternateWhenBothQueuesRemainPopulated() throws Exception {
    var queue = new SerialMutationQueue(2, 100, 2, 100, Duration.ofSeconds(10));
    var activeScope = RequestScope.withoutDeadline();
    var regularScope = RequestScope.withoutDeadline();
    var firstReservedScope = RequestScope.withoutDeadline();
    var secondReservedScope = RequestScope.withoutDeadline();
    var activeWork = new CompletableFuture<String>();
    var firstReservedWork = new CompletableFuture<String>();
    var order = java.util.Collections.synchronizedList(new ArrayList<String>());
    var activeStarted = new CountDownLatch(1);
    var firstReservedStarted = new CountDownLatch(1);
    var secondReservedStarted = new CountDownLatch(1);
    try {
      var active = queue.submit(10, activeScope, ignored -> {
        order.add("active");
        activeStarted.countDown();
        return activeWork;
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      var regular = queue.submit(10, regularScope, ignored -> {
        order.add("regular");
        return CompletableFuture.completedFuture("regular");
      });
      var firstReserved = queue.submitReserved(10, firstReservedScope, ignored -> {
        order.add("reserved-1");
        firstReservedStarted.countDown();
        return firstReservedWork;
      });
      activeWork.complete("active");
      assertEquals("active", active.get(5, TimeUnit.SECONDS));
      assertTrue(firstReservedStarted.await(5, TimeUnit.SECONDS));
      var secondReserved = queue.submitReserved(10, secondReservedScope, ignored -> {
        order.add("reserved-2");
        secondReservedStarted.countDown();
        return CompletableFuture.completedFuture("reserved-2");
      });
      firstReservedWork.complete("reserved-1");
      assertEquals("reserved-1", firstReserved.get(5, TimeUnit.SECONDS));
      assertEquals("regular", regular.get(5, TimeUnit.SECONDS));
      assertTrue(secondReservedStarted.await(5, TimeUnit.SECONDS));
      assertEquals("reserved-2", secondReserved.get(5, TimeUnit.SECONDS));
      assertEquals(List.of("active", "reserved-1", "regular", "reserved-2"), order);
    } finally {
      activeWork.complete("finish");
      firstReservedWork.complete("finish");
      activeScope.close();
      regularScope.close();
      firstReservedScope.close();
      secondReservedScope.close();
      closeAndAssertDrained(queue);
    }
  }

  @Test
  void backgroundWorkHasReservedCapacityAndRoundRobinsWithOtherLanes() throws Exception {
    var queue = new SerialMutationQueue(2, 100, 1, 100, Duration.ofSeconds(10));
    var activeScope = RequestScope.withoutDeadline();
    var regularScope = RequestScope.withoutDeadline();
    var reservedScope = RequestScope.withoutDeadline();
    var backgroundScope = RequestScope.withoutDeadline();
    var activeWork = new CompletableFuture<String>();
    var reservedWork = new CompletableFuture<String>();
    var backgroundWork = new CompletableFuture<String>();
    var order = java.util.Collections.synchronizedList(new ArrayList<String>());
    var activeStarted = new CountDownLatch(1);
    var reservedStarted = new CountDownLatch(1);
    var backgroundStarted = new CountDownLatch(1);
    var regularStarted = new CountDownLatch(1);
    try {
      var active = queue.submit(10, activeScope, ignored -> {
        order.add("active");
        activeStarted.countDown();
        return activeWork;
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      var regular = queue.submit(10, regularScope, ignored -> {
        order.add("regular");
        regularStarted.countDown();
        return CompletableFuture.completedFuture("regular");
      });
      var reserved = queue.submitReserved(10, reservedScope, ignored -> {
        order.add("reserved");
        reservedStarted.countDown();
        return reservedWork;
      });
      var background = queue.submitBackground(backgroundScope, ignored -> {
        order.add("background");
        backgroundStarted.countDown();
        return backgroundWork;
      });

      assertEquals(3, queue.snapshot().waiting());
      assertEquals(31, queue.snapshot().retainedBytes());
      activeWork.complete("active");
      assertEquals("active", active.get(5, TimeUnit.SECONDS));
      assertTrue(reservedStarted.await(5, TimeUnit.SECONDS));
      reservedWork.complete("reserved");
      assertEquals("reserved", reserved.get(5, TimeUnit.SECONDS));
      assertTrue(backgroundStarted.await(5, TimeUnit.SECONDS));
      backgroundWork.complete("background");
      assertEquals("background", background.get(5, TimeUnit.SECONDS));
      assertTrue(regularStarted.await(5, TimeUnit.SECONDS));
      assertEquals("regular", regular.get(5, TimeUnit.SECONDS));
      assertEquals(List.of("active", "reserved", "background", "regular"), order);
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      activeWork.complete("finish");
      reservedWork.complete("finish");
      backgroundWork.complete("finish");
      activeScope.close();
      regularScope.close();
      reservedScope.close();
      backgroundScope.close();
      closeAndAssertDrained(queue);
    }
  }

  @Test
  void backgroundAdmissionCancellationAndCloseReleaseOnlyItsOwnBudget() throws Exception {
    var queue = new SerialMutationQueue(0, 100, 1, 100, Duration.ofSeconds(10));
    var activeScope = RequestScope.withoutDeadline();
    var cancelledScope = RequestScope.withoutDeadline();
    var rejectedScope = RequestScope.withoutDeadline();
    var activeWork = new CompletableFuture<String>();
    var activeStarted = new CountDownLatch(1);
    var backgroundStarts = new AtomicInteger();
    try {
      var active = queue.submit(10, activeScope, ignored -> {
        activeStarted.countDown();
        return activeWork;
      });
      assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
      var cancelled = queue.submitBackground(cancelledScope, ignored -> {
        backgroundStarts.incrementAndGet();
        return CompletableFuture.completedFuture("must not run");
      });
      assertEquals(11, queue.snapshot().retainedBytes());
      assertCategory(BackendFailure.Category.RESOURCE_LIMIT,
          queue.submitBackground(rejectedScope, ignored -> CompletableFuture.completedFuture("over-limit")));

      cancelledScope.cancel();
      assertCategory(BackendFailure.Category.CANCELLED, cancelled);
      assertEquals(10, queue.snapshot().retainedBytes());
      assertEquals(0, queue.snapshot().waiting());
      assertEquals(0, backgroundStarts.get());

      var rejectedOnClose = queue.submitBackground(rejectedScope,
          ignored -> CompletableFuture.completedFuture("must be rejected on close"));
      assertEquals(11, queue.snapshot().retainedBytes());
      queue.close();
      assertCategory(BackendFailure.Category.UNAVAILABLE, rejectedOnClose);
      assertEquals(10, queue.snapshot().retainedBytes());
      assertFalse(queue.drained().isDone());

      activeWork.complete("active");
      assertEquals("active", active.get(5, TimeUnit.SECONDS));
      queue.drained().get(5, TimeUnit.SECONDS);
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      activeWork.complete("finish");
      activeScope.close();
      cancelledScope.close();
      rejectedScope.close();
      closeAndAssertDrained(queue);
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
      closeAndAssertDrained(queue);
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
      closeAndAssertDrained(queue);
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
      closeAndAssertDrained(queue);
      assertEquals(0, queue.snapshot().retainedBytes());
    } finally {
      request.close();
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
    }
  }

  private static void closeAndAssertDrained(SerialMutationQueue queue) throws Exception {
    queue.close();
    queue.drained().get(5, TimeUnit.SECONDS);
    assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
  }

  private static void assertCategory(BackendFailure.Category category, CompletableFuture<?> result) {
    var failure = assertThrows(CompletionException.class, () -> result.orTimeout(5, TimeUnit.SECONDS).join());
    assertEquals(category, ((BackendFailure) failure.getCause()).category());
  }
}
