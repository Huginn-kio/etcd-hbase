package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BoundedPublicationHandoffTest {
  @Test
  void closedRequestCancelsBeforeTransferWithoutStartingServiceWork() {
    var handoff = handoff();
    var requestScope = RequestScope.withoutDeadline();
    var starts = new AtomicInteger();
    requestScope.close();

    CompletableFuture<String> result = handoff.transfer(invocation(), requestScope, serviceScope -> {
      starts.incrementAndGet();
      return CompletableFuture.completedFuture("unexpected");
    });

    CompletionException failure = assertThrows(CompletionException.class, result::join);
    assertEquals(BackendFailure.Category.CANCELLED,
        ((BackendFailure) failure.getCause()).category());
    assertEquals(0, starts.get());
    handoff.close();
    handoff.drained().join();
  }

  @Test
  void expiredButOpenRequestRejectsBeforeStartingServiceWork() {
    var handoff = handoff();
    var requestScope = RequestScope.withTimeoutNanos(0, () -> 0L);
    var starts = new AtomicInteger();
    try {
      assertFalse(requestScope.isClosed());
      assertTrue(requestScope.isDeadlineExceeded());

      CompletableFuture<String> result = handoff.transfer(invocation(), requestScope, serviceScope -> {
        starts.incrementAndGet();
        return CompletableFuture.completedFuture("unexpected");
      });

      CompletionException failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.TIMEOUT,
          ((BackendFailure) failure.getCause()).category());
      assertEquals(0, starts.get());
    } finally {
      requestScope.close();
      handoff.close();
      handoff.drained().join();
    }
  }

  @Test
  void closedHandoffRejectsNewAdmission() {
    var handoff = handoff();
    handoff.close();

    CompletableFuture<String> result = handoff.transfer(invocation(),
        RequestScope.withoutDeadline(), ignored -> CompletableFuture.completedFuture("unexpected"));

    CompletionException failure = assertThrows(CompletionException.class, result::join);
    assertEquals(BackendFailure.Category.UNAVAILABLE,
        ((BackendFailure) failure.getCause()).category());
    handoff.drained().join();
  }

  @Test
  void closeWaitsForAcceptedWorkAndActualServiceCleanup() throws Exception {
    var handoff = handoff();
    var requestScope = RequestScope.withoutDeadline();
    var serviceResult = new CompletableFuture<String>();
    var cleanupCompletion = new CompletableFuture<Void>();
    var started = new CountDownLatch(1);
    var cleanupStarted = new CountDownLatch(1);

    CompletableFuture<String> result = handoff.transfer(invocation(), requestScope, serviceScope -> {
      serviceScope.registerScopedCleanup(cleanupStarted::countDown, cleanupCompletion);
      started.countDown();
      return serviceResult;
    });
    assertTrue(started.await(5, TimeUnit.SECONDS));
    handoff.close();
    requestScope.close();
    assertFalse(handoff.drained().isDone());

    serviceResult.complete("published-observation");
    assertEquals("published-observation", result.get(5, TimeUnit.SECONDS));
    assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
    assertFalse(handoff.drained().isDone(), "capacity remains owned until physical cleanup completes");

    cleanupCompletion.complete(null);
    handoff.drained().get(5, TimeUnit.SECONDS);
    assertTrue(handoff.drained().isDone());
  }

  @Test
  void fatalWorkerSubmissionSettlesCapacityAndDoesNotRunLateTask() throws Exception {
    var submissionFatal = new OutOfMemoryError("synthetic worker creation failure");
    var factoryCalls = new AtomicInteger();
    ThreadFactory fallbackFactory =
        Thread.ofPlatform().daemon().name("commit-publish-test-", 0).factory();
    ThreadFactory threads = command -> {
      return switch (factoryCalls.getAndIncrement()) {
        case 0 -> null;
        case 1 -> throw submissionFatal;
        default -> fallbackFactory.newThread(command);
      };
    };
    var handoff = new BoundedPublicationHandoff(1, 2, Duration.ofSeconds(30), threads);
    var failedRequestScope = RequestScope.withoutDeadline();
    var secondRequestScope = RequestScope.withoutDeadline();
    var thirdRequestScope = RequestScope.withoutDeadline();
    var fourthRequestScope = RequestScope.withoutDeadline();
    var overflowRequestScope = RequestScope.withoutDeadline();
    var serviceResult = new CompletableFuture<String>();
    var validStarts = new CountDownLatch(3);
    var serviceStarts = new AtomicInteger();
    try {
      OutOfMemoryError failure = assertThrows(OutOfMemoryError.class,
          () -> handoff.transfer(invocation(), failedRequestScope, serviceScope -> {
            serviceStarts.incrementAndGet();
            return CompletableFuture.completedFuture("unexpected");
          }));
      assertSame(submissionFatal, failure);
      assertEquals(0, serviceStarts.get());

      CompletableFuture<String> second = handoff.transfer(invocation(), secondRequestScope,
          serviceScope -> {
            serviceStarts.incrementAndGet();
            validStarts.countDown();
            return serviceResult;
          });
      CompletableFuture<String> third = handoff.transfer(invocation(), thirdRequestScope,
          serviceScope -> {
            serviceStarts.incrementAndGet();
            validStarts.countDown();
            return serviceResult;
          });
      CompletableFuture<String> fourth = handoff.transfer(invocation(), fourthRequestScope,
          serviceScope -> {
            serviceStarts.incrementAndGet();
            validStarts.countDown();
            return serviceResult;
          });

      CompletableFuture<String> overflow = handoff.transfer(invocation(), overflowRequestScope,
          serviceScope -> CompletableFuture.completedFuture("unexpected"));
      CompletionException limit = assertThrows(CompletionException.class, overflow::join);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) limit.getCause()).category());
      assertTrue(validStarts.await(5, TimeUnit.SECONDS));

      handoff.close();
      failedRequestScope.close();
      secondRequestScope.close();
      thirdRequestScope.close();
      fourthRequestScope.close();
      overflowRequestScope.close();
      serviceResult.complete("published-observation");
      assertEquals("published-observation", second.get(5, TimeUnit.SECONDS));
      assertEquals("published-observation", third.get(5, TimeUnit.SECONDS));
      assertEquals("published-observation", fourth.get(5, TimeUnit.SECONDS));
      assertEquals(3, serviceStarts.get(), "the rejected task must not run after a worker appears");
      handoff.drained().get(5, TimeUnit.SECONDS);
    } finally {
      serviceResult.complete("published-observation");
      failedRequestScope.close();
      secondRequestScope.close();
      thirdRequestScope.close();
      fourthRequestScope.close();
      overflowRequestScope.close();
      handoff.close();
    }
  }

  @Test
  void cleanupFatalIsReturnedAndDrainWaitsForPhysicalCleanup() throws Exception {
    var handoff = handoff();
    var requestScope = RequestScope.withoutDeadline();
    var actualCleanup = new CompletableFuture<Void>();
    var cleanupStarted = new CountDownLatch(1);
    var cleanupFatal = new AssertionError("synthetic service cleanup failure");
    var serviceScopeReference = new AtomicReference<RequestScope>();
    try {
      CompletableFuture<String> result = handoff.transfer(invocation(), requestScope, serviceScope -> {
        serviceScopeReference.set(serviceScope);
        serviceScope.registerScopedCleanup(() -> {
          cleanupStarted.countDown();
          throw cleanupFatal;
        }, actualCleanup);
        return CompletableFuture.completedFuture("published-observation");
      });
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));

      CompletionException failure = assertThrows(CompletionException.class, result::join);
      assertSame(cleanupFatal, failure.getCause());
      assertFalse(handoff.drained().isDone(), "capacity remains owned until actual cleanup completes");
      assertEquals(0, serviceScopeReference.get().cleanupFailureCount());

      handoff.close();
      requestScope.close();
      actualCleanup.complete(null);
      handoff.drained().get(5, TimeUnit.SECONDS);
      assertEquals(1, serviceScopeReference.get().cleanupFailureCount());
    } finally {
      actualCleanup.complete(null);
      requestScope.close();
      handoff.close();
    }
  }

  @Test
  void fatalSynchronousFailureKeepsItsIdentityAndStillWaitsForCleanup() throws Exception {
    Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
    var uncaught = new CountDownLatch(1);
    var uncaughtFailure = new AtomicReference<Throwable>();
    Thread.setDefaultUncaughtExceptionHandler((thread, failure) -> {
      if (thread.getName().startsWith("commit-publish-")) {
        uncaughtFailure.set(failure);
        uncaught.countDown();
      } else if (previous != null) {
        previous.uncaughtException(thread, failure);
      }
    });

    var handoff = handoff();
    var requestScope = RequestScope.withoutDeadline();
    var cleanupCompletion = new CompletableFuture<Void>();
    var started = new CountDownLatch(1);
    var cleanupStarted = new CountDownLatch(1);
    var fatal = new AssertionError("fatal publication setup failure");
    try {
      CompletableFuture<String> result = handoff.transfer(invocation(), requestScope, serviceScope -> {
        serviceScope.registerScopedCleanup(cleanupStarted::countDown, cleanupCompletion);
        started.countDown();
        throw fatal;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      handoff.close();
      requestScope.close();

      CompletionException failure = assertThrows(CompletionException.class, result::join);
      assertSame(fatal, failure.getCause());
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      assertTrue(uncaught.await(5, TimeUnit.SECONDS));
      assertSame(fatal, uncaughtFailure.get());
      assertFalse(handoff.drained().isDone(), "fatal propagation does not settle owned resources");

      cleanupCompletion.complete(null);
      handoff.drained().get(5, TimeUnit.SECONDS);
    } finally {
      cleanupCompletion.complete(null);
      handoff.close();
      Thread.setDefaultUncaughtExceptionHandler(previous);
    }
  }

  private static BoundedPublicationHandoff handoff() {
    return new BoundedPublicationHandoff(1, 1, Duration.ofSeconds(30));
  }

  private static InvocationBinding invocation() {
    return InvocationBinding.begin(new InvocationId(UUID.randomUUID()), digest(1), digest(2),
        TransactionId.create(), 1, 0, 1);
  }

  private static ByteKey digest(int marker) {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) marker;
    return ByteKey.copyOf(bytes);
  }
}
