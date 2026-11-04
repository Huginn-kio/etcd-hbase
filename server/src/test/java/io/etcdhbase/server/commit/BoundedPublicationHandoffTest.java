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
