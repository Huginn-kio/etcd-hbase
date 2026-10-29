package io.etcdhbase.core.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RequestScopeTest {
  @Test
  void completionWaitsForAsyncAndLateCleanupWithoutBlockingReentrantClose() throws Exception {
    var scope = RequestScope.withoutDeadline();
    var pending = new java.util.concurrent.atomic.AtomicReference<Runnable>();
    scope.registerCleanup(scope::close);
    assertTrue(scope.closeAsync(pending::set));
    assertTrue(scope.isClosed());
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
    scope.close();
    pending.get().run();
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
    var fatal = new AssertionError("private-cleanup-error");
    assertSame(
        fatal,
        assertThrows(
            AssertionError.class,
            () ->
                scope.registerCleanup(
                    () -> {
                      throw fatal;
                    })));
    assertEquals(1, scope.cleanupFailureCount());
  }

  @Test
  void rejectedCleanupExecutorRetainsTheCallersCleanupResponsibility() {
    var scope = RequestScope.withoutDeadline();
    var closes = new AtomicLong();
    scope.registerCleanup(closes::incrementAndGet);
    assertThrows(
        java.util.concurrent.RejectedExecutionException.class,
        () ->
            scope.closeAsync(
                task -> {
                  throw new java.util.concurrent.RejectedExecutionException();
                }));
    assertEquals(1, closes.get());
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
  }

  @Test
  void cancellationWinsBeforeLateRegistrationAndAllCleanupStillRuns() throws Exception {
    RequestScope scope = RequestScope.withoutDeadline();
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    scope.registerCleanup(
        () -> {
          entered.countDown();
          release.await();
        });
    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      var cancellation = executor.submit(scope::cancel);
      try {
        assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
        AtomicLong closes = new AtomicLong();
        AutoCloseable late = closes::incrementAndGet;
        scope.registerCleanup(late);
        scope.registerCleanup(late);
        assertEquals(1, closes.get());
        assertTrue(scope.isCancelled());
        assertTrue(scope.isClosed());
        release.countDown();
        assertTrue(cancellation.get(2, java.util.concurrent.TimeUnit.SECONDS));
      } finally {
        release.countDown();
      }
    } finally {
      release.countDown();
    }
  }

  @Test
  void closesRemainingResourcesAfterFailuresIncludingFatalCleanup() {
    RequestScope scope = RequestScope.withoutDeadline();
    var fatal = new AssertionError("sensitive-error");
    AtomicLong closes = new AtomicLong();
    scope.registerCleanup(closes::incrementAndGet);
    scope.registerCleanup(
        () -> {
          throw fatal;
        });
    scope.registerCleanup(
        () -> {
          throw new IOException("sensitive-exception");
        });
    assertSame(fatal, assertThrows(AssertionError.class, scope::close));
    assertEquals(1, closes.get());
    assertEquals(2, scope.cleanupFailureCount());
    scope.close();
  }

  @Test
  void tracksOwnedFuturesButCancelsOnlyTheDependentOfSharedWork() {
    RequestScope scope = RequestScope.withoutDeadline();
    CompletableFuture<String> shared = new CompletableFuture<>();
    var dependent = scope.track(shared.copy());
    scope.cancel();
    assertTrue(dependent.isCancelled());
    assertFalse(shared.isCancelled());
    shared.complete("still-shared");
    assertEquals("still-shared", shared.join());
    assertTrue(scope.track(new CompletableFuture<>()).isCancelled());
  }

  @Test
  void monotonicDeadlineSurvivesNanoTimeWrapAndLongTimeoutsStayBounded() {
    AtomicLong now = new AtomicLong(Long.MAX_VALUE - 5);
    RequestScope scope = new RequestScope(10, now::get);
    now.addAndGet(7);
    assertEquals(3, scope.remainingDeadlineNanos());
    now.addAndGet(4);
    assertTrue(scope.isDeadlineExceeded());
    assertTrue(RequestScope.withTimeout(Duration.ofSeconds(Long.MAX_VALUE)).hasDeadline());
  }

  @Test
  void closesRegisteredResourcesOnceInReverseOrder() {
    RequestScope scope = RequestScope.withoutDeadline();
    List<Integer> closed = new ArrayList<>();
    AutoCloseable first = () -> closed.add(1);
    AutoCloseable second = () -> closed.add(2);

    scope.registerCleanup(first);
    scope.registerCleanup(first);
    scope.registerCleanup(second);
    scope.close();
    scope.close();

    assertEquals(List.of(2, 1), closed);
    assertTrue(scope.isClosed());
  }

  @Test
  void closesLateRegistrationsImmediatelyAndOnlyOnce() {
    RequestScope scope = RequestScope.withoutDeadline();
    List<String> closed = new ArrayList<>();
    AutoCloseable resource = () -> closed.add("closed");
    scope.close();

    scope.registerCleanup(resource);
    scope.registerCleanup(resource);

    assertEquals(List.of("closed"), closed);
  }

  @Test
  void cancellationClosesResourcesAndIsIdempotent() {
    RequestScope scope = RequestScope.withoutDeadline();
    AtomicLong closes = new AtomicLong();
    scope.registerCleanup(closes::incrementAndGet);

    assertTrue(scope.cancel());
    assertFalse(scope.cancel());
    scope.close();

    assertTrue(scope.isCancelled());
    assertEquals(1, closes.get());
  }

  @Test
  void usesMonotonicTimeForRemainingDeadline() {
    AtomicLong now = new AtomicLong(100);
    RequestScope scope = new RequestScope(10, now::get);

    assertEquals(10, scope.remainingDeadlineNanos());
    now.addAndGet(11);
    assertTrue(scope.isDeadlineExceeded());
    assertEquals(0, scope.remainingDeadlineNanos());
  }

  @Test
  void boundsAndRejectsInvalidTimeouts() {
    assertEquals(Long.MAX_VALUE, RequestScope.withoutDeadline().remainingDeadlineNanos());
    assertThrows(
        IllegalArgumentException.class, () -> RequestScope.withTimeout(Duration.ofNanos(-1)));
  }

  @Test
  void operationCompletionClosesScopeWithoutReplacingItsFailure() {
    RequestScope scope = RequestScope.withoutDeadline();
    IOException cleanupFailure = new IOException("credential-must-not-be-logged");
    IOException operationFailure = new IOException("operation failed");
    scope.registerCleanup(
        () -> {
          throw cleanupFailure;
        });
    CompletableFuture<String> operation = new CompletableFuture<>();

    CompletableFuture<String> owned = scope.own(operation).toCompletableFuture();
    operation.completeExceptionally(operationFailure);

    CompletionException observed = assertThrows(CompletionException.class, owned::join);
    assertSame(operationFailure, observed.getCause());
    assertEquals(1, scope.cleanupFailureCount());
    assertTrue(scope.isClosed());
  }
}
