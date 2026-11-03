package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class HBaseCommitControlStoreCompletionTest {
  @Test
  void scopeCancellationKeepsPendingProducerTrackedAndLeavesCasOutcomeUnknown() {
    RequestScope scope = RequestScope.withoutDeadline();
    CompletableFuture<Boolean> hbaseCas = new CompletableFuture<>();
    CompletableFuture<Boolean> dependent = hbaseCas.thenApply(Boolean::booleanValue);
    CompletableFuture<Boolean> request =
        HBaseRequestCompletion.bindRequestToProducer(scope, () -> dependent);

    assertTrue(scope.cancel());
    BackendFailure cancelled = assertInstanceOf(BackendFailure.class,
        assertThrows(CompletionException.class, request::join).getCause());
    assertEquals(BackendFailure.Category.CANCELLED, cancelled.category());
    assertFalse(hbaseCas.isDone());
    assertFalse(dependent.isDone());
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());

    assertTrue(hbaseCas.complete(true));
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
    BackendFailure stillUnknown = assertInstanceOf(BackendFailure.class,
        assertThrows(CompletionException.class, request::join).getCause());
    assertEquals(BackendFailure.Category.CANCELLED, stillUnknown.category());
  }

  @Test
  void closedScopeDoesNotStartProducer() {
    RequestScope scope = RequestScope.withoutDeadline();
    assertTrue(scope.cancel());
    var started = new AtomicBoolean();

    CompletableFuture<Boolean> request = HBaseRequestCompletion.startRequest(
        scope, () -> {
          started.set(true);
          return CompletableFuture.completedFuture(true);
        });

    assertFalse(started.get());
    BackendFailure cancelled = assertInstanceOf(BackendFailure.class,
        assertThrows(CompletionException.class, request::join).getCause());
    assertEquals(BackendFailure.Category.CANCELLED, cancelled.category());
  }
}
