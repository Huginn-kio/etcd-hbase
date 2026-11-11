package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class M3CacheScopeDrainTest {
  @Test
  void waitsForLateProducerAndCleanupAfterScopeClose() throws Exception {
    var scope = RequestScope.withTimeout(Duration.ofSeconds(30));
    var producer = new CompletableFuture<Void>();
    var cleanup = new CompletableFuture<Void>();
    scope.trackPhysicalProducer(producer);
    scope.registerScopedCleanup(() -> {}, cleanup);
    var waiting = new CountDownLatch(1);
    try (var worker = Executors.newSingleThreadExecutor()) {
      var completed = worker.submit(() -> {
        try (var drain = new M3CacheScopeDrain(scope); scope) {
          waiting.countDown();
        }
        return null;
      });
      try {
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> completed.get(100, TimeUnit.MILLISECONDS));
        producer.complete(null);
        assertThrows(TimeoutException.class, () -> completed.get(100, TimeUnit.MILLISECONDS));
        cleanup.complete(null);
        completed.get(5, TimeUnit.SECONDS);
        assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
        assertTrue(scope.physicalProducersCompletion().toCompletableFuture().isDone());
        assertFalse(scope.cleanupCompletion().toCompletableFuture().isCompletedExceptionally());
      } finally {
        producer.complete(null);
        cleanup.complete(null);
        scope.close();
      }
    }
  }
}
