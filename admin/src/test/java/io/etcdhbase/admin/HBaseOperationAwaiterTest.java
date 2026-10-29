package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class HBaseOperationAwaiterTest {
  @Test
  void preservesBackendFailureAndCancelsAnExpiredOperation() {
    var unavailable = new IOException("private-fixture-diagnostic");
    assertSame(
        unavailable,
        assertThrows(
            IOException.class,
            () ->
                HBaseOperationAwaiter.await(
                    CompletableFuture.failedFuture(unavailable), Duration.ZERO)));
    var pending = new CompletableFuture<Void>();
    assertThrows(TimeoutException.class, () -> HBaseOperationAwaiter.await(pending, Duration.ZERO));
    assertTrue(pending.isCancelled());
  }

  @Test
  void preservesInterruptionAndCancelsThePendingOperation() {
    var pending = new CompletableFuture<Void>();
    try {
      Thread.currentThread().interrupt();
      assertThrows(
          InterruptedException.class,
          () -> HBaseOperationAwaiter.await(pending, Duration.ofSeconds(30)));
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(pending.isCancelled());
    } finally {
      Thread.interrupted();
    }
  }
}
