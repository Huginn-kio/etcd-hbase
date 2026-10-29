package io.etcdhbase.hbase.probe;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class BackendProbeTest {
  @Test
  void timeoutCancelsOwnedOperationRatherThanLeavingItPending() {
    var pending = new CompletableFuture<>();
    assertThrows(TimeoutException.class, () -> BackendProbe.await(pending, Duration.ofMillis(1)));
    assertTrue(pending.isCancelled());
  }

  @Test
  void interruptCancelsOperationAndPreservesThreadInterruptStatus() {
    var pending = new CompletableFuture<>();
    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedException.class, () -> BackendProbe.await(pending, Duration.ofSeconds(1)));
      assertTrue(pending.isCancelled());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void backendFailureIsPropagatedInsteadOfBecomingAnEmptyResult() {
    IOException original = new IOException("local private detail");
    assertSame(
        original,
        assertThrows(
            IOException.class,
            () ->
                BackendProbe.await(
                    CompletableFuture.failedFuture(original), Duration.ofSeconds(1))));
  }
}
