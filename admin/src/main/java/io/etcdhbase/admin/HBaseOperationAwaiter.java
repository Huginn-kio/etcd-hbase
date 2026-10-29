package io.etcdhbase.admin;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Waits for an operation, without claiming that cancellation rolls back a server procedure. */
final class HBaseOperationAwaiter {
  static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);

  private HBaseOperationAwaiter() {}

  static <T> T await(CompletableFuture<T> operation, Duration timeout) throws Exception {
    try {
      return operation.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    } catch (TimeoutException expired) {
      operation.cancel(true);
      throw expired;
    } catch (InterruptedException interrupted) {
      operation.cancel(true);
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (ExecutionException failed) {
      if (failed.getCause() instanceof Exception cause) {
        throw cause;
      }
      if (failed.getCause() instanceof Error cause) {
        throw cause;
      }
      throw new IllegalStateException("HBase initialization operation failed", failed.getCause());
    }
  }
}
