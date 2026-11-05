package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class M2CostMetricsTest {
  @Test
  void recordsWrappedUnknownWithoutHidingTheFailure() {
    M2CostMetrics metrics = new M2CostMetrics();
    BackendFailure timeout = new BackendFailure(BackendFailure.Category.TIMEOUT);
    CompletableFuture<Boolean> result = metrics.observe("read", 7,
        () -> CompletableFuture.failedFuture(new CompletionException(timeout)),
        ignored -> 0, ignored -> false);

    CompletionException failure = assertThrows(CompletionException.class, result::join);
    assertSame(timeout, CompletionFailures.unwrapCompletion(failure));
    M2CostMetrics.Value value = metrics.snapshot().get("read");
    assertEquals(1, value.calls());
    assertEquals(1, value.unknown());
    assertEquals(0, value.failed());
  }

  @Test
  void recordsSynchronousFatalFailureBeforePropagatingIt() {
    M2CostMetrics metrics = new M2CostMetrics();
    AssertionError fatal = new AssertionError("fixture fatal");
    AssertionError failure = assertThrows(AssertionError.class, () -> metrics.<Boolean>observe(
        "write", 0, () -> { throw fatal; }, ignored -> 0, ignored -> false));

    assertSame(fatal, failure);
    M2CostMetrics.Value value = metrics.snapshot().get("write");
    assertEquals(1, value.calls());
    assertEquals(0, value.unknown());
    assertEquals(1, value.failed());
  }
}
