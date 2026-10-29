package io.etcdhbase.server.observability;

import static io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass.KV_UNARY;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.BACKEND_FAILURE;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.request.RequestScope;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestMetricsTest {
  @Test
  void countsOnlyBoundedMethodAndOutcomeDimensions() {
    RequestMetrics metrics = new RequestMetrics();
    RequestScope scope = RequestScope.withoutDeadline();

    metrics.recordCompletion(KV_UNARY, SUCCESS, "00000000-0000-0000-0000-000000000001", scope, 11);
    metrics.recordCompletion(KV_UNARY, SUCCESS, "00000000-0000-0000-0000-000000000001", scope, 17);
    metrics.recordCompletion(
        KV_UNARY, BACKEND_FAILURE, "00000000-0000-0000-0000-000000000001", scope, 19);
    Map<RequestMetrics.Series, RequestMetrics.Snapshot> snapshot = metrics.snapshot();

    assertEquals(
        io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass.values().length
            * io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.values().length,
        snapshot.size());
    assertEquals(2, snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).completed());
    assertEquals(
        28, snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).totalElapsedNanos());
    assertEquals(1, snapshot.get(new RequestMetrics.Series(KV_UNARY, BACKEND_FAILURE)).completed());
  }

  @Test
  void keepsExpectedCompletionsInMetricsWithoutPerRequestLogs() {
    var summaries = new java.util.ArrayList<RequestMetrics.FailureSummary>();
    var metrics = new RequestMetrics(() -> 100L, summaries::add);
    var scope = RequestScope.withoutDeadline();
    for (var outcome : io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.values()) {
      if (outcome == BACKEND_FAILURE) continue;
      for (int index = 0; index < 1000; index++)
        metrics.recordCompletion(
            KV_UNARY, outcome, "00000000-0000-0000-0000-000000000001", scope, 1);
      assertEquals(
          1000, metrics.snapshot().get(new RequestMetrics.Series(KV_UNARY, outcome)).completed());
    }
    assertEquals(0, summaries.size());
  }

  @Test
  void rateLimitsUnexpectedSummariesAndRetainsAllFailureCounters() {
    var now = new java.util.concurrent.atomic.AtomicLong(100);
    var summaries = new java.util.ArrayList<RequestMetrics.FailureSummary>();
    var metrics = new RequestMetrics(now::get, summaries::add);
    var scope = RequestScope.withoutDeadline();
    for (int index = 0; index < 1000; index++)
      metrics.recordCompletion(
          KV_UNARY, BACKEND_FAILURE, "00000000-0000-0000-0000-000000000001", scope, 1);
    assertEquals(1, summaries.size());
    assertEquals(
        1000,
        metrics.snapshot().get(new RequestMetrics.Series(KV_UNARY, BACKEND_FAILURE)).completed());
    now.addAndGet(RequestMetrics.WARNING_INTERVAL_NANOS);
    metrics.recordCompletion(
        KV_UNARY, BACKEND_FAILURE, "00000000-0000-0000-0000-000000000001", scope, 1);
    assertEquals(2, summaries.size());
    assertEquals(1001, summaries.getLast().unexpectedFailures());
    assertEquals(1001, summaries.getLast().completed());
  }

  @Test
  void concurrentFailuresStillEmitAtMostOneSummaryPerInterval() throws Exception {
    var summaries = new java.util.concurrent.ConcurrentLinkedQueue<RequestMetrics.FailureSummary>();
    var metrics = new RequestMetrics(() -> Long.MAX_VALUE - 1, summaries::add);
    var scope = RequestScope.withoutDeadline();
    var start = new java.util.concurrent.CountDownLatch(1);
    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
      var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int worker = 0; worker < 4; worker++)
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int index = 0; index < 500; index++)
                    metrics.recordCompletion(
                        KV_UNARY,
                        BACKEND_FAILURE,
                        "00000000-0000-0000-0000-000000000001",
                        scope,
                        1);
                  return null;
                }));
      start.countDown();
      for (var future : futures) future.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      start.countDown();
    }
    assertEquals(1, summaries.size());
    assertEquals(
        2000,
        metrics.snapshot().get(new RequestMetrics.Series(KV_UNARY, BACKEND_FAILURE)).completed());
  }

  @Test
  void refusesUntrustedTraceTextBeforeLoggingOrCounting() {
    RequestMetrics metrics = new RequestMetrics();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            metrics.recordCompletion(
                KV_UNARY,
                SUCCESS,
                "forged\npassword=private-value",
                RequestScope.withoutDeadline(),
                0));
    assertEquals(
        0,
        metrics.snapshot().values().stream().mapToLong(RequestMetrics.Snapshot::completed).sum());
  }

  @Test
  void rejectsNegativeDurations() {
    RequestMetrics metrics = new RequestMetrics();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            metrics.recordCompletion(
                KV_UNARY,
                SUCCESS,
                "00000000-0000-0000-0000-000000000001",
                RequestScope.withoutDeadline(),
                -1));
  }
}
