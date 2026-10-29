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
