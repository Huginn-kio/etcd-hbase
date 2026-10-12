package io.etcdhbase.server.observability;

import io.etcdhbase.core.request.RequestScope;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static io.etcdhbase.server.observability.RequestMetrics.MethodClass.KV_UNARY;
import static io.etcdhbase.server.observability.RequestMetrics.Outcome.BACKEND_FAILURE;
import static io.etcdhbase.server.observability.RequestMetrics.Outcome.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestMetricsTest {
  @Test
  void countsOnlyBoundedMethodAndOutcomeDimensions() {
    RequestMetrics metrics = new RequestMetrics();
    RequestScope scope = RequestScope.withoutDeadline();

    metrics.recordCompletion(KV_UNARY, SUCCESS, scope, 11);
    metrics.recordCompletion(KV_UNARY, BACKEND_FAILURE, scope, 17);
    Map<RequestMetrics.Series, RequestMetrics.Snapshot> snapshot = metrics.snapshot();

    assertEquals(6 * 5, snapshot.size());
    assertEquals(1, snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).completed());
    assertEquals(
        11,
        snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).totalElapsedNanos());
    assertEquals(1, snapshot.get(new RequestMetrics.Series(KV_UNARY, BACKEND_FAILURE)).completed());
  }

  @Test
  void rejectsNegativeDurations() {
    RequestMetrics metrics = new RequestMetrics();

    assertThrows(
        IllegalArgumentException.class,
        () -> metrics.recordCompletion(KV_UNARY, SUCCESS, RequestScope.withoutDeadline(), -1));
  }
}
