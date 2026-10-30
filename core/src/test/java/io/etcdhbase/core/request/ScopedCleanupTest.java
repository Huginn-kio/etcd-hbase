package io.etcdhbase.core.request;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ScopedCleanupTest {
  @Test
  void completedPagesDoNotAccumulateResourcesAndCloseIsIdempotent() throws Exception {
    var closed = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline()) {
      for (int index = 0; index < 10_000; index++) {
        var registration = scope.registerScopedCleanup(closed::incrementAndGet);
        assertEquals(1, scope.registeredResourceCount());
        registration.close();
        registration.close();
        assertEquals(0, scope.registeredResourceCount());
      }
    }
    assertEquals(10_000, closed.get());
  }

  @Test
  void lateAndCancellationCleanupStillHaveExactlyOneOwner() throws Exception {
    var closed = new AtomicInteger();
    var scope = RequestScope.withoutDeadline();
    var registration = scope.registerScopedCleanup(closed::incrementAndGet);
    scope.cancel();
    registration.close();
    scope.registerScopedCleanup(closed::incrementAndGet).close();
    assertEquals(2, closed.get());
    assertEquals(0, scope.registeredResourceCount());
  }
}
