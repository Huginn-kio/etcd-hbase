package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LeaseKeepAliveTimingTest {
  @Test
  void preservesTheClientScheduleAndResponseSafetyMargin() {
    assertEquals(6_000, LeaseKeepAliveTiming.minimumRemainingMillis(15),
        "TTL/3 covers the v3.5.23 client delay and the one-second polling/transport margin");
    assertTrue(LeaseKeepAliveTiming.canSendPositiveResponse(15, 6_001));
    assertFalse(LeaseKeepAliveTiming.canSendPositiveResponse(15, 6_000));
    assertFalse(LeaseKeepAliveTiming.canSendPositiveResponse(15, 4_000));
  }

  @Test
  void capsTheMarginForShortTtlsWithoutMakingAResponseWindowImpossible() {
    assertEquals(900, LeaseKeepAliveTiming.minimumRemainingMillis(1));
    assertTrue(LeaseKeepAliveTiming.canSendPositiveResponse(1, 901));
    assertFalse(LeaseKeepAliveTiming.canSendPositiveResponse(1, 900));
  }

  @Test
  void rejectsNonPositiveOrUnsupportedTtls() {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseKeepAliveTiming.minimumRemainingMillis(0));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseKeepAliveTiming.minimumRemainingMillis(Long.MAX_VALUE));
  }
}
