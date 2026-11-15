package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LeaseTimeRulesTest {
  @Test
  void reportsConservativeRemainingTtlInMillisAndCapsAtGrantedTtl() {
    LeaseState state = LeaseState.grant(new LeaseIdentity(1, 1), 15, 25_000, 1);

    assertEquals(15_000, LeaseTimeRules.remainingTtlMillis(state,
        new LeaseTimeObservation(0, 0, true)));
    assertEquals(3_766, LeaseTimeRules.remainingTtlMillis(state,
        new LeaseTimeObservation(21_234, 21_234, true)));
    assertEquals(3, LeaseTimeRules.remainingTtlSeconds(state,
        new LeaseTimeObservation(21_234, 21_234, true)));
    assertEquals(0, LeaseTimeRules.remainingTtlMillis(state,
        new LeaseTimeObservation(25_000, 25_000, true)));
    assertEquals(-1, LeaseTimeRules.remainingTtlMillis(state.revoke(2),
        new LeaseTimeObservation(21_234, 21_234, true)));
  }
}
