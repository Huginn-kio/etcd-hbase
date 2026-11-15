package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseRevocationExecutorTest {
  private static final LeaseState EXPIRED_LEASE =
      LeaseState.grant(new LeaseIdentity(42, 1), 30, 10_000, 1);

  @Test
  void unhealthyClockAtSelectionRejectsAnEarlierExpiryObservation() {
    Optional<LeaseRevocationExecutor.CandidateRejection> rejection =
        LeaseRevocationExecutor.expiryRejectionAtSelection(
            EXPIRED_LEASE, new LeaseTimeObservation(0, 0, false));

    assertEquals(Optional.of(LeaseRevocationExecutor.CandidateRejection.CLOCK_UNHEALTHY), rejection);
  }

  @Test
  void lowerBoundBeforeDeadlineRejectsAnEarlierExpiryObservation() {
    Optional<LeaseRevocationExecutor.CandidateRejection> rejection =
        LeaseRevocationExecutor.expiryRejectionAtSelection(
            EXPIRED_LEASE, new LeaseTimeObservation(9_999, 10_000, true));

    assertEquals(Optional.of(LeaseRevocationExecutor.CandidateRejection.RECHECK), rejection);
  }

  @Test
  void healthyLowerBoundAtDeadlineAllowsExpirySelection() {
    Optional<LeaseRevocationExecutor.CandidateRejection> rejection =
        LeaseRevocationExecutor.expiryRejectionAtSelection(
            EXPIRED_LEASE, new LeaseTimeObservation(10_000, 10_001, true));

    assertTrue(rejection.isEmpty());
  }
}
