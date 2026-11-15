package io.etcdhbase.core.lease;

import java.util.Objects;

/** Identifies one successful renewal within a Lease incarnation. */
public record LeaseRenewalVersion(LeaseIdentity identity, long renewalGeneration) {
  public LeaseRenewalVersion {
    Objects.requireNonNull(identity, "identity");
    if (renewalGeneration <= 0) {
      throw new IllegalArgumentException("renewal generation must be positive");
    }
  }

  public static LeaseRenewalVersion from(LeaseState state) {
    Objects.requireNonNull(state, "state");
    if (state.status() != LeaseState.Status.ACTIVE) {
      throw new IllegalArgumentException("renewal version must come from an active Lease");
    }
    return new LeaseRenewalVersion(state.identity(), state.renewalGeneration());
  }

  public boolean matches(LeaseState state) {
    return state != null && state.status() == LeaseState.Status.ACTIVE
        && identity.equals(state.identity()) && renewalGeneration == state.renewalGeneration();
  }
}
