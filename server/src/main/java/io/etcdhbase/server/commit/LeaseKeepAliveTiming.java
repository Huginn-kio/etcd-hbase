package io.etcdhbase.server.commit;

import io.etcdhbase.core.lease.LeaseTimeRules;

/** Conservative response window for the fixed etcd v3.5.23 KeepAlive client schedule. */
final class LeaseKeepAliveTiming {
  // The pinned v3.5.23 client polls its send loop every 500 ms. The other 500 ms is an explicit
  // local response-handoff budget assumption, not a guarantee about arbitrary network pauses.
  private static final long CLIENT_POLL_AND_LOCAL_HANDOFF_BUDGET_MILLIS = 1_000;
  // Preserve a sendable interval below a freshly granted TTL when the full budget exceeds a short TTL.
  private static final long MINIMUM_SENDABLE_WINDOW_MILLIS = 100;

  private LeaseKeepAliveTiming() {}

  static long minimumRemainingMillis(long grantedTtlSeconds) {
    if (grantedTtlSeconds <= 0 || grantedTtlSeconds > LeaseTimeRules.MAX_TTL_SECONDS) {
      throw new IllegalArgumentException("invalid KeepAlive TTL");
    }
    long ttlMillis = Math.multiplyExact(grantedTtlSeconds, 1_000);
    long nextKeepAliveDelayMillis = ttlMillis / 3;
    long availableMarginMillis = Math.max(0,
        ttlMillis - nextKeepAliveDelayMillis - MINIMUM_SENDABLE_WINDOW_MILLIS);
    long safetyMarginMillis = Math.min(CLIENT_POLL_AND_LOCAL_HANDOFF_BUDGET_MILLIS,
        availableMarginMillis);
    return Math.addExact(nextKeepAliveDelayMillis, safetyMarginMillis);
  }

  static boolean canSendPositiveResponse(long grantedTtlSeconds, long remainingMillis) {
    return remainingMillis > minimumRemainingMillis(grantedTtlSeconds);
  }
}
