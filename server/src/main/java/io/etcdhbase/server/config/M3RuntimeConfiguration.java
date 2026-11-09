package io.etcdhbase.server.config;

import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import java.time.Duration;

/** Explicit activation settings; member identity and budgets are never invented at startup. */
public record M3RuntimeConfiguration(
    String memberId,
    int ownerLeaseMs,
    int heartbeatMs,
    int startupTimeoutMs,
    int shutdownTimeoutMs,
    long memoryBytes,
    int maxEtcdRequestBytes,
    int pinShards,
    int pinSlotsPerShard,
    int pinLeaseMs) {
  private static final long MIN_MEMORY = 256L << 20;
  private static final long MAX_MEMORY = 1L << 40;

  public M3RuntimeConfiguration {
    if (memberId == null || !memberId.matches("[1-9][0-9]{0,19}")) {
      throw new IllegalArgumentException("member ID must be an explicit nonzero uint64");
    }
    try {
      if (Long.parseUnsignedLong(memberId) == 0) throw new NumberFormatException();
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("member ID must be an explicit nonzero uint64");
    }
    if (ownerLeaseMs < 1_000 || ownerLeaseMs > 600_000 || heartbeatMs < 100
        || heartbeatMs > ownerLeaseMs / 3 || startupTimeoutMs < 100 || startupTimeoutMs > 600_000
        || shutdownTimeoutMs < 1_000 || shutdownTimeoutMs > 600_000
        || memoryBytes < MIN_MEMORY || memoryBytes > MAX_MEMORY
        || maxEtcdRequestBytes < 1 || maxEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid M3 runtime limits");
    }
    new SnapshotPinConfiguration(pinShards, pinSlotsPerShard, Duration.ofMillis(pinLeaseMs));
  }

  /** Preserve the raw bits of etcd's uint64, including values above Long.MAX_VALUE. */
  public long memberIdBits() { return Long.parseUnsignedLong(memberId); }

  public SnapshotPinConfiguration pinConfiguration() {
    return new SnapshotPinConfiguration(pinShards, pinSlotsPerShard, Duration.ofMillis(pinLeaseMs));
  }
}
