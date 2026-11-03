package io.etcdhbase.core.commit;

import java.time.Duration;
import java.util.Objects;

/** Fixed, durable geometry and lease policy for one table group's snapshot-pin registry. */
public record SnapshotPinConfiguration(int shardCount, int slotsPerShard, Duration leaseDuration) {
  public static final int MAX_SHARDS = 256;
  public static final int MAX_SLOTS_PER_SHARD = 512;
  public static final long MIN_LEASE_MILLIS = 10;
  public static final long MAX_LEASE_MILLIS = 300_000;

  public SnapshotPinConfiguration {
    Objects.requireNonNull(leaseDuration, "leaseDuration");
    long leaseMillis;
    try {
      leaseMillis = leaseDuration.toMillis();
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("snapshot pin lease is outside its supported range", overflow);
    }
    if (shardCount <= 0 || shardCount > MAX_SHARDS || slotsPerShard <= 0
        || slotsPerShard > MAX_SLOTS_PER_SHARD || leaseDuration.isNegative()
        || leaseDuration.isZero() || leaseMillis < MIN_LEASE_MILLIS
        || leaseMillis > MAX_LEASE_MILLIS || !Duration.ofMillis(leaseMillis).equals(leaseDuration)) {
      throw new IllegalArgumentException("invalid snapshot pin registry configuration");
    }
  }

  public long leaseMillis() {
    return leaseDuration.toMillis();
  }
}
