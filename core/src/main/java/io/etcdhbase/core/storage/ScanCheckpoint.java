package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Last fully consumed logical key, bound to a bucket, interval, revision and direction. */
public record ScanCheckpoint(int bucket, KeyRange.Kind kind, Optional<ByteKey> lower,
    Optional<ByteKey> upper, long revision, ScanDirection direction, UUID viewId, ByteKey key) {
  public ScanCheckpoint {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(lower, "lower");
    Objects.requireNonNull(upper, "upper");
    Objects.requireNonNull(direction, "direction");
    Objects.requireNonNull(viewId, "viewId");
    Objects.requireNonNull(key, "key");
    if (bucket < 0 || revision <= 0) throw new IllegalArgumentException("invalid scan checkpoint");
  }

  public static ScanCheckpoint after(ScanRequest request, ByteKey key) {
    Objects.requireNonNull(request, "request");
    return after(request.bucket(), request.range(), request.revisionUpperBound(), request.direction(), request.viewId(), key);
  }

  static ScanCheckpoint after(int bucket, KeyRange range, long revision, ScanDirection direction, UUID viewId, ByteKey key) {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(key, "key");
    if (!range.contains(key)) throw new IllegalArgumentException("checkpoint is outside the logical interval");
    return new ScanCheckpoint(bucket, range.kind(), range.lowerInclusive(), range.upperExclusive(),
        revision, direction, viewId, key);
  }

  boolean matches(int expectedBucket, KeyRange range, long expectedRevision, ScanDirection expectedDirection,
      UUID expectedViewId) {
    return bucket == expectedBucket && kind == range.kind() && lower.equals(range.lowerInclusive())
        && upper.equals(range.upperExclusive()) && revision == expectedRevision && direction == expectedDirection
        && viewId.equals(expectedViewId) && range.contains(key);
  }
}
