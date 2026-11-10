package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Frozen-dataset page request. Resume excludes the last completely consumed logical key. */
public record ScanRequest(int bucket, KeyRange range, Optional<ScanCheckpoint> checkpoint,
    long revisionUpperBound, ReadProjection projection, int maximumRows, long maximumBytes,
    int maximumPhysicalRows, ScanDirection direction, UUID viewId, Optional<PhysicalScanProgress> physicalProgress) {
  private record Initial(UUID viewId, Optional<ScanCheckpoint> checkpoint) {}

  /** Existing scan callers retain ascending order and logical-key resume semantics. */
  public ScanRequest(int bucket, KeyRange range, Optional<ByteKey> after,
      long revisionUpperBound, ReadProjection projection, int maximumRows, long maximumBytes,
      int maximumPhysicalRows) {
    this(bucket, range, initial(bucket, range, after, revisionUpperBound), revisionUpperBound,
        projection, maximumRows, maximumBytes, maximumPhysicalRows);
  }

  private ScanRequest(int bucket, KeyRange range, Initial initial, long revisionUpperBound,
      ReadProjection projection, int maximumRows, long maximumBytes, int maximumPhysicalRows) {
    this(bucket, range, initial.checkpoint(), revisionUpperBound, projection, maximumRows, maximumBytes,
        maximumPhysicalRows, ScanDirection.ASCENDING, initial.viewId(), Optional.empty());
  }

  public ScanRequest(int bucket, KeyRange range, Optional<ScanCheckpoint> checkpoint,
      long revisionUpperBound, ReadProjection projection, int maximumRows, long maximumBytes,
      int maximumPhysicalRows, ScanDirection direction) {
    this(bucket, range, requireNewView(checkpoint), revisionUpperBound, projection, maximumRows,
        maximumBytes, maximumPhysicalRows, direction, UUID.randomUUID(), Optional.empty());
  }

  public ScanRequest {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(projection, "projection");
    Objects.requireNonNull(direction, "direction");
    Objects.requireNonNull(viewId, "viewId");
    Objects.requireNonNull(physicalProgress, "physicalProgress");
    if (bucket < 0 || revisionUpperBound <= 0 || maximumRows <= 0 || maximumRows > 1024
        || maximumBytes <= 0 || maximumBytes > 64L * 1024 * 1024
        || maximumPhysicalRows <= 0 || maximumPhysicalRows > 1_000_000) {
      throw new IllegalArgumentException("invalid bounded scan request");
    }
    if (checkpoint.isPresent() && !checkpoint.orElseThrow().matches(bucket, range, revisionUpperBound, direction, viewId)) {
      throw new IllegalArgumentException("checkpoint belongs to a different scan");
    }
    if (physicalProgress.isPresent()) {
      var progress = physicalProgress.orElseThrow();
      if (!progress.viewId().equals(viewId) || progress.bucket() != bucket || progress.revision() != revisionUpperBound
          || progress.direction() != direction || !progress.logicalCheckpoint().equals(checkpoint)) {
        throw new IllegalArgumentException("physical progress belongs to a different scan");
      }
    }
  }

  public Optional<ByteKey> after() { return checkpoint.map(ScanCheckpoint::key); }

  public ScanRequest resumeAfter(Optional<ByteKey> lastConsumed) {
    Objects.requireNonNull(lastConsumed, "lastConsumed");
    return new ScanRequest(bucket, range, lastConsumed.map(key -> ScanCheckpoint.after(this, key)),
        revisionUpperBound, projection, maximumRows, maximumBytes, maximumPhysicalRows, direction, viewId, Optional.empty());
  }

  public ScanRequest resumeFrom(ScanCheckpoint saved) {
    return new ScanRequest(bucket, range, Optional.of(Objects.requireNonNull(saved, "saved")), revisionUpperBound,
        projection, maximumRows, maximumBytes, maximumPhysicalRows, direction, viewId, Optional.empty());
  }

  public ScanRequest resumePhysical(PhysicalScanProgress progress) {
    Objects.requireNonNull(progress, "progress");
    if (!progress.matches(this)) throw new IllegalArgumentException("physical progress belongs to a different scan");
    return new ScanRequest(bucket, range, checkpoint, revisionUpperBound, projection, maximumRows,
        maximumBytes, maximumPhysicalRows, direction, viewId, Optional.of(progress));
  }

  private static Initial initial(int bucket, KeyRange range, Optional<ByteKey> after, long revision) {
    UUID id = UUID.randomUUID();
    return new Initial(id, Objects.requireNonNull(after, "after").map(key ->
        ScanCheckpoint.after(bucket, range, revision, ScanDirection.ASCENDING, id, key)));
  }

  private static Optional<ScanCheckpoint> requireNewView(Optional<ScanCheckpoint> checkpoint) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    if (checkpoint.isPresent()) throw new IllegalArgumentException("resume requires the original scan view");
    return checkpoint;
  }
}
