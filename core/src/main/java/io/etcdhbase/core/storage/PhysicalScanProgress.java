package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Internal physical resume position; it never represents a completed logical key. */
public record PhysicalScanProgress(UUID viewId, int bucket, long revision, ScanDirection direction,
    Optional<ScanCheckpoint> logicalCheckpoint, ByteKey lastPhysicalRow, Optional<UUID> physicalSourceId) {
  public PhysicalScanProgress {
    Objects.requireNonNull(viewId, "viewId");
    Objects.requireNonNull(direction, "direction");
    Objects.requireNonNull(logicalCheckpoint, "logicalCheckpoint");
    Objects.requireNonNull(lastPhysicalRow, "lastPhysicalRow");
    Objects.requireNonNull(physicalSourceId, "physicalSourceId");
    if (bucket < 0 || revision <= 0 || lastPhysicalRow.length() == 0) {
      throw new IllegalArgumentException("invalid physical scan progress");
    }
  }

  public static PhysicalScanProgress after(ScanRequest request, ByteKey physicalRow) {
    Objects.requireNonNull(request, "request");
    return new PhysicalScanProgress(request.viewId(), request.bucket(), request.revisionUpperBound(),
        request.direction(), request.checkpoint(), physicalRow, Optional.empty());
  }

  public static PhysicalScanProgress after(ScanRequest request, ByteKey physicalRow, UUID physicalSourceId) {
    Objects.requireNonNull(request, "request");
    return new PhysicalScanProgress(request.viewId(), request.bucket(), request.revisionUpperBound(),
        request.direction(), request.checkpoint(), physicalRow, Optional.of(physicalSourceId));
  }

  boolean matches(ScanRequest request) {
    return viewId.equals(request.viewId()) && bucket == request.bucket() && revision == request.revisionUpperBound()
        && direction == request.direction() && logicalCheckpoint.equals(request.checkpoint());
  }
}
