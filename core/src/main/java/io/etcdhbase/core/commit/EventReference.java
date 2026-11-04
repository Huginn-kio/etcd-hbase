package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Authenticated pointer to a contiguous ordinal range of immutable events. */
public record EventReference(ByteKey identity, ByteKey digest, long firstOrdinal,
    long lastOrdinal, long itemCount, long totalBytes, int leafPageCount, int level) {
  public EventReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (identity.length() == 0 || digest.length() != 32 || firstOrdinal < 0
        || lastOrdinal < firstOrdinal || lastOrdinal >= ManifestRoot.MAX_EVENTS || itemCount <= 0
        || lastOrdinal - firstOrdinal + 1 != itemCount || totalBytes < 0
        || totalBytes > ManifestRoot.MAX_EVENT_BYTES || leafPageCount <= 0
        || leafPageCount > itemCount
        || level < 0 || level > EventDirectoryPage.MAX_DEPTH) {
      throw new IllegalArgumentException("invalid event page reference");
    }
  }
}
