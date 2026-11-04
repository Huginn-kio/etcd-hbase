package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Bounded authenticated pointer to a candidate leaf page or lower manifest directory node. */
public record ManifestChildReference(
    ByteKey identity,
    ByteKey digest,
    int firstBucket,
    int lastBucket,
    ByteKey firstKey,
    ByteKey lastKey,
    long itemCount,
    long valueBytes,
    int leafPageCount,
    int level) {
  public ManifestChildReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    Objects.requireNonNull(firstKey, "firstKey");
    Objects.requireNonNull(lastKey, "lastKey");
    if (identity.length() == 0 || digest.length() != 32 || firstBucket < 0
        || lastBucket < firstBucket || firstKey.length() == 0 || lastKey.length() == 0
        || firstBucket == lastBucket && firstKey.compareTo(lastKey) > 0
        || itemCount <= 0 || leafPageCount > itemCount || valueBytes < 0
        || valueBytes > ManifestRoot.MAX_VALUE_BYTES || leafPageCount <= 0
        || level < 0 || level > ManifestDirectoryPage.MAX_DEPTH) {
      throw new IllegalArgumentException("invalid manifest child reference");
    }
  }
}
