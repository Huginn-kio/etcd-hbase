package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Logical key traversal direction; interval bounds retain their original meaning. */
public enum ScanDirection {
  ASCENDING,
  DESCENDING;

  public int compare(ByteKey left, ByteKey right) {
    Objects.requireNonNull(left, "left");
    Objects.requireNonNull(right, "right");
    return this == ASCENDING ? left.compareTo(right) : right.compareTo(left);
  }
}
