package io.etcdhbase.core.storage;

import java.util.Objects;

/** Physical record metadata; it deliberately makes no transaction visibility decision. */
public record StorageMetadata(long createRevision, long modificationRevision, long version,
    long leaseId, State state, int valueLength) {
  public enum State { LIVE, TOMBSTONE }

  public StorageMetadata {
    Objects.requireNonNull(state, "state");
    if (createRevision <= 0 || modificationRevision < createRevision || version <= 0) {
      throw new IllegalArgumentException("invalid record revision or version");
    }
    if (valueLength < 0 || state == State.TOMBSTONE && valueLength != 0) {
      throw new IllegalArgumentException("invalid record value length");
    }
  }
}
