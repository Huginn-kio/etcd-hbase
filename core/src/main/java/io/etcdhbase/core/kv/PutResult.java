package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;

/** Logical Put response, independent of the endpoint serving a recovered response. */
public record PutResult(ByteKey key, long revision, Optional<StoredRecord> previous) {
  public PutResult {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(previous, "previous");
    if (key.length() == 0 || revision <= 0) {
      throw new IllegalArgumentException("invalid Put result key or revision");
    }
    if (previous.isPresent()) {
      StoredRecord record = previous.orElseThrow();
      if (!record.key().equals(key) || record.metadata().state() != StorageMetadata.State.LIVE
          || !record.hasValue() || record.metadata().modificationRevision() > revision) {
        throw new IllegalArgumentException("invalid previous record in Put result");
      }
    }
  }
}
