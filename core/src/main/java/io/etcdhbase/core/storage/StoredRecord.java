package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;

/** Immutable logical projection. An omitted value is distinct from a present empty value. */
public final class StoredRecord {
  private final ByteKey key;
  private final StorageMetadata metadata;
  private final byte[] value;

  private StoredRecord(ByteKey key, StorageMetadata metadata, byte[] value) {
    this.key = Objects.requireNonNull(key, "key");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.value = value == null ? null : value.clone();
  }

  public static StoredRecord metadataOnly(ByteKey key, StorageMetadata metadata) {
    return new StoredRecord(key, metadata, null);
  }

  public static StoredRecord live(ByteKey key, StorageMetadata metadata, byte[] value) {
    Objects.requireNonNull(value, "value");
    if (metadata.state() != StorageMetadata.State.LIVE || value.length != metadata.valueLength()) {
      throw new IllegalArgumentException("value does not match live metadata");
    }
    return new StoredRecord(key, metadata, value);
  }

  public ByteKey key() {
    return key;
  }

  public StorageMetadata metadata() {
    return metadata;
  }

  public boolean hasValue() {
    return value != null;
  }

  public Optional<byte[]> value() {
    return value == null ? Optional.empty() : Optional.of(value.clone());
  }

  /** Conservative application accounting units, not an assertion about JVM object layout. */
  public long retainedBytes() {
    return 256L + key.length() + (value == null ? 0 : value.length);
  }
}
