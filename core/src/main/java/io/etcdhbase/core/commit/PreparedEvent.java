package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Ordered immutable KV event with its exact selected value and optional PrevKV projection. */
public final class PreparedEvent {
  public enum Kind { PUT, DELETE }
  public static final int MAX_EVENT_BYTES = 8 * 1024 * 1024 - 4096;

  private final TransactionId transactionId;
  private final long ownerEpoch;
  private final long baseSequence;
  private final long baseRevision;
  private final long ordinal;
  private final Kind kind;
  private final StoredRecord current;
  private final Optional<StoredRecord> previous;
  private final ByteKey digest;
  private final int encodedBytes;

  public PreparedEvent(TransactionId transactionId, long ownerEpoch, long baseSequence,
      long baseRevision, long ordinal, Kind kind, StoredRecord current,
      Optional<StoredRecord> previous) {
    this(transactionId, ownerEpoch, baseSequence, baseRevision, ordinal, kind, current,
        previous, null);
  }

  private PreparedEvent(TransactionId transactionId, long ownerEpoch, long baseSequence,
      long baseRevision, long ordinal, Kind kind, StoredRecord current,
      Optional<StoredRecord> previous, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.kind = Objects.requireNonNull(kind, "kind");
    this.current = Objects.requireNonNull(current, "current");
    this.previous = Objects.requireNonNull(previous, "previous");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0 || ordinal < 0
        || current.metadata().modificationRevision() != Math.addExact(baseRevision, 1)
        || current.metadata().state() != (kind == Kind.PUT
            ? StorageMetadata.State.LIVE : StorageMetadata.State.TOMBSTONE)
        || previous.filter(value -> !value.key().equals(current.key())
            || value.metadata().modificationRevision() > baseRevision).isPresent()) {
      throw new IllegalArgumentException("event does not match its commit identity or operation kind");
    }
    requireDecodableProjection(current);
    previous.ifPresent(PreparedEvent::requireDecodableProjection);
    this.ownerEpoch = ownerEpoch;
    this.baseSequence = baseSequence;
    this.baseRevision = baseRevision;
    this.ordinal = ordinal;
    byte[] canonical = canonicalBytes();
    this.encodedBytes = canonical.length;
    this.digest = ManifestDigest.sha256(canonical);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("event digest mismatch");
    }
  }

  public static PreparedEvent restore(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long ordinal, Kind kind, StoredRecord current,
      Optional<StoredRecord> previous, ByteKey digest) {
    if (Objects.requireNonNull(digest, "digest").length() != 32) {
      throw new IllegalArgumentException("event digest must be SHA-256");
    }
    return new PreparedEvent(transactionId, ownerEpoch, baseSequence, baseRevision, ordinal,
        kind, current, previous, digest);
  }

  public TransactionId transactionId() { return transactionId; }
  public long ownerEpoch() { return ownerEpoch; }
  public long baseSequence() { return baseSequence; }
  public long baseRevision() { return baseRevision; }
  public long ordinal() { return ordinal; }
  public Kind kind() { return kind; }
  public StoredRecord current() { return current; }
  public Optional<StoredRecord> previous() { return previous; }
  public ByteKey digest() { return digest; }

  /** Stable event identity is ordinal-addressed so repeated mutations of one key stay ordered. */
  public ByteKey systemIdentity() {
    return ByteKey.copyOf(java.nio.ByteBuffer.allocate(16 + 1 + Long.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.PREPARED_EVENT).putLong(ordinal).array());
  }

  public int retainedBytes() {
    return encodedBytes;
  }

  private byte[] canonicalBytes() {
    byte[] bytes = ManifestDigest.encode(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeLong(ownerEpoch);
      output.writeLong(baseSequence);
      output.writeLong(baseRevision);
      output.writeLong(ordinal);
      output.writeByte(kind.ordinal());
      writeRecord(output, current);
      output.writeBoolean(previous.isPresent());
      if (previous.isPresent()) writeRecord(output, previous.orElseThrow());
    });
    if (bytes.length > MAX_EVENT_BYTES) throw new IllegalArgumentException("event exceeds byte limit");
    return bytes;
  }

  private static void writeRecord(DataOutputStream output, StoredRecord record) throws IOException {
    byte[] key = record.key().bytes();
    var metadata = record.metadata();
    byte[] value = record.value().orElseGet(() -> new byte[0]);
    output.writeInt(key.length);
    output.write(key);
    output.writeLong(metadata.createRevision());
    output.writeLong(metadata.modificationRevision());
    output.writeLong(metadata.version());
    output.writeLong(metadata.leaseId());
    output.writeByte(metadata.state().ordinal());
    output.writeInt(metadata.valueLength());
    output.writeInt(value.length);
    output.write(value);
  }

  private static void requireDecodableProjection(final StoredRecord record) {
    if (record.metadata().state() == StorageMetadata.State.LIVE
        && (!record.hasValue()
            || record.metadata().valueLength()
                > PreparedCandidate.MAX_VALUE_BYTES)) {
      throw new IllegalArgumentException(
          "live event value must be present and within the storage limit");
    }
  }

}
