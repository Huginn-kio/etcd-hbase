package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Immutable private KV candidate; it becomes visible only through its sealed manifest. */
public final class PreparedCandidate {
  public static final int MAX_VALUE_BYTES = 4 * 1024 * 1024;
  private final TransactionId transactionId;
  private final long ownerEpoch;
  private final long baseSequence;
  private final long baseRevision;
  private final StoredRecord record;
  private final ByteKey contentDigest;

  public PreparedCandidate(
      TransactionId transactionId,
      long ownerEpoch,
      long baseSequence,
      long baseRevision,
      StoredRecord record) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.record = Objects.requireNonNull(record, "record");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0
        || record.metadata().modificationRevision() != Math.addExact(baseRevision, 1)
        || record.metadata().createRevision() > record.metadata().modificationRevision()
        || record.metadata().valueLength() > MAX_VALUE_BYTES
        || record.metadata().state() == StorageMetadata.State.LIVE && !record.hasValue()
        || record.metadata().state() == StorageMetadata.State.TOMBSTONE && record.hasValue()) {
      throw new IllegalArgumentException("candidate does not match its commit base or value state");
    }
    this.ownerEpoch = ownerEpoch;
    this.baseSequence = baseSequence;
    this.baseRevision = baseRevision;
    this.contentDigest = ByteKey.copyOf(digest(canonicalBytes()));
  }

  public TransactionId transactionId() { return transactionId; }
  public long ownerEpoch() { return ownerEpoch; }
  public long baseSequence() { return baseSequence; }
  public long baseRevision() { return baseRevision; }
  public StoredRecord record() { return record; }
  public ByteKey contentDigest() { return contentDigest; }

  /** Key used for direct candidate lookup; the variable key is length-framed after txnId. */
  public ByteKey systemIdentity() {
    byte[] key = record.key().bytes();
    ByteBuffer identity = ByteBuffer.allocate(16 + Integer.BYTES + key.length)
        .put(transactionId.bytes()).putInt(key.length).put(key);
    return ByteKey.copyOf(identity.array());
  }

  public ByteKey valueDigest() {
    return ByteKey.copyOf(digest(record.value().orElseGet(() -> new byte[0])));
  }

  private byte[] canonicalBytes() {
    byte[] key = record.key().bytes();
    byte[] value = record.value().orElseGet(() -> new byte[0]);
    var metadata = record.metadata();
    int state = metadata.state() == StorageMetadata.State.LIVE ? 1 : 2;
    ByteBuffer bytes = ByteBuffer.allocate(16 + Long.BYTES * 7 + Integer.BYTES * 2
        + key.length + value.length + 1);
    bytes.put(transactionId.bytes()).putLong(ownerEpoch).putLong(baseSequence).putLong(baseRevision)
        .putLong(metadata.createRevision()).putLong(metadata.modificationRevision())
        .putLong(metadata.version()).putLong(metadata.leaseId()).putInt(key.length).put(key)
        .put((byte) state).putInt(value.length).put(value);
    return bytes.array();
  }

  private static byte[] digest(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }
}
