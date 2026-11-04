package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Immutable sorted candidate page; bounded independently from the full transaction size. */
public final class ManifestPage {
  public static final int MAX_ENTRIES = 128;
  public static final int MAX_PAGE_BYTES = 1024 * 1024;
  private final TransactionId transactionId;
  private final int pageIndex;
  private final int bucket;
  private final List<ManifestCandidateReference> candidates;
  private final ByteKey digest;

  private ManifestPage(TransactionId transactionId, int pageIndex,
      List<ManifestCandidateReference> candidates, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.candidates = List.copyOf(candidates);
    if (pageIndex < 0 || this.candidates.isEmpty() || this.candidates.size() > MAX_ENTRIES
        || expectedDigest != null && expectedDigest.length() != 32
        || encodedLength(this.candidates) > MAX_PAGE_BYTES) {
      throw new IllegalArgumentException("manifest page exceeds its contract");
    }
    for (int index = 1; index < this.candidates.size(); index++) {
      if (this.candidates.get(index - 1).bucket() != this.candidates.get(index).bucket()
          || this.candidates.get(index - 1).logicalKey()
          .compareTo(this.candidates.get(index).logicalKey()) >= 0) {
        throw new IllegalArgumentException("manifest page must contain one bucket with sorted keys");
      }
    }
    this.pageIndex = pageIndex;
    this.bucket = this.candidates.getFirst().bucket();
    this.digest = calculateDigest(transactionId, pageIndex, this.candidates);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("manifest page digest mismatch");
    }
  }

  public static ManifestPage create(
      TransactionId transactionId, int pageIndex, List<ManifestCandidateReference> candidates) {
    return new ManifestPage(transactionId, pageIndex, candidates, null);
  }

  public static ManifestPage restore(TransactionId transactionId, int pageIndex,
      List<ManifestCandidateReference> candidates, ByteKey digest) {
    return new ManifestPage(transactionId, pageIndex, candidates,
        Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public int pageIndex() { return pageIndex; }
  public int bucket() { return bucket; }
  public List<ManifestCandidateReference> candidates() { return candidates; }
  public ByteKey digest() { return digest; }
  public ByteKey firstKey() { return candidates.getFirst().logicalKey(); }
  public ByteKey lastKey() { return candidates.getLast().logicalKey(); }

  public ManifestChildReference reference() {
    long valueBytes = candidates.stream().mapToLong(ManifestCandidateReference::valueLength)
        .reduce(0, Math::addExact);
    return new ManifestChildReference(systemIdentity(), digest, bucket, bucket, firstKey(), lastKey(),
        candidates.size(), valueBytes, 1, 0);
  }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.MANIFEST_PAGE).putInt(pageIndex).array());
  }

  private static int encodedLength(List<ManifestCandidateReference> candidates) {
    long total = 64;
    for (ManifestCandidateReference candidate : candidates) {
      total += 6L * Integer.BYTES + candidate.logicalKey().length()
          + candidate.candidateIdentity().length() + candidate.candidateDigest().length()
          + candidate.valueDigest().length() + 1;
      if (total > MAX_PAGE_BYTES) return MAX_PAGE_BYTES + 1;
    }
    return (int) total;
  }

  private static ByteKey calculateDigest(
      TransactionId transactionId, int pageIndex, List<ManifestCandidateReference> candidates) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(2);
      output.write(transactionId.bytes());
      output.writeInt(pageIndex);
      output.writeInt(candidates.size());
      for (ManifestCandidateReference candidate : candidates) {
        output.writeInt(candidate.bucket());
        ManifestDigest.writeBytes(output, candidate.logicalKey().bytes());
        ManifestDigest.writeBytes(output, candidate.candidateIdentity().bytes());
        ManifestDigest.writeBytes(output, candidate.candidateDigest().bytes());
        ManifestDigest.writeBytes(output, candidate.valueDigest().bytes());
        output.writeInt(candidate.valueLength());
        output.writeBoolean(candidate.tombstone());
      }
    });
  }
}
