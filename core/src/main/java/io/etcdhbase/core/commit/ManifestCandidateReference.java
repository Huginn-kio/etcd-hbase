package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import java.util.Objects;

/** Bounded reference to one immutable private candidate, excluding the candidate value bytes. */
public record ManifestCandidateReference(
    int bucket,
    ByteKey logicalKey,
    ByteKey candidateIdentity,
    ByteKey candidateDigest,
    ByteKey valueDigest,
    int valueLength,
    boolean tombstone) {
  public ManifestCandidateReference {
    Objects.requireNonNull(logicalKey, "logicalKey");
    Objects.requireNonNull(candidateIdentity, "candidateIdentity");
    Objects.requireNonNull(candidateDigest, "candidateDigest");
    Objects.requireNonNull(valueDigest, "valueDigest");
    if (bucket < 0 || logicalKey.length() == 0 || candidateIdentity.length() == 0
        || candidateDigest.length() != 32 || valueDigest.length() != 32
        || valueLength < 0 || valueLength > PreparedCandidate.MAX_VALUE_BYTES
        || tombstone && valueLength != 0) {
      throw new IllegalArgumentException("invalid manifest candidate reference");
    }
  }

  public static ManifestCandidateReference from(PreparedCandidate candidate, int bucket) {
    Objects.requireNonNull(candidate, "candidate");
    return new ManifestCandidateReference(bucket, candidate.record().key(), candidate.systemIdentity(),
        candidate.contentDigest(), candidate.valueDigest(), candidate.record().metadata().valueLength(),
        candidate.record().metadata().state() == StorageMetadata.State.TOMBSTONE);
  }
}
