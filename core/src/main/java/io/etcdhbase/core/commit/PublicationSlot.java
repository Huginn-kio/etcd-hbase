package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** The single durable publication decision not yet fully materialized. */
public record PublicationSlot(
    TransactionId transactionId,
    InvocationId invocationId,
    long ownerEpoch,
    long baseSequence,
    long baseRevision,
    long sequence,
    long revision,
    boolean containsKeyChanges,
    ByteKey manifestRoot,
    ByteKey manifestDigest) {

  public PublicationSlot {
    Objects.requireNonNull(transactionId, "transactionId");
    Objects.requireNonNull(invocationId, "invocationId");
    Objects.requireNonNull(manifestRoot, "manifestRoot");
    Objects.requireNonNull(manifestDigest, "manifestDigest");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0
        || sequence != Math.addExact(baseSequence, 1)
        || revision != Math.addExact(baseRevision, containsKeyChanges ? 1 : 0)
        || manifestRoot.length() == 0 || manifestDigest.length() != 32) {
      throw new IllegalArgumentException("invalid publication slot");
    }
  }
}
