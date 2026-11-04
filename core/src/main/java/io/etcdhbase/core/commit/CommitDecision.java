package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Immutable archive of the control-row selection and its exact sealed manifest identity. */
public record CommitDecision(
    TransactionId transactionId,
    InvocationId invocationId,
    long sequence,
    long baseRevision,
    long revision,
    ByteKey manifestRoot,
    ByteKey manifestDigest,
    boolean containsKeyChanges) {
  public CommitDecision {
    Objects.requireNonNull(transactionId, "transactionId");
    Objects.requireNonNull(invocationId, "invocationId");
    Objects.requireNonNull(manifestRoot, "manifestRoot");
    Objects.requireNonNull(manifestDigest, "manifestDigest");
    if (sequence <= 0 || baseRevision <= 0
        || revision != Math.addExact(baseRevision, containsKeyChanges ? 1 : 0)
        || manifestRoot.length() == 0
        || manifestRoot.length() > 8192 || manifestDigest.length() != 32) {
      throw new IllegalArgumentException("invalid immutable commit decision");
    }
  }

  public static CommitDecision from(PublicationSlot slot) {
    Objects.requireNonNull(slot, "slot");
    return new CommitDecision(slot.transactionId(), slot.invocationId(), slot.sequence(),
        slot.baseRevision(), slot.revision(), slot.manifestRoot(), slot.manifestDigest(),
        slot.containsKeyChanges());
  }

  public boolean matches(PublicationSlot slot) {
    return slot != null && transactionId.equals(slot.transactionId())
        && invocationId.equals(slot.invocationId()) && sequence == slot.sequence()
        && baseRevision == slot.baseRevision()
        && revision == slot.revision() && manifestRoot.equals(slot.manifestRoot())
        && manifestDigest.equals(slot.manifestDigest())
        && containsKeyChanges == slot.containsKeyChanges();
  }
}
