package io.etcdhbase.core.commit;

import java.util.Objects;
import java.util.Optional;

/** Internal evidence that an attempt can no longer occupy its proposed sequence. */
public final class NotSelectedProof {
  public enum Reason { DIFFERENT_TRANSACTION_SELECTED, OLD_OWNER_FENCED_WITH_EMPTY_SLOT }

  private final long preparedOwnerEpoch;
  private final long baseSequence;
  private final long observedOwnerEpoch;
  private final long observedPublishedSequence;
  private final Optional<TransactionId> selectedTransaction;
  private final Reason reason;

  NotSelectedProof(long preparedOwnerEpoch, long baseSequence, long observedOwnerEpoch,
      long observedPublishedSequence, Optional<TransactionId> selectedTransaction, Reason reason) {
    this.preparedOwnerEpoch = preparedOwnerEpoch;
    this.baseSequence = baseSequence;
    this.observedOwnerEpoch = observedOwnerEpoch;
    this.observedPublishedSequence = observedPublishedSequence;
    this.selectedTransaction = Objects.requireNonNull(selectedTransaction, "selectedTransaction");
    this.reason = Objects.requireNonNull(reason, "reason");
    if (preparedOwnerEpoch <= 0 || baseSequence < 0 || observedOwnerEpoch < preparedOwnerEpoch
        || observedPublishedSequence < baseSequence) {
      throw new IllegalArgumentException("invalid not-selected evidence");
    }
    if (reason == Reason.DIFFERENT_TRANSACTION_SELECTED
        && (observedPublishedSequence < Math.addExact(baseSequence, 1)
            || selectedTransaction.isEmpty())) {
      throw new IllegalArgumentException("missing competing selection evidence");
    }
    if (reason == Reason.OLD_OWNER_FENCED_WITH_EMPTY_SLOT
        && (observedPublishedSequence != baseSequence || observedOwnerEpoch <= preparedOwnerEpoch
            || selectedTransaction.isPresent())) {
      throw new IllegalArgumentException("old owner was not durably fenced");
    }
  }

  boolean proves(InvocationBinding binding) {
    return preparedOwnerEpoch == binding.preparedOwnerEpoch()
        && baseSequence == binding.baseSequence()
        && (selectedTransaction.isEmpty()
            || !selectedTransaction.orElseThrow().equals(binding.transactionId()));
  }
}
