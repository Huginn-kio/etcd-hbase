package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class NotSelectedProofTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final InvocationId INVOCATION = new InvocationId(
      UUID.fromString("00000000-0000-0000-0000-000000000001"));
  private static final TransactionId ATTEMPT = new TransactionId(
      UUID.fromString("00000000-0000-0000-0000-000000000002"));
  private static final TransactionId COMPETITOR = new TransactionId(
      UUID.fromString("00000000-0000-0000-0000-000000000003"));

  @Test
  void laterSequenceCannotStandInForTheAttemptProposedDecision() {
    assertThrows(IllegalArgumentException.class, () -> new NotSelectedProof(3, 7, 4, 9,
        Optional.of(new NotSelectedProof.SelectedDecision(9, COMPETITOR)),
        NotSelectedProof.Reason.DIFFERENT_TRANSACTION_SELECTED));
  }

  @Test
  void archivedDifferentTransactionMustBeAtBasePlusOneEvenAfterSequenceAdvances() {
    var proof = new NotSelectedProof(3, 7, 4, 9,
        Optional.of(new NotSelectedProof.SelectedDecision(8, COMPETITOR)),
        NotSelectedProof.Reason.DIFFERENT_TRANSACTION_SELECTED);
    var binding = InvocationBinding.begin(
        INVOCATION, DIGEST, DIGEST, ATTEMPT, 3, 7, 11);

    assertTrue(binding.notSelected(proof).state() == InvocationBinding.State.NOT_SELECTED);
  }

  @Test
  void anAttemptCannotBeMarkedNotSelectedByTheSameTransaction() {
    var proof = new NotSelectedProof(3, 7, 3, 8,
        Optional.of(new NotSelectedProof.SelectedDecision(8, ATTEMPT)),
        NotSelectedProof.Reason.DIFFERENT_TRANSACTION_SELECTED);
    var binding = InvocationBinding.begin(
        INVOCATION, DIGEST, DIGEST, ATTEMPT, 3, 7, 11);

    assertFalse(proof.proves(binding));
  }
}
