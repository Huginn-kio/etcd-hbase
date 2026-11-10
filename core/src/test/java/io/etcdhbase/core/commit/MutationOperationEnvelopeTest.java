package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import org.junit.jupiter.api.Test;

class MutationOperationEnvelopeTest {
  @Test
  void distinguishesVersionedTxnFromExistingRawPutBytes() {
    var existingPut = ByteKey.copyOf(new byte[] {10, 1, 'a'});
    assertFalse(MutationOperationEnvelope.isTransaction(existingPut));
    byte[] payload = {0, (byte) 0xff, 1};
    ByteKey transaction = MutationOperationEnvelope.transaction(payload);
    payload[0] = 42;
    assertTrue(MutationOperationEnvelope.isTransaction(transaction));
    assertArrayEquals(new byte[] {0, (byte) 0xff, 1}, MutationOperationEnvelope.transactionBytes(transaction));
    ByteKey delete = MutationOperationEnvelope.deleteRange(new byte[] {10, 1, 'a'});
    assertEquals(MutationOperationEnvelope.Kind.DELETE_RANGE, MutationOperationEnvelope.kind(delete));
    assertFalse(MutationOperationEnvelope.isTransaction(delete));
    assertArrayEquals(new byte[] {10, 1, 'a'}, MutationOperationEnvelope.deleteRangeBytes(delete));
    assertThrows(IllegalArgumentException.class, () -> MutationOperationEnvelope.transactionBytes(delete));
  }

  @Test
  void malformedTaggedOrOversizeEnvelopesFailClosed() {
    assertThrows(IllegalArgumentException.class,
        () -> MutationOperationEnvelope.isTransaction(ByteKey.copyOf(new byte[] {0, 'T'})));
    assertThrows(IllegalArgumentException.class,
        () -> MutationOperationEnvelope.transactionBytes(ByteKey.copyOf(new byte[] {10, 1, 'a'})));
    assertThrows(IllegalArgumentException.class,
        () -> MutationOperationEnvelope.transaction(new byte[MutationForwardRequest.MAX_OPERATION_BYTES]));
  }
}
