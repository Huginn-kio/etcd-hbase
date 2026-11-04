package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import org.junit.jupiter.api.Test;

class CommitDecisionMessagesTest {
  @Test
  void roundTripsEveryImmutableDecisionField() {
    CommitDecision decision = new CommitDecision(TransactionId.create(), InvocationId.create(),
        7, 12, 13, ByteKey.copyOf(new byte[] {1, 2, 3}), digest(4), true);

    CommitDecision restored = CommitDecisionMessages.decode(CommitDecisionMessages.encode(decision));

    assertEquals(decision, restored);
  }

  @Test
  void rejectsMalformedOrEmptyPersistedDecisionBytes() {
    assertThrows(IllegalArgumentException.class, () -> CommitDecisionMessages.decode(new byte[0]));
    assertThrows(IllegalArgumentException.class,
        () -> CommitDecisionMessages.decode(new byte[] {0x7f}));
  }

  private static ByteKey digest(int marker) {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) marker;
    return ByteKey.copyOf(bytes);
  }
}
