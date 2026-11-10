package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TxnCommandTest {
  @Test
  void mutuallyExclusiveBranchesMayWriteTheSameKey() {
    var put = new TxnCommand.Put(command("a"));
    var txn = new TxnCommand(List.of(), List.of(put), List.of(put));
    assertTrue(txn.mayWrite());
    assertFalse(new TxnCommand(List.of(), List.of(new TxnCommand.PointRange(key("a"))),
        List.of()).mayWrite());
    assertTrue(new TxnCommand(List.of(), List.of(), List.of(new TxnCommand.Unsupported(true))).mayWrite());
    assertFalse(new TxnCommand(List.of(), List.of(new TxnCommand.Unsupported(false)), List.of()).mayWrite());
  }

  @Test
  void validatesBothBranchesEvenWhenOnlyOneWillBeSelected() {
    var put = new TxnCommand.Put(command("a"));
    KvFailure failure = assertThrows(KvFailure.class,
        () -> new TxnCommand(List.of(), List.of(), List.of(put, put)));
    assertEquals(KvFailure.Reason.DUPLICATE_KEY, failure.reason());
    KvFailure oversized = assertThrows(KvFailure.class,
        () -> new TxnCommand(List.of(), Collections.nCopies(129,
            new TxnCommand.PointRange(key("a"))), List.of()));
    assertEquals(KvFailure.Reason.TOO_MANY_OPS, oversized.reason());
    assertEquals(KvFailure.Reason.EMPTY_KEY, assertThrows(KvFailure.class,
        () -> new TxnCommand.PointRange(ByteKey.empty())).reason());
  }

  private static PutCommand command(String key) {
    return new PutCommand(key(key), new byte[] {1}, 0, false, false, false);
  }

  private static ByteKey key(String value) {
    return ByteKey.copyOf(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }
}
