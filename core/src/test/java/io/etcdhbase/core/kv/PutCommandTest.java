package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import org.junit.jupiter.api.Test;

class PutCommandTest {
  @Test
  void ownsKeyAndValueBytesAndKeepsEmptyValuePresent() {
    byte[] key = {0, (byte) 0xff};
    byte[] value = {0, (byte) 0x80};
    PutCommand command = new PutCommand(ByteKey.copyOf(key), value, 0, true, false, false);
    key[0] = 9;
    value[0] = 9;
    byte[] returned = command.value();
    returned[0] = 8;

    assertArrayEquals(new byte[] {0, (byte) 0xff}, command.key().bytes());
    assertArrayEquals(new byte[] {0, (byte) 0x80}, command.value());
    assertTrue(command.previousRequested());
    assertEquals(PutCommand.LeaseIntent.NONE, command.leaseIntent());
    assertEquals(0, new PutCommand(ByteKey.copyOf(new byte[] {0}), new byte[0],
        0, false, false, false).value().length);
  }

  @Test
  void validatesStaticErrorsInNativeOrder() {
    assertReason(KvFailure.Reason.EMPTY_KEY, ByteKey.empty(), new byte[] {1}, 1, true, true);
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    assertReason(KvFailure.Reason.VALUE_PROVIDED, key, new byte[] {1}, 1, true, true);
    assertReason(KvFailure.Reason.LEASE_PROVIDED, key, new byte[0], 1, true, true);
  }

  @Test
  void distinguishesPreservedExplicitAndAbsentLease() {
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    PutCommand preserve = new PutCommand(key, new byte[0], 0, false, true, true);
    PutCommand explicit = new PutCommand(key, new byte[0], Long.MIN_VALUE, false, false, false);

    assertTrue(preserve.ignoreValue());
    assertEquals(PutCommand.LeaseIntent.PRESERVE, preserve.leaseIntent());
    assertEquals(PutCommand.LeaseIntent.EXPLICIT, explicit.leaseIntent());
    assertEquals(Long.MIN_VALUE, explicit.leaseId());
  }

  private static void assertReason(KvFailure.Reason reason, ByteKey key, byte[] value,
      long leaseId, boolean ignoreValue, boolean ignoreLease) {
    KvFailure failure = assertThrows(KvFailure.class,
        () -> new PutCommand(key, value, leaseId, false, ignoreValue, ignoreLease));
    assertEquals(reason, failure.reason());
  }
}
