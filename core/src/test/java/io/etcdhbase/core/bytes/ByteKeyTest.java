package io.etcdhbase.core.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteKeyTest {
  @Test
  void ownsBytesAndUsesUnsignedContentOrder() {
    byte[] input = {0, (byte) 0xff};
    ByteKey key = ByteKey.copyOf(input);
    input[0] = 9;
    byte[] exported = key.bytes();
    exported[0] = 8;
    assertArrayEquals(new byte[] {0, (byte) 0xff}, key.bytes());
    assertEquals(key, ByteKey.copyOf(new byte[] {0, (byte) 0xff}));
    assertEquals(key.hashCode(), ByteKey.copyOf(key.bytes()).hashCode());
    assertTrue(ByteKey.copyOf(new byte[] {0x7f}).compareTo(ByteKey.copyOf(new byte[] {(byte) 0x80})) < 0);
    assertTrue(ByteKey.copyOf(new byte[] {0}).compareTo(key) < 0);
    assertEquals(0, ByteKey.empty().length());
  }
}
