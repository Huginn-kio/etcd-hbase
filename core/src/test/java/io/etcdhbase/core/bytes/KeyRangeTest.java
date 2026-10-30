package io.etcdhbase.core.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class KeyRangeTest {
  private static ByteKey key(String hex) {
    return ByteKey.copyOf(HexFormat.of().parseHex(hex));
  }

  @Test
  void prefixesHandleCarryAndInfiniteUpperBounds() {
    KeyRange range = KeyRange.prefix(key("12ff"));
    assertEquals(key("13"), range.upperExclusive().orElseThrow());
    assertTrue(range.contains(key("12ff00")));
    assertFalse(range.contains(key("13")));
    assertTrue(KeyRange.prefix(key("ffff")).upperExclusive().isEmpty());
    assertTrue(KeyRange.prefix(ByteKey.empty()).contains(key("ff00")));
  }

  @Test
  void emptyPointAndUnboundedRangesStayDistinct() {
    assertTrue(KeyRange.between(key("61"), key("61")).isEmpty());
    assertTrue(KeyRange.between(key("62"), key("61")).isEmpty());
    assertTrue(KeyRange.point(key("61")).contains(key("61")));
    assertFalse(KeyRange.point(key("61")).contains(key("6100")));
    assertFalse(KeyRange.empty().contains(ByteKey.empty()));
    assertTrue(KeyRange.all().contains(ByteKey.empty()));
  }
}
