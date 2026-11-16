package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EventSubscriptionIndexTest {
  @Test
  void matchesRawPointsAndHalfOpenRangesAndRemovesSubscriptions() {
    var index = new EventSubscriptionIndex<String>();
    ByteKey zero = key(0);
    ByteKey high = key(0xff);
    KeyRange point = KeyRange.point(zero);
    KeyRange bounded = KeyRange.between(zero, high);
    KeyRange fromHigh = KeyRange.interval(Optional.of(high), Optional.empty());
    KeyRange all = KeyRange.all();
    index.add("point", point);
    index.add("bounded", bounded);
    index.add("from-high", fromHigh);
    index.add("all", all);
    index.add("empty", KeyRange.empty());

    assertEquals(Set.of("point", "bounded", "all"), matches(index, zero));
    assertEquals(Set.of("bounded", "all"), matches(index, key(1)));
    assertEquals(Set.of("from-high", "all"), matches(index, high));
    assertEquals(Set.of("from-high", "all"), matches(index, key(0xff, 0)));
    index.remove("point", point);
    index.remove("bounded", bounded);
    index.remove("all", all);
    assertEquals(Set.of(), matches(index, zero));
    assertEquals(Set.of("from-high"), matches(index, high));
    index.remove("from-high", fromHigh);
    assertEquals(Set.of(), matches(index, high));
  }

  private static Set<String> matches(EventSubscriptionIndex<String> index, ByteKey key) {
    Set<String> matches = new HashSet<>();
    index.forEachMatch(key, matches::add);
    return matches;
  }

  private static ByteKey key(int... bytes) {
    byte[] value = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) value[i] = (byte) bytes[i];
    return ByteKey.copyOf(value);
  }
}
