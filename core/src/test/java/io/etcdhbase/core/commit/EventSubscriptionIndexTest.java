package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
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

  @Test
  void replacingRegistrationWithEmptyRangeRemovesPreviousPointOrInterval() {
    var index = new EventSubscriptionIndex<String>();
    ByteKey pointKey = key(1);
    KeyRange point = KeyRange.point(pointKey);
    KeyRange range = KeyRange.between(key(2), key(4));
    index.add("point", point);
    index.add("range", range);

    index.add("point", KeyRange.empty());
    index.add("range", KeyRange.empty());

    assertEquals(Set.of(), matches(index, pointKey));
    assertEquals(Set.of(), matches(index, key(3)));
  }

  @Test
  void prunesDisjointIntervalsWithoutScanningEveryLowerBound() {
    var index = new EventSubscriptionIndex<String>();
    int intervalCount = 16_384;
    for (int interval = 0; interval < intervalCount; interval++) {
      index.add("range-" + interval,
          KeyRange.between(intKey(interval * 2), intKey(interval * 2 + 1)));
    }

    Set<String> matches = new HashSet<>();
    int visited = index.forEachMatch(intKey(intervalCount - 1), matches::add);

    assertEquals(Set.of(), matches, "the probe key lies between all indexed intervals");
    assertTrue(visited < 64,
        "the augmented balanced index must prune disjoint subtrees, visits=" + visited);
  }

  @Test
  void returnsAllOverlappingIntervalsAcrossRotationsAndRemovals() {
    var index = new EventSubscriptionIndex<String>();
    Map<String, KeyRange> registrations = new LinkedHashMap<>();
    for (int interval = 0; interval < 1_000; interval++) {
      String subscription = "range-" + interval;
      KeyRange range = interval(interval);
      registrations.put(subscription, range);
      index.add(subscription, range);
    }

    assertMatchesEveryProbe(index, registrations);
    for (var registration : registrations.entrySet()) {
      if (Integer.parseInt(registration.getKey().substring("range-".length())) % 3 == 0) {
        index.remove(registration.getKey(), registration.getValue());
        registration.setValue(KeyRange.empty());
      }
    }
    assertMatchesEveryProbe(index, registrations);
  }

  private static Set<String> matches(EventSubscriptionIndex<String> index, ByteKey key) {
    Set<String> matches = new HashSet<>();
    index.forEachMatch(key, matches::add);
    return matches;
  }

  private static void assertMatchesEveryProbe(EventSubscriptionIndex<String> index,
      Map<String, KeyRange> registrations) {
    for (int probe = 0; probe <= 150; probe++) {
      ByteKey key = intKey(probe);
      Set<String> expected = new HashSet<>();
      registrations.forEach((subscription, range) -> {
        if (range.contains(key)) expected.add(subscription);
      });
      assertEquals(expected, matches(index, key), "probe=" + probe);
    }
  }

  private static KeyRange interval(int index) {
    if (index % 61 == 0) return KeyRange.all();
    if (index % 37 == 0) {
      return KeyRange.interval(Optional.of(intKey(index % 100)), Optional.empty());
    }
    if (index % 31 == 0) {
      return KeyRange.interval(Optional.empty(), Optional.of(intKey(30 + index % 50)));
    }
    int lower = index % 101;
    return KeyRange.between(intKey(lower), intKey(lower + index % 13 + 1));
  }

  private static ByteKey key(int... bytes) {
    byte[] value = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) value[i] = (byte) bytes[i];
    return ByteKey.copyOf(value);
  }

  private static ByteKey intKey(int value) {
    return ByteKey.copyOf(new byte[] {
        (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
    });
  }
}
