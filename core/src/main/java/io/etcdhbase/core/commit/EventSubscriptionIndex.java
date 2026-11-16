package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Indexes active subscriptions by raw key or interval lower bound; caller serializes access. */
final class EventSubscriptionIndex<T> {
  private final Map<ByteKey, Set<T>> points = new HashMap<>();
  private final NavigableMap<ByteKey, Map<T, KeyRange>> ranges = new TreeMap<>();

  void add(T subscription, KeyRange range) {
    if (range.kind() == KeyRange.Kind.POINT) {
      points.computeIfAbsent(range.lowerInclusive().orElseThrow(), ignored -> new HashSet<>())
          .add(subscription);
    } else if (range.kind() == KeyRange.Kind.RANGE) {
      ranges.computeIfAbsent(range.lowerInclusive().orElse(ByteKey.empty()), ignored -> new HashMap<>())
          .put(subscription, range);
    }
  }

  void remove(T subscription, KeyRange range) {
    if (range.kind() == KeyRange.Kind.POINT) {
      ByteKey key = range.lowerInclusive().orElseThrow();
      Set<T> group = points.get(key);
      if (group != null && group.remove(subscription) && group.isEmpty()) points.remove(key);
    } else if (range.kind() == KeyRange.Kind.RANGE) {
      ByteKey lower = range.lowerInclusive().orElse(ByteKey.empty());
      Map<T, KeyRange> group = ranges.get(lower);
      if (group != null && group.remove(subscription) != null && group.isEmpty()) ranges.remove(lower);
    }
  }

  void forEachMatch(ByteKey key, Consumer<T> receiver) {
    Set<T> exact = points.get(key);
    if (exact != null) exact.forEach(receiver);
    for (Map<T, KeyRange> group : ranges.headMap(key, true).values()) {
      group.forEach((subscription, range) -> {
        if (range.contains(key)) receiver.accept(subscription);
      });
    }
  }
}
