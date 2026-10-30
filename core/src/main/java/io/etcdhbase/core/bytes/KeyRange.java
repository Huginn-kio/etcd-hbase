package io.etcdhbase.core.bytes;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Explicit empty, point or half-open range. Missing bounds mean infinity, never a magic key. */
public final class KeyRange {
  public enum Kind { EMPTY, POINT, RANGE }

  private final Kind kind;
  private final Optional<ByteKey> lowerInclusive;
  private final Optional<ByteKey> upperExclusive;

  private KeyRange(Kind kind, Optional<ByteKey> lower, Optional<ByteKey> upper) {
    this.kind = kind;
    this.lowerInclusive = lower;
    this.upperExclusive = upper;
  }

  public static KeyRange empty() {
    return new KeyRange(Kind.EMPTY, Optional.empty(), Optional.empty());
  }

  public static KeyRange point(ByteKey key) {
    return new KeyRange(Kind.POINT, Optional.of(key), Optional.empty());
  }

  public static KeyRange all() {
    return interval(Optional.empty(), Optional.empty());
  }

  public static KeyRange between(ByteKey start, ByteKey end) {
    return interval(Optional.of(start), Optional.of(end));
  }

  public static KeyRange interval(Optional<ByteKey> start, Optional<ByteKey> end) {
    Objects.requireNonNull(start, "start");
    Objects.requireNonNull(end, "end");
    if (start.isPresent() && end.isPresent() && start.get().compareTo(end.get()) >= 0) {
      return empty();
    }
    return new KeyRange(Kind.RANGE, start, end);
  }

  public static KeyRange prefix(ByteKey prefix) {
    return interval(Optional.of(prefix), prefixEnd(prefix));
  }

  /** The least upper bound of all keys with this prefix, or positive infinity. */
  public static Optional<ByteKey> prefixEnd(ByteKey prefix) {
    byte[] upper = prefix.bytes();
    for (int index = upper.length - 1; index >= 0; index--) {
      if (upper[index] != (byte) 0xff) {
        upper[index]++;
        return Optional.of(ByteKey.copyOf(Arrays.copyOf(upper, index + 1)));
      }
    }
    return Optional.empty();
  }

  public Kind kind() {
    return kind;
  }

  public boolean isEmpty() {
    return kind == Kind.EMPTY;
  }

  public Optional<ByteKey> lowerInclusive() {
    return lowerInclusive;
  }

  public Optional<ByteKey> upperExclusive() {
    return upperExclusive;
  }

  public boolean contains(ByteKey key) {
    Objects.requireNonNull(key, "key");
    if (kind == Kind.EMPTY) return false;
    if (kind == Kind.POINT) return lowerInclusive.orElseThrow().equals(key);
    return lowerInclusive.map(lower -> lower.compareTo(key) <= 0).orElse(true)
        && upperExclusive.map(upper -> key.compareTo(upper) < 0).orElse(true);
  }
}
