package io.etcdhbase.core.bytes;

import java.util.Arrays;
import java.util.Objects;

/** Immutable binary key ordered by unsigned bytes; no character decoding participates in order. */
public final class ByteKey implements Comparable<ByteKey> {
  private static final ByteKey EMPTY = new ByteKey(new byte[0]);
  private final byte[] bytes;
  private final int hash;

  private ByteKey(byte[] bytes) {
    this.bytes = bytes;
    this.hash = Arrays.hashCode(bytes);
  }

  public static ByteKey copyOf(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    return bytes.length == 0 ? EMPTY : new ByteKey(bytes.clone());
  }

  public static ByteKey empty() {
    return EMPTY;
  }

  public byte[] bytes() {
    return bytes.clone();
  }

  public int length() {
    return bytes.length;
  }

  @Override
  public int compareTo(ByteKey other) {
    return Arrays.compareUnsigned(bytes, other.bytes);
  }

  @Override
  public boolean equals(Object other) {
    return this == other || other instanceof ByteKey key && Arrays.equals(bytes, key.bytes);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return "ByteKey[length=" + bytes.length + "]";
  }
}
