package io.etcdhbase.hbase.codec;

/** A valid logical key cannot fit in its encoded HBase row. */
public final class PhysicalRowLimitException extends IllegalArgumentException {
  public PhysicalRowLimitException() {
    super("encoded key exceeds the storage row limit");
  }
}
