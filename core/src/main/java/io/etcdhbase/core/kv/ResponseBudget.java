package io.etcdhbase.core.kv;

/** Conservative accounting for decoding and projecting a retained logical response. */
public final class ResponseBudget {
  public static final long FIXED_BYTES = 64L << 10;
  private static final long COPY_ALLOWANCE = 8;

  private ResponseBudget() {}

  public static long forEncodedResult(long encodedBytes) {
    if (encodedBytes < 1) throw new IllegalArgumentException("result must not be empty");
    try {
      return Math.addExact(FIXED_BYTES, Math.multiplyExact(COPY_ALLOWANCE, encodedBytes));
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("result exceeds response accounting bounds", overflow);
    }
  }
}
