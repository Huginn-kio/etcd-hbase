package io.etcdhbase.spi.salt;

import java.util.Objects;
import java.util.zip.CRC32;

/** Public deterministic candidate that maps a complete logical identifier to a CRC32 bucket. */
public final class Crc32BucketSaltStrategy implements SaltStrategy {
  public static final String STRATEGY_ID = "crc32-bucket-v1";

  @Override
  public String strategyId() {
    return STRATEGY_ID;
  }

  @Override
  public byte[] prefixFor(byte[] logicalIdentifier, SaltParameters parameters) {
    Objects.requireNonNull(logicalIdentifier, "logicalIdentifier");
    Objects.requireNonNull(parameters, "parameters");
    if (logicalIdentifier.length == 0) {
      throw new IllegalArgumentException("logicalIdentifier must not be empty");
    }
    if (parameters.encodingVersion() != 1) {
      throw new IllegalArgumentException("crc32-bucket-v1 only supports encoding version 1");
    }

    CRC32 crc32 = new CRC32();
    crc32.update(logicalIdentifier);
    int bucketNumber = (int) (crc32.getValue() % parameters.bucketCount());
    return parameters.encodeBucket(bucketNumber);
  }
}
