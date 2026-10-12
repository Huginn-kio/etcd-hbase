package io.etcdhbase.spi.salt;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Crc32BucketSaltStrategyTest {
  private final Crc32BucketSaltStrategy strategy = new Crc32BucketSaltStrategy();

  @Test
  void mapsCompleteLogicalIdentifierToFixedWidthBigEndianBucket() {
    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SaltParameters parameters = new SaltParameters(1, 16, 1);

    byte[] prefix = strategy.prefixFor(key, parameters);

    assertArrayEquals(new byte[] {0x09}, prefix);
    assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), key);
  }

  @Test
  void supportsMultiByteBigEndianPrefixes() {
    SaltParameters parameters = new SaltParameters(1, 300, 2);

    assertArrayEquals(new byte[] {0x00, 0x09}, parameters.encodeBucket(9));
    assertArrayEquals(new byte[] {0x01, 0x2b}, parameters.encodeBucket(299));
  }

  @Test
  void returnsNewPrefixAndRejectsInvalidInputs() {
    SaltParameters parameters = new SaltParameters(1, 16, 1);

    byte[] first = strategy.prefixFor(new byte[] {1, 2, 3}, parameters);
    byte[] second = strategy.prefixFor(new byte[] {1, 2, 3}, parameters);

    assertNotSame(first, second);
    assertThrows(IllegalArgumentException.class, () -> strategy.prefixFor(new byte[0], parameters));
    assertThrows(IllegalArgumentException.class, () -> new SaltParameters(1, 257, 1));
    assertThrows(IllegalArgumentException.class, () -> parameters.encodeBucket(16));
  }
}
