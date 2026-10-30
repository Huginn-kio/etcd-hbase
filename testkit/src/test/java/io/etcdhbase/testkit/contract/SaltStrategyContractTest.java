package io.etcdhbase.testkit.contract;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SaltStrategyContractTest {
  @Test
  void builtInAndExternalStrategiesShareOwnershipAndBoundaryContracts() {
    for (SaltStrategy strategy : List.of(new HashSaltStrategy(), new ExternalParitySalt())) {
      byte[] identifier = {0x00, (byte) 0xff, 0x61};
      byte[] original = identifier.clone();
      byte[] first = strategy.salt(identifier);
      assertArrayEquals(original, identifier, "strategy mutated the complete logical identifier");
      assertEquals(strategy.id(), strategy.profile().strategyId());
      assertEquals(strategy.profile().saltWidthBytes(), first.length);
      byte[] expected = first.clone();
      Arrays.fill(first, (byte) 0x7f);
      assertArrayEquals(
          expected, strategy.salt(identifier), "returned salt aliases provider state");
      List<byte[]> boundaries = strategy.splitKeys();
      assertEquals(strategy.profile().bucketCount() - 1, boundaries.size());
      for (int bucket = 1; bucket < strategy.profile().bucketCount(); bucket++) {
        assertArrayEquals(strategy.profile().encodeBucket(bucket), boundaries.get(bucket - 1));
        if (bucket > 1)
          assertTrue(
              Arrays.compareUnsigned(boundaries.get(bucket - 2), boundaries.get(bucket - 1)) < 0);
      }
      byte[] boundary = boundaries.getFirst().clone();
      Arrays.fill(boundaries.getFirst(), (byte) 0x7f);
      assertArrayEquals(
          boundary, strategy.splitKeys().getFirst(), "split arrays alias provider state");
    }
  }

  /**
   * This strategy only demonstrates third-party compilation; runtime config still selects CRC32.
   */
  private static final class ExternalParitySalt implements SaltStrategy {
    private static final SaltProfile PROFILE = new SaltProfile("fixture-parity-v1", 1, 2, 1);

    public String id() {
      return PROFILE.strategyId();
    }

    public SaltProfile profile() {
      return PROFILE;
    }

    public byte[] salt(byte[] identifier) {
      int parity = 0;
      for (byte value : identifier) parity ^= value & 1;
      return PROFILE.encodeBucket(parity);
    }

    public List<byte[]> splitKeys() {
      return List.of(PROFILE.encodeBucket(1));
    }
  }
}
