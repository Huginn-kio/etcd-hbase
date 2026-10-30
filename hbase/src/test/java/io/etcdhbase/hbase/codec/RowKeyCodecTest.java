package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.Test;

class RowKeyCodecTest {
  private final RowKeyCodec current = new RowKeyCodec(new HashSaltStrategy(), RowKeyCodec.Shape.CURRENT);
  private final RowKeyCodec history = new RowKeyCodec(new HashSaltStrategy(), RowKeyCodec.Shape.HISTORY);

  @Test
  void rejectsOversizedKeysBeforeCopyingForRowsAndPhysicalBounds() {
    ByteKey oversized = ByteKey.copyOf(new byte[20 * 1024 * 1024]);
    for (RowKeyCodec codec : List.of(current, history)) {
      EncodingAllocationAssertions.rejectsWithoutLargeCopy(() -> codec.encode(oversized, 1));
      EncodingAllocationAssertions.rejectsWithoutLargeCopy(
          () -> codec.physicalRange(0, KeyRange.point(oversized)));
    }
  }

  @Test
  void acceptsExactRowLimitAndRejectsRawLengthOrEscapingOverflow() {
    for (RowKeyCodec codec : List.of(current, history)) {
      int overhead = codec.saltProfile().saltWidthBytes() + 2
          + (codec.shape() == RowKeyCodec.Shape.HISTORY ? Long.BYTES : 0);
      byte[] original = new byte[HConstants.MAX_ROW_LENGTH - overhead];
      Arrays.fill(original, (byte) 1);
      assertEquals(HConstants.MAX_ROW_LENGTH, codec.encode(ByteKey.copyOf(original), 1).length);
      original[0] = 0;
      assertThrows(IllegalArgumentException.class, () -> codec.encode(ByteKey.copyOf(original), 1));
      byte[] tooLong = Arrays.copyOf(original, original.length + 1);
      assertThrows(IllegalArgumentException.class, () -> codec.encode(ByteKey.copyOf(tooLong), 1));
    }
  }

  @Test
  void providerWidthAndBucketChecksRemainAtEveryEncodingBoundary() {
    for (byte[] invalid : List.of(new byte[1], new byte[] {0, 16})) {
      SaltStrategy provider = new SaltStrategy() {
        private final SaltProfile profile = new SaltProfile("synthetic", 1, 16, 2);
        @Override public String id() { return profile.strategyId(); }
        @Override public SaltProfile profile() { return profile; }
        @Override public byte[] salt(byte[] identifier) { return invalid.clone(); }
        @Override public List<byte[]> splitKeys() { throw new UnsupportedOperationException(); }
      };
      var codec = new RowKeyCodec(provider, RowKeyCodec.Shape.CURRENT);
      assertThrows(IllegalArgumentException.class, () -> codec.bucket(ByteKey.empty()));
      assertThrows(IllegalArgumentException.class, () -> codec.encode(ByteKey.empty(), 1));
      assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[4]));
    }
  }

  @Test
  void matchesFixedVectorsAndRejectsMalformedOrWrongBucketRows() {
    assertArrayEquals(hex("0003610000"), current.encode(ByteKey.copyOf(hex("61")), 1));
    byte[] encoded = history.encode(ByteKey.copyOf(hex("6100ff")), Long.MAX_VALUE);
    assertEquals(ByteKey.copyOf(hex("6100ff")), history.decode(encoded).key());
    assertEquals(Long.MAX_VALUE, history.decode(encoded).revision().orElseThrow());
    for (String malformed : List.of("", "0003", "00036100", "0003610001", "0004610000", "000361000000")) {
      assertThrows(IllegalArgumentException.class, () -> current.decode(hex(malformed)));
    }
    assertThrows(IllegalArgumentException.class, () -> history.decode(current.encode(ByteKey.empty(), 1)));
    assertThrows(IllegalArgumentException.class, () -> history.encode(ByteKey.empty(), 0));
    assertThrows(IllegalArgumentException.class, () -> current.encode(ByteKey.copyOf(new byte[20_000]), 1));
  }

  @Test
  void preservesTheIndependentUnsignedOrderOfEveryShortKeyIncludingHistorySuffixes() {
    var singleBucket = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 1, 2));
    var codec = new RowKeyCodec(singleBucket, RowKeyCodec.Shape.HISTORY);
    List<byte[]> keys = new ArrayList<>();
    keys.add(new byte[0]);
    for (int first = 0; first < 256; first++) {
      keys.add(new byte[] {(byte) first});
      for (int second = 0; second < 256; second++) {
        keys.add(new byte[] {(byte) first, (byte) second});
      }
    }
    keys.sort(Arrays::compareUnsigned);
    byte[] previous = null;
    for (byte[] key : keys) {
      ByteKey logical = ByteKey.copyOf(key);
      byte[] lower = codec.encode(logical, 1);
      byte[] upper = codec.encode(logical, Long.MAX_VALUE);
      assertEquals(logical, codec.decode(upper).key());
      assertTrue(Arrays.compareUnsigned(lower, upper) < 0);
      if (previous != null) assertTrue(Arrays.compareUnsigned(previous, lower) < 0);
      previous = upper;
    }
  }

  @Test
  void physicalBoundsMatchLogicalBoundsAndNeverLeakIntoAnotherBucket() {
    for (int bucket = 0; bucket < 16; bucket++) {
      for (KeyRange logical : List.of(KeyRange.all(), KeyRange.prefix(ByteKey.copyOf(hex("61"))),
          KeyRange.prefix(ByteKey.copyOf(hex("ff"))), KeyRange.point(ByteKey.copyOf(hex("6100"))), KeyRange.empty())) {
        KeyRange physical = history.physicalRange(bucket, logical);
        for (String keyHex : List.of("", "00", "61", "6100", "6162", "62", "ff", "ff00")) {
          ByteKey key = ByteKey.copyOf(hex(keyHex));
          byte[] encoded = history.encode(key, Long.MAX_VALUE);
          boolean sameBucket = Arrays.equals(Arrays.copyOf(encoded, 2), new HashSaltStrategy().profile().encodeBucket(bucket));
          assertEquals(sameBucket && logical.contains(key), physical.contains(ByteKey.copyOf(encoded)));
        }
      }
    }
  }

  @Test
  void nativeTimestampMappingAvoidsTheLatestTimestampSentinel() {
    for (long revision : new long[] {1, 2, 256, Long.MAX_VALUE}) {
      assertEquals(revision, RevisionTimestamp.revision(RevisionTimestamp.timestamp(revision)));
      assertEquals(revision, RevisionTimestamp.exclusiveUpper(revision));
    }
    assertThrows(IllegalArgumentException.class, () -> RevisionTimestamp.timestamp(0));
    assertThrows(IllegalArgumentException.class, () -> RevisionTimestamp.revision(Long.MAX_VALUE));
  }

  private static byte[] hex(String value) {
    return HexFormat.of().parseHex(value);
  }
}
