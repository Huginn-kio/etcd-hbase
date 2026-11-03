package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.Test;

class SystemRecordKeyCodecTest {
  private static final SystemRecordKeyCodec CODEC = new SystemRecordKeyCodec(
      new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)));
  private static final int KEY_OVERHEAD = 2 + 1 + Integer.BYTES;

  @Test
  void binaryIdentityRoundTripsForEveryRecordDomain() {
    ByteKey identity = ByteKey.copyOf(new byte[] {0, 1, (byte) 0xff, 0});
    for (SystemRecordType type : SystemRecordType.values()) {
      var decoded = CODEC.decode(CODEC.encode(type, identity));
      assertEquals(type, decoded.type());
      assertEquals(identity, decoded.identity());
    }
  }

  @Test
  void enforcesIdentityAndRowLengthBoundaries() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.encode(SystemRecordType.CONTROL, ByteKey.empty()));

    byte[] maximumIdentity = new byte[HConstants.MAX_ROW_LENGTH - KEY_OVERHEAD];
    byte[] maximumRow = CODEC.encode(SystemRecordType.CONTROL, ByteKey.copyOf(maximumIdentity));
    assertEquals(HConstants.MAX_ROW_LENGTH, maximumRow.length);
    assertArrayEquals(maximumIdentity,
        CODEC.decode(maximumRow).identity().bytes());
    assertThrows(IllegalArgumentException.class, () -> CODEC.encode(SystemRecordType.CONTROL,
        ByteKey.copyOf(new byte[maximumIdentity.length + 1])));
  }

  @Test
  void rejectsUnknownTypesInvalidSaltAndMalformedLengths() {
    byte[] valid = CODEC.encode(SystemRecordType.CONTROL, ByteKey.copyOf(new byte[] {7}));

    byte[] invalidSalt = valid.clone();
    invalidSalt[0] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(invalidSalt));

    byte[] unknownType = valid.clone();
    unknownType[2] = (byte) 0xff;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(unknownType));

    byte[] invalidLength = valid.clone();
    invalidLength[3] = 0;
    invalidLength[4] = 0;
    invalidLength[5] = 0;
    invalidLength[6] = 2;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(invalidLength));
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(new byte[7]));
  }
}
