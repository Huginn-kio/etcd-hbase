package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.proto.StorageProtos;
import java.util.HexFormat;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageMessagesTest {
  @Test
  void roundTripsEmptyLiveValuesAndDistinguishesTombstonesAndProjection() {
    var key = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
    var live = StoredRecord.live(key, new StorageMetadata(1, 3, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    var decoded = StorageMessages.decode(key, StorageMessages.metadata(live.metadata()), Optional.of(StorageMessages.value(live)));
    assertArrayEquals(HexFormat.of().parseHex("0801100118032002280032020800"), StorageMessages.metadata(live.metadata()));
    assertArrayEquals(HexFormat.of().parseHex("080110031a00"), StorageMessages.value(live));
    assertEquals(live.metadata(), decoded.metadata());
    assertArrayEquals(new byte[0], decoded.value().orElseThrow());
    assertFalse(StorageMessages.decode(key, StorageMessages.metadata(live.metadata()), Optional.empty()).hasValue());
    var tombstone = new StorageMetadata(1, 4, 2, 0, StorageMetadata.State.TOMBSTONE, 0);
    assertEquals(tombstone, StorageMessages.decode(key, StorageMessages.metadata(tombstone), Optional.empty()).metadata());
  }

  @Test
  void rejectsUnknownMissingCorruptAndMismatchedVersions() {
    ByteKey key = ByteKey.empty();
    var metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 1);
    byte[] encoded = StorageMessages.metadata(metadata);
    var other = StoredRecord.live(key, new StorageMetadata(1, 3, 3, 0, StorageMetadata.State.LIVE, 1), new byte[] {7});
    assertThrows(IllegalArgumentException.class, () -> StorageMessages.decode(key, encoded, Optional.of(StorageMessages.value(other))));
    assertThrows(IllegalArgumentException.class, () -> StorageMessages.decode(key, new byte[0], Optional.empty()));
    assertThrows(IllegalArgumentException.class, () -> StorageMessages.decode(key, new byte[] {(byte) 0xff}, Optional.empty()));
    byte[] unknown = StorageProtos.RecordMetadata.newBuilder().setFormatVersion(99).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> StorageMessages.decode(key, unknown, Optional.empty()));
    assertThrows(IllegalArgumentException.class, () -> new StorageMetadata(3, 2, 1, 0, StorageMetadata.State.LIVE, 1));
    assertThrows(IllegalArgumentException.class, () -> StoredRecord.live(key, metadata, new byte[0]));
  }
}
