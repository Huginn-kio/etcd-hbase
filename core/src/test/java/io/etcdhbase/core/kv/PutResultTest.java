package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PutResultTest {
  @Test
  void keepsPreviousPresenceAndAcceptsAnEmptyPreviousValue() {
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    StoredRecord previous = StoredRecord.live(key,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    PutResult result = new PutResult(key, 3, Optional.of(previous));

    assertEquals(0, result.previous().orElseThrow().value().orElseThrow().length);
    assertEquals(Optional.empty(), new PutResult(key, 3, Optional.empty()).previous());
  }

  @Test
  void rejectsMissingValueWrongKeyAndFuturePreviousVersion() {
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    StorageMetadata old = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 1);
    StorageMetadata future = new StorageMetadata(1, 4, 4, 0, StorageMetadata.State.LIVE, 1);
    assertThrows(IllegalArgumentException.class,
        () -> new PutResult(key, 3, Optional.of(StoredRecord.metadataOnly(key, old))));
    assertThrows(IllegalArgumentException.class,
        () -> new PutResult(key, 3, Optional.of(StoredRecord.live(
            ByteKey.copyOf(new byte[] {'x'}), old, new byte[] {1}))));
    assertThrows(IllegalArgumentException.class,
        () -> new PutResult(key, 3, Optional.of(StoredRecord.live(key, future, new byte[] {1}))));
  }

  @Test
  void permitsPreviousRecordFromAnEarlierOperationInTheSameTransaction() {
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    StorageMetadata metadata = new StorageMetadata(1, 3, 2, 0, StorageMetadata.State.LIVE, 1);
    StoredRecord previous = StoredRecord.live(key, metadata, new byte[] {1});

    assertEquals(3, new PutResult(key, 3, Optional.of(previous)).revision());
  }
}
