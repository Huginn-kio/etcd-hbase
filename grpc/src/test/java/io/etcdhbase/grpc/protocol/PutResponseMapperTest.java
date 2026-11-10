package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PutResponseMapperTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff});

  @Test
  void projectsStableIdentityRevisionAndPresentEmptyPreviousValue() {
    var previous = StoredRecord.live(KEY,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    var response = PutResponseMapper.project(command(true), new PutResult(KEY, 3, Optional.of(previous)),
        decision(3, true), Long.MIN_VALUE, -1);
    assertEquals(Long.MIN_VALUE, response.getHeader().getClusterId());
    assertEquals(-1, response.getHeader().getMemberId());
    assertEquals(3, response.getHeader().getRevision());
    assertEquals(0, response.getHeader().getRaftTerm());
    assertTrue(response.hasPrevKv());
    assertArrayEquals(KEY.bytes(), response.getPrevKv().getKey().toByteArray());
    assertArrayEquals(new byte[0], response.getPrevKv().getValue().toByteArray());
    assertEquals(2, response.getPrevKv().getModRevision());
    assertEquals(2, response.getPrevKv().getVersion());
    assertFalse(PutResponseMapper.project(command(true), new PutResult(KEY, 3, Optional.empty()),
        decision(3, true), 1, 2).hasPrevKv());
  }

  @Test
  void rejectsNonmutatingShapeMismatchedRevisionKeyAndInventedIdentity() {
    PutResult result = new PutResult(KEY, 3, Optional.empty());
    assertThrows(IllegalArgumentException.class,
        () -> PutResponseMapper.project(command(true), result, decision(4, true), 1, 2));
    assertThrows(IllegalArgumentException.class,
        () -> PutResponseMapper.project(command(true), result, decision(3, false), 1, 2));
    assertThrows(IllegalArgumentException.class,
        () -> PutResponseMapper.project(command(true), result, decision(3, true), 0, 2));
    var other = new PutCommand(ByteKey.copyOf(new byte[] {'x'}), new byte[0], 0, true, false, false);
    assertThrows(IllegalArgumentException.class,
        () -> PutResponseMapper.project(other, result, decision(3, true), 1, 2));
    var previous = StoredRecord.live(KEY,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    assertThrows(IllegalArgumentException.class, () -> PutResponseMapper.project(command(false),
        new PutResult(KEY, 3, Optional.of(previous)), decision(3, true), 1, 2));
  }

  private static PutCommand command(boolean previous) {
    return new PutCommand(KEY, new byte[0], 0, previous, false, false);
  }

  private static CommitDecision decision(long revision, boolean changes) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), 1,
        changes ? revision - 1 : revision, revision, ByteKey.copyOf(new byte[] {1}),
        ByteKey.copyOf(new byte[32]), changes);
  }
}
