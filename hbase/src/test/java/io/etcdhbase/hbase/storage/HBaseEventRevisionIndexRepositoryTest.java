package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.EventRevisionIndexMessages;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class HBaseEventRevisionIndexRepositoryTest {
  @Test
  void readsAndCreatesAnImmutableRevisionRecord() {
    SystemRecordStore records = mock(SystemRecordStore.class);
    EventRevisionIndex index = index();
    ByteKey encoded = ByteKey.copyOf(EventRevisionIndexMessages.encode(index));
    when(records.read(eq(SystemRecordType.EVENT_REVISION_INDEX), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(encoded)));
    when(records.createIfAbsent(eq(SystemRecordType.EVENT_REVISION_INDEX), any(), eq(encoded), any()))
        .thenReturn(CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED));
    var repository = new HBaseEventRevisionIndexRepository(records);

    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertEquals(Optional.of(index), repository.read(9, scope).join());
      assertEquals(SystemRecordStore.CreateResult.CREATED, repository.createIfAbsent(index, scope).join());
      assertThrows(CompletionException.class, () -> repository.read(10, scope).join());
      assertThrows(IllegalArgumentException.class, () -> repository.read(0, scope));
    }
  }

  @Test
  void propagatesConflictsAndDoesNotInventMissingIndexRecords() {
    SystemRecordStore records = mock(SystemRecordStore.class);
    when(records.read(eq(SystemRecordType.EVENT_REVISION_INDEX), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(records.createIfAbsent(eq(SystemRecordType.EVENT_REVISION_INDEX), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CONFLICT));
    var repository = new HBaseEventRevisionIndexRepository(records);

    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertEquals(Optional.empty(), repository.read(9, scope).join());
      assertEquals(SystemRecordStore.CreateResult.CONFLICT, repository.createIfAbsent(index(), scope).join());
    }
  }

  private static EventRevisionIndex index() {
    ByteKey identity = ByteKey.copyOf(new byte[] {1});
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    return new EventRevisionIndex(new CommitDecision(TransactionId.create(), InvocationId.create(),
        4, 8, 9, identity, digest, true), new EventReference(identity, digest, 0, 0, 1, 8, 1, 0));
  }
}
