package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SequentialViewCursorTest {
  @Test
  void exhaustsTheMaximumBucketCountWithoutRecursionOverflow() {
    var reads = new AtomicInteger();
    AsyncCursor<StoredRecord> empty = () -> {
      reads.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.empty());
    };
    try (var scope = RequestScope.withoutDeadline();
         var cursor = new SequentialViewCursor(Collections.nCopies(4096, empty), scope)) {
      assertTrue(cursor.next().join().isEmpty());
      assertEquals(4096, reads.get());
      assertTrue(cursor.next().join().isEmpty());
      assertEquals(4096, reads.get());
    }
  }

  @Test
  void closingDuringAReadRejectsLateResultsAndConcurrentReads() {
    var pending = new CompletableFuture<Optional<StoredRecord>>();
    try (var scope = RequestScope.withoutDeadline();
         var cursor = new SequentialViewCursor(List.of(() -> pending), scope)) {
      var first = cursor.next();
      assertFalse(first.isDone());
      assertInstanceOf(IllegalStateException.class,
          assertThrows(CompletionException.class, () -> cursor.next().join()).getCause());
      cursor.close();
      pending.complete(Optional.of(live()));
      var failure = assertInstanceOf(BackendFailure.class,
          assertThrows(CompletionException.class, first::join).getCause());
      assertEquals(BackendFailure.Category.CANCELLED, failure.category());
    }
  }

  @Test
  void backendFailureIsNotInterpretedAsAnEmptyBucket() {
    var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    var laterReads = new AtomicInteger();
    AsyncCursor<StoredRecord> broken = () -> CompletableFuture.failedFuture(failure);
    AsyncCursor<StoredRecord> later = () -> {
      laterReads.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.of(live()));
    };
    try (var scope = RequestScope.withoutDeadline();
         var cursor = new SequentialViewCursor(List.of(broken, later), scope)) {
      assertSame(failure, assertThrows(CompletionException.class, () -> cursor.next().join()).getCause());
      assertEquals(0, laterReads.get());
      assertThrows(CompletionException.class, () -> cursor.next().join());
    }
  }

  private static StoredRecord live() {
    return StoredRecord.metadataOnly(ByteKey.copyOf(new byte[] {1}),
        new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 1));
  }
}
