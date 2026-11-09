package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class OrderedViewCursorTest {
  @Test
  void mergesBucketsInByteOrderAndReleasesEveryHead() throws Exception {
    try (var fixture = new Fixture()) {
      try (var cursor = fixture.cursor(source("a", "c"), source("b", "d"))) {
        for (String key : List.of("a", "b", "c", "d")) {
          assertEquals(key(key), cursor.next().join().orElseThrow().key());
        }
        assertTrue(cursor.next().join().isEmpty());
      }
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void rejectsDuplicateKeysWithoutLeakingTheAlreadyLoadedHeads() throws Exception {
    try (var fixture = new Fixture()) {
      try (var cursor = fixture.cursor(source("a", "c"), source("b", "c"))) {
        assertEquals(key("a"), cursor.next().join().orElseThrow().key());
        assertEquals(key("b"), cursor.next().join().orElseThrow().key());
        assertEquals(key("c"), cursor.next().join().orElseThrow().key());
        assertThrows(CompletionException.class, () -> cursor.next().join());
      }
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void abandoningAFutureDoesNotReleaseAStillRunningSource() throws Exception {
    try (var fixture = new Fixture()) {
      var pending = new CompletableFuture<Optional<StoredRecord>>();
      try (var cursor = fixture.cursor(() -> pending)) {
        var response = cursor.next();
        assertTrue(response.cancel(false));
        assertThrows(CompletionException.class, () -> cursor.next().join());
        pending.complete(Optional.of(record("a")));
        assertTrue(fixture.manager.snapshot().allocatedBytes() > 0);
      }
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void closingWhileAReadIsPendingDiscardsTheLateHead() throws Exception {
    try (var fixture = new Fixture()) {
      var pending = new CompletableFuture<Optional<StoredRecord>>();
      var cursor = fixture.cursor(() -> pending);
      var result = cursor.next();
      cursor.close();
      assertFalse(result.isDone());
      pending.complete(Optional.of(record("a")));
      assertThrows(CompletionException.class, result::join);
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final BudgetManager.Limits limits = new BudgetManager.Limits(1, 0, 1, 1, 1, 0, 8192);
    private final BudgetManager manager = new BudgetManager(limits, Map.of("test", limits), limits);
    private final BudgetManager.Account account;

    private Fixture() throws Exception {
      account = manager.admit("test", scope, new BudgetManager.Reservation(4096, 1, 1, 0));
    }

    @SafeVarargs
    private final OrderedViewCursor cursor(AsyncCursor<StoredRecord>... sources) {
      return new OrderedViewCursor(List.of(sources), account, scope);
    }

    @Override public void close() {
      account.close();
      scope.close();
    }
  }

  private static AsyncCursor<StoredRecord> source(String... keys) {
    return AsyncCursor.fromIterator(java.util.Arrays.stream(keys).map(OrderedViewCursorTest::record).iterator());
  }

  private static StoredRecord record(String key) {
    return StoredRecord.metadataOnly(key(key), new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 1));
  }

  private static ByteKey key(String value) {
    return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
  }
}
