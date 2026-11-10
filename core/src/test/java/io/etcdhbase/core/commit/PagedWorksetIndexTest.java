package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ScanDirection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class PagedWorksetIndexTest {
  @Test
  void pagesLargeOperationAndReadsOnlyCompletedBucketKeyOrder() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var index = index(identity(), records, scope);
      index.begin().join();
      var entries = new ArrayList<PagedWorksetIndex.EventRef>();
      for (int ordinal = 0; ordinal < 300; ordinal++) {
        int number = 299 - ordinal;
        entries.add(ref(number % 4, String.format("key-%04d", number), ordinal));
      }
      index.commitOperation(AsyncCursor.fromIterator(entries.iterator())).join();
      assertEquals(300, index.eventCount());
      assertTrue(records.maximumRecordBytes <= 1024);
      assertTrue(records.rows.size() > 2, "the index must be paged");
      assertEquals(42, index.find(1, key("key-0257")).join().orElseThrow().ordinal());
      assertTrue(index.find(0, key("absent")).join().isEmpty());
      var cursor = index.scan(1, KeyRange.between(key("key-0100"), key("key-0120")));
      var found = new ArrayList<String>();
      for (var next = cursor.next().join(); next.isPresent(); next = cursor.next().join()) {
        found.add(text(next.orElseThrow().key()));
      }
      assertEquals(List.of("key-0101", "key-0105", "key-0109", "key-0113", "key-0117"), found);
      var reverse = index.scan(1, KeyRange.between(key("key-0100"), key("key-0120")), ScanDirection.DESCENDING);
      var descending = new ArrayList<String>();
      for (var next = reverse.next().join(); next.isPresent(); next = reverse.next().join()) {
        descending.add(text(next.orElseThrow().key()));
      }
      assertEquals(List.of("key-0117", "key-0113", "key-0109", "key-0105", "key-0101"), descending);
      int beforeNarrowSeek = records.readCalls;
      var absent = index.scan(1, KeyRange.between(key("z"), key("zz")), ScanDirection.DESCENDING);
      assertTrue(absent.next().join().isEmpty());
      assertTrue(records.readCalls - beforeNarrowSeek <= 16,
          "a range beyond the indexed keys should prune directory pages; reads=" + (records.readCalls - beforeNarrowSeek));
      var all = index.scanAll();
      int count = 0;
      PagedWorksetIndex.EventRef previous = null;
      for (var next = all.next().join(); next.isPresent(); next = all.next().join()) {
        var current = next.orElseThrow();
        if (previous != null) {
          assertTrue(previous.bucket() < current.bucket() || previous.bucket() == current.bucket()
              && previous.key().compareTo(current.key()) < 0);
        }
        previous = current;
        count++;
      }
      assertEquals(300, count);
    }
  }

  @Test
  void incompleteOperationNeverChangesTheCommittedRootAndFailureFencesTheAttempt() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var index = index(identity(), records, scope);
      index.begin().join();
      index.commitOperation(AsyncCursor.fromIterator(List.of(ref(0, "a", 0)).iterator())).join();
      var pendingPage = new CompletableFuture<Optional<PagedWorksetIndex.EventRef>>();
      var calls = new int[] {0};
      var operation = index.commitOperation(() -> calls[0]++ == 0
          ? CompletableFuture.completedFuture(Optional.of(ref(0, "b", 1))) : pendingPage);
      assertFalse(operation.isDone());
      assertEquals(0, index.find(0, key("a")).join().orElseThrow().ordinal());
      assertTrue(index.find(0, key("b")).join().isEmpty());
      pendingPage.completeExceptionally(new IllegalStateException("page failed"));
      assertThrows(CompletionException.class, operation::join);
      assertThrows(CompletionException.class, () -> index.find(0, key("a")).join());
      assertThrows(CompletionException.class, () -> index.commitOperation(
          AsyncCursor.fromIterator(List.of(ref(0, "b", 1)).iterator())).join());
    }
  }

  @Test
  void detectsCorruptPagesAndDoesNotReuseAnotherTransaction() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var first = index(identity(), records, scope);
      first.begin().join();
      first.commitOperation(AsyncCursor.fromIterator(List.of(ref(0, "a", 0)).iterator())).join();
      var second = index(identity(), records, scope);
      second.begin().join();
      assertTrue(second.find(0, key("a")).join().isEmpty());
      records.corruptFirstPage();
      assertThrows(CompletionException.class, () -> first.find(0, key("a")).join());
    }
  }

  @Test
  void durableStartPreventsBlindReplayOfAnUnfinishedInvocation() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      ResultIdentity identity = identity();
      var first = index(identity, records, scope);
      first.begin().join();
      first.commitOperation(AsyncCursor.fromIterator(List.of(ref(0, "a", 0)).iterator())).join();
      var retried = index(identity, records, scope);
      assertThrows(CompletionException.class, () -> retried.begin().join());
      assertThrows(CompletionException.class, () -> retried.find(0, key("a")).join());
      assertEquals(0, first.find(0, key("a")).join().orElseThrow().ordinal());
    }
  }

  @Test
  void laterCompletedOperationReplacesOneKeyButKeepsAllEventOrdinals() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var index = index(identity(), records, scope);
      index.begin().join();
      var first = ref(0, "a", 0);
      var second = ref(0, "b", 1);
      var replacement = ref(0, "a", 2);
      index.commitOperation(AsyncCursor.fromIterator(List.of(first, second).iterator())).join();
      var before = index.scanAll();
      index.commitOperation(AsyncCursor.fromIterator(List.of(replacement).iterator())).join();
      assertEquals(2, index.entryCount());
      assertEquals(3, index.eventCount());
      assertEquals(2, index.find(0, key("a")).join().orElseThrow().ordinal());
      assertEquals(0, before.next().join().orElseThrow().ordinal(), "a cursor retains its completed checkpoint");
      ByteKey chain = ByteKey.copyOf(new byte[32]);
      for (var event : List.of(first, second, replacement)) chain = PagedWorksetIndex.advanceEventChain(chain, event);
      assertEquals(chain, index.eventChain());
    }
  }

  @Test
  void cancelledCursorFutureDoesNotReleaseItsInFlightReadOrSuppressFailure() throws Exception {
    MemoryRecords records = new MemoryRecords();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var index = index(identity(), records, scope);
      index.begin().join();
      index.commitOperation(AsyncCursor.fromIterator(List.of(ref(0, "a", 0), ref(0, "b", 1)).iterator())).join();
      var cursor = index.scanAll();
      records.blockNextRead();
      var abandoned = cursor.next();
      assertFalse(abandoned.isDone());
      assertTrue(abandoned.cancel(false));
      assertThrows(CompletionException.class, () -> cursor.next().join());
      records.finishBlockedRead(false);
      assertEquals(key("b"), cursor.next().join().orElseThrow().key());
      assertTrue(cursor.next().join().isEmpty());

      records.blockNextRead();
      var abandonedFind = index.find(0, key("a"));
      assertTrue(abandonedFind.cancel(false));
      records.finishBlockedRead(true);
      assertThrows(CompletionException.class, () -> index.find(0, key("a")).join());
    }
  }

  private static PagedWorksetIndex.EventRef ref(int bucket, String key, long ordinal) {
    return new PagedWorksetIndex.EventRef(bucket, key(key), ordinal, ByteKey.copyOf(new byte[32]));
  }

  private static ResultIdentity identity() {
    return new ResultIdentity(InvocationId.create(), TransactionId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), 1, 0, 1);
  }

  private static PagedWorksetIndex index(ResultIdentity identity, MemoryRecords records, RequestScope scope)
      throws Exception {
    var limits = new BudgetManager.Limits(1, 0, 1, 1, 1, 0, 1L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var account = manager.admit("public", scope, new BudgetManager.Reservation(0, 1, 1, 0, 128 * 1024));
    return new PagedWorksetIndex(identity, records, scope, account, 1024);
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8)); }
  private static String text(ByteKey value) { return new String(value.bytes(), StandardCharsets.UTF_8); }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private int maximumRecordBytes;
    private int readCalls;
    private CompletableFuture<Optional<ByteKey>> blockedRead;
    private ByteKey blockedIdentity;

    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      readCalls++;
      if (blockedRead != null) {
        blockedIdentity = identity;
        return blockedRead;
      }
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(identity)));
    }
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      maximumRecordBytes = Math.max(maximumRecordBytes, value.length());
      ByteKey existing = rows.putIfAbsent(identity, value);
      return CompletableFuture.completedFuture(existing == null ? CreateResult.CREATED
          : existing.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
    private void corruptFirstPage() {
      var entry = rows.entrySet().stream().filter(row -> row.getKey().length() == 29).findFirst().orElseThrow();
      byte[] bytes = entry.getValue().bytes();
      bytes[bytes.length - 1] ^= 1;
      entry.setValue(ByteKey.copyOf(bytes));
    }
    private void blockNextRead() {
      blockedRead = new CompletableFuture<>();
      blockedIdentity = null;
    }
    private void finishBlockedRead(boolean fail) {
      var pending = blockedRead;
      ByteKey identity = blockedIdentity;
      blockedRead = null;
      blockedIdentity = null;
      if (fail) pending.completeExceptionally(new IllegalStateException("delayed page read failed"));
      else pending.complete(Optional.ofNullable(rows.get(identity)));
    }
  }
}
