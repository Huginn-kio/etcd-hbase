package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class LogicalViewCursorTest {
  @Test
  void mergesMetadataAcrossBucketsAtTheRequestedRevisionAndClosesPages() throws Exception {
    try (var fixture = new Fixture()) {
      try (var cursor = fixture.cursor(KeyRange.all(), 2)) {
        for (String expected : List.of("a", "b", "c", "d")) {
          var record = cursor.next().join().orElseThrow();
          assertEquals(key(expected), record.key());
          assertFalse(record.hasValue());
        }
        assertTrue(cursor.next().join().isEmpty());
      }
      assertTrue(fixture.requests.stream().allMatch(request -> request.revisionUpperBound() == 2
          && request.projection() == ReadProjection.METADATA));
      assertEquals(fixture.requests.size(), fixture.closedOperations);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void mergesDescendingMetadataAcrossBuckets() throws Exception {
    try (var fixture = new Fixture();
         var cursor = fixture.cursor(fixture::scan, KeyRange.all(), 2, LogicalViewCursor.Traversal.KEY_DESCENDING)) {
      for (String expected : List.of("d", "c", "b", "a")) {
        assertEquals(key(expected), cursor.next().join().orElseThrow().key());
      }
      assertTrue(cursor.next().join().isEmpty());
      assertTrue(fixture.requests.stream().allMatch(request -> request.direction() == ScanDirection.DESCENDING));
    }
  }

  @Test
  void pointReadsOnlyItsBucketAndEmptyRangesDoNotOpenScans() throws Exception {
    try (var fixture = new Fixture()) {
      try (var cursor = fixture.cursor(KeyRange.point(key("b")), 5)) {
        assertEquals(key("b"), cursor.next().join().orElseThrow().key());
        assertTrue(cursor.next().join().isEmpty());
      }
      assertTrue(fixture.requests.stream().allMatch(request -> request.bucket() == 1));
      int opened = fixture.requests.size();
      try (var cursor = fixture.cursor(KeyRange.empty(), 5)) {
        assertTrue(cursor.next().join().isEmpty());
      }
      assertEquals(opened, fixture.requests.size());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void unorderedExistenceCanStopBeforeOpeningOtherBuckets() throws Exception {
    try (var fixture = new Fixture();
         var cursor = fixture.cursor(fixture::scan, KeyRange.all(), 2, LogicalViewCursor.Traversal.ANY)) {
      assertEquals(key("a"), cursor.next().join().orElseThrow().key());
      assertEquals(List.of(0), fixture.requests.stream().map(ScanRequest::bucket).toList());
    }
  }

  @Test
  void unorderedAbsenceRequiresEveryBucket() throws Exception {
    try (var fixture = new Fixture();
         var cursor = fixture.cursor(fixture::scan, KeyRange.between(key("x"), key("z")), 2,
             LogicalViewCursor.Traversal.ANY)) {
      assertTrue(cursor.next().join().isEmpty());
      assertEquals(List.of(0, 1), fixture.requests.stream().map(ScanRequest::bucket).toList());
    }
  }

  @Test
  void rejectsAnInitialCheckpointUntilWorksetResumeIsSupported() throws Exception {
    try (var fixture = new Fixture()) {
      var request = new ScanRequest(0, KeyRange.all(), Optional.of(key("a")), 5,
          ReadProjection.METADATA, 1, 2048, 16);
      assertThrows(IllegalArgumentException.class, () -> new LogicalViewCursor(fixture::scan,
          Optional.empty(), request, 2, ignored -> 0, fixture.account, fixture.scope));
      assertTrue(fixture.requests.isEmpty());
    }
  }

  @Test
  void closeCancelsAnInFlightPageAndReleasesItsBudgetAfterCompletion() throws Exception {
    for (var traversal : LogicalViewCursor.Traversal.values()) {
      try (var fixture = new Fixture()) {
        var pending = new CompletableFuture<StoragePage>();
        var operation = new Operation(pending, () -> fixture.closedOperations++);
        ScanSource source = request -> operation;
        var cursor = fixture.cursor(source, KeyRange.point(key("a")), 5, traversal);
        var result = cursor.next();
        cursor.close();
        assertTrue(operation.closed);
        pending.complete(new StoragePage(List.of(record("a")), true, new StoragePage.Cost(1, 1, 1)));
        assertThrows(CompletionException.class, result::join);
        assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final BudgetManager.Limits limits = new BudgetManager.Limits(1, 0, 2, 2, 2, 0, 65536);
    private final BudgetManager budgets = new BudgetManager(limits, Map.of("test", limits), limits);
    private final BudgetManager.Account account;
    private final List<ScanRequest> requests = new ArrayList<>();
    private int closedOperations;

    private Fixture() throws Exception {
      account = budgets.admit("test", scope, new BudgetManager.Reservation(16384, 4096, 4096, 4096));
    }

    private LogicalViewCursor cursor(KeyRange range, long revision) {
      return cursor(this::scan, range, revision);
    }

    private LogicalViewCursor cursor(ScanSource source, KeyRange range, long revision) {
      return cursor(source, range, revision, LogicalViewCursor.Traversal.KEY_ASCENDING);
    }

    private LogicalViewCursor cursor(ScanSource source, KeyRange range, long revision,
        LogicalViewCursor.Traversal traversal) {
      var direction = traversal == LogicalViewCursor.Traversal.KEY_DESCENDING
          ? ScanDirection.DESCENDING : ScanDirection.ASCENDING;
      var request = new ScanRequest(0, range, Optional.empty(), revision, ReadProjection.METADATA, 1, 2048, 16, direction);
      return new LogicalViewCursor(source, Optional.empty(), request, 2,
          value -> value.equals(key("b")) || value.equals(key("d")) ? 1 : 0, account, scope, traversal);
    }

    private ScanOperation scan(ScanRequest request) {
      requests.add(request);
      List<String> bucket = request.bucket() == 0 ? List.of("a", "c") : List.of("b", "d");
      var remaining = bucket.stream().map(LogicalViewCursorTest::record)
          .filter(value -> request.range().contains(value.key()))
          .filter(value -> request.after().map(after -> request.direction().compare(value.key(), after) > 0).orElse(true))
          .sorted((left, right) -> request.direction().compare(left.key(), right.key())).toList();
      var page = new StoragePage(remaining.stream().limit(1).toList(), remaining.size() <= 1,
          new StoragePage.Cost(remaining.isEmpty() ? 0 : 1, 1, 1));
      return new Operation(CompletableFuture.completedFuture(page), () -> closedOperations++);
    }

    @Override public void close() { account.close(); scope.close(); }
  }

  private static final class Operation implements ScanOperation {
    private final CompletableFuture<StoragePage> result;
    private final Runnable onClose;
    private boolean closed;
    private Operation(CompletableFuture<StoragePage> result, Runnable onClose) {
      this.result = result;
      this.onClose = onClose;
    }
    @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }
    @Override public void close() { if (!closed) { closed = true; onClose.run(); } }
  }

  private static StoredRecord record(String value) {
    return StoredRecord.metadataOnly(key(value), new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.LIVE, 1));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8)); }
}
