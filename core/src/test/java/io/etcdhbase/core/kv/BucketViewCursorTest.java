package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
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

class BucketViewCursorTest {
  @Test
  void releasesEachPhysicalPageWhileRetainingTheReturnedHead() throws Exception {
    try (var fixture = new Fixture()) {
      try (var cursor = fixture.cursor()) {
        var first = cursor.next();
        fixture.complete(false, "a");
        assertEquals(key("a"), first.join().orElseThrow().key());
        assertTrue(fixture.operations.getFirst().closed);
        assertTrue(fixture.manager.snapshot().allocatedBytes() > 0);
        var second = cursor.next();
        assertEquals(Optional.of(key("a")), fixture.requests.getLast().after());
        fixture.complete(true, "b");
        assertEquals(key("b"), second.join().orElseThrow().key());
        assertTrue(fixture.operations.getLast().closed);
        assertTrue(cursor.next().join().isEmpty());
      }
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void closeCancelsThePhysicalReadAndRejectsALatePage() throws Exception {
    try (var fixture = new Fixture()) {
      var cursor = fixture.cursor();
      var result = cursor.next();
      cursor.close();
      assertTrue(fixture.operations.getFirst().closed);
      assertThrows(CompletionException.class, result::join);
      fixture.complete(true, "a");
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final BudgetManager.Limits limits = new BudgetManager.Limits(1, 0, 1, 1, 1, 0, 8192);
    private final BudgetManager manager = new BudgetManager(limits, Map.of("test", limits), limits);
    private final BudgetManager.Account account;
    private final List<ScanRequest> requests = new ArrayList<>();
    private final List<Operation> operations = new ArrayList<>();

    private Fixture() throws Exception {
      account = manager.admit("test", scope, new BudgetManager.Reservation(4096, 1, 1, 0));
    }

    private BucketViewCursor cursor() {
      ScanSource source = request -> {
        requests.add(request);
        var operation = new Operation();
        operations.add(operation);
        return operation;
      };
      var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 5,
          ReadProjection.METADATA, 1, 4096, 16);
      return new BucketViewCursor(source, request, account, scope);
    }

    private void complete(boolean exhausted, String value) {
      var record = StoredRecord.metadataOnly(key(value),
          new StorageMetadata(1, 5, 1, 0, StorageMetadata.State.LIVE, 1));
      operations.getLast().result.complete(new StoragePage(List.of(record), exhausted, new StoragePage.Cost(1, 1, 1)));
    }

    @Override public void close() {
      account.close();
      scope.close();
    }
  }

  private static final class Operation implements ScanOperation {
    private final CompletableFuture<StoragePage> result = new CompletableFuture<>();
    private boolean closed;
    @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }
    @Override public void close() { closed = true; }
  }

  private static ByteKey key(String value) {
    return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
  }
}
