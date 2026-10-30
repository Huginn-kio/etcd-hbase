package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class ExistsExecutorTest {
  @Test
  void aPositiveCanStopBeforeOtherBucketsAndDoesNotFetchAnyValues() throws Exception {
    var live = live(2, 8192);
    var source = new FrozenScanSource(List.of(List.of(SortedMergeCursorTest.record(new byte[] {1}, true)), List.of(live), List.of(live(3, 8192))));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var executor = new ExistsExecutor(scheduler.bind(source, account), SortedMergeCursorTest.template(), 3, scope)) {
      assertEquals(1, executor.execute().join().count());
      assertEquals(2, source.requests.size());
      assertTrue(source.requests.stream().allMatch(request -> request.projection() == ReadProjection.METADATA));
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void aNegativeRequiresAllBucketsAndErrorsNeverBecomeZero() throws Exception {
    var source = new FrozenScanSource(List.of(List.of(), List.of(SortedMergeCursorTest.record(new byte[] {1}, true)), List.of()));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var executor = new ExistsExecutor(scheduler.bind(source, account), SortedMergeCursorTest.template(), 3, scope)) {
      assertEquals(0, executor.execute().join().count());
      assertEquals(3, source.requests.size());
    }
    ScanSource failing = request -> new ScanOperation() {
      @Override public CompletionStage<StoragePage> completion() { return CompletableFuture.failedStage(new IllegalStateException("synthetic")); }
      @Override public void close() {}
    };
    try (var scope = RequestScope.withoutDeadline(); var executor = new ExistsExecutor(failing, SortedMergeCursorTest.template(), 3, scope)) {
      assertThrows(CompletionException.class, () -> executor.execute().join());
    }
  }

  @Test
  void pointExistenceReadsMetadataAtTheRequestedRevisionAndReturnsOnlyZeroOrOne() throws Exception {
    var key = ByteKey.copyOf(new byte[] {1});
    var store = new FrozenRecordStore(List.of(List.of(live(1, 8192))));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096))) {
      assertEquals(1, ExistsExecutor.point(store, key, 7, account).join());
      assertEquals(List.of(new FrozenRecordStore.Get(key, 7, ReadProjection.METADATA)), store.gets);
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertEquals(0, ExistsExecutor.point(store, ByteKey.empty(), 7, account).join());
    }
  }

  static StoredRecord live(int key, int bytes) {
    return StoredRecord.live(ByteKey.copyOf(new byte[] {(byte) key}),
        new StorageMetadata(1, 7, 2, 0, StorageMetadata.State.LIVE, bytes), new byte[bytes]);
  }
}
