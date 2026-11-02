package io.etcdhbase.core.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class SortedMergeCursorDeadlineTest {
  @Test
  void deadlineCrossingDuringFinalHeadCleanupRejectsSuccessAndReleasesOutput() throws Exception {
    var clock = new ControlledClock();
    var scope = new RequestScope(10, clock);
    var budget = budget();
    var source = new DelayedSource();
    try (scope; var account = budget.admit("public", scope, reservation());
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 1, account, scope)) {
      var result = cursor.read(1);
      source.pending.getFirst().complete(page(record(new byte[] {1}), false));
      assertEquals(2, source.pending.size(), "the first result requires a refill before proving More");

      // The fourth deadline read is the final success-boundary check after HEAD cleanup.
      clock.expireOnRead(clock.reads() + 4);
      source.pending.getLast().complete(page(record(new byte[] {2}), true));

      var failure = assertThrows(CompletionException.class, result::join);
      assertTrue(failure.getCause() instanceof BackendFailure);
      assertEquals(BackendFailure.Category.TIMEOUT, ((BackendFailure) failure.getCause()).category());
      assertFalse(scope.isClosed(), "deadline expiry does not require another thread to close the scope");
      assertEquals(0, budget.snapshot().allocatedBytes(), "the rejected RangePage must release its OUTPUT lease");
    }
  }

  @Test
  void sameMergeCompletesBeforeDeadlineAndTransfersPageOwnership() throws Exception {
    var clock = new ControlledClock();
    var scope = new RequestScope(10, clock);
    var budget = budget();
    var source = new DelayedSource();
    try (scope; var account = budget.admit("public", scope, reservation());
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 1, account, scope)) {
      var result = cursor.read(1);
      source.pending.getFirst().complete(page(record(new byte[] {1}), false));
      source.pending.getLast().complete(page(record(new byte[] {2}), true));

      var response = result.join();
      assertEquals(1, response.count());
      assertTrue(response.more());
      assertFalse(scope.isClosed());
      assertTrue(budget.snapshot().allocatedBytes() > 0);
      response.close();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  private static BudgetManager budget() {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    return new BudgetManager(limits, Map.of("public", limits), limits);
  }

  private static BudgetManager.Reservation reservation() {
    return new BudgetManager.Reservation(1024, 1024, 4096, 4096);
  }

  private static ScanRequest template() {
    return new ScanRequest(0, KeyRange.all(), Optional.empty(), Long.MAX_VALUE,
        ReadProjection.METADATA, 1, 1024, 32);
  }

  private static StoragePage page(StoredRecord record, boolean exhausted) {
    return new StoragePage(List.of(record), exhausted,
        new StoragePage.Cost(1, record.retainedBytes(), 1));
  }

  private static StoredRecord record(byte[] key) {
    var metadata = new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 0);
    return StoredRecord.metadataOnly(ByteKey.copyOf(key), metadata);
  }

  private static final class ControlledClock implements LongSupplier {
    private final AtomicInteger reads = new AtomicInteger();
    private volatile int expirationRead = Integer.MAX_VALUE;

    @Override
    public long getAsLong() {
      return reads.incrementAndGet() >= expirationRead ? 10 : 0;
    }

    int reads() { return reads.get(); }
    void expireOnRead(int read) { expirationRead = read; }
  }

  private static final class DelayedSource implements ScanSource {
    private final List<CompletableFuture<StoragePage>> pending = new ArrayList<>();

    @Override
    public ScanOperation startScan(ScanRequest request) {
      var completion = new CompletableFuture<StoragePage>();
      pending.add(completion);
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() { return completion.minimalCompletionStage(); }
        @Override public void close() {}
      };
    }
  }
}
