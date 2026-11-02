package io.etcdhbase.core.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.scan.ExistsExecutor;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ExistsExecutorDeadlineTest {
  private static final ScanRequest REQUEST = new ScanRequest(0, KeyRange.all(), Optional.empty(),
      Long.MAX_VALUE, ReadProjection.METADATA, 1, 1024, 32);

  @Test
  void deadlineCrossingWhileClosingPositivePageCannotReturnOne() {
    var clock = new ManualRequestClock();
    var scope = clock.scope(Duration.ofSeconds(10));
    var source = new ControlledSource(page(List.of(live()), true),
        () -> clock.advance(Duration.ofSeconds(20)));
    try (scope; var executor = new ExistsExecutor(source, REQUEST, 1, scope)) {
      var failure = assertThrows(CompletionException.class, () -> executor.execute().join());
      assertCategory(failure, BackendFailure.Category.TIMEOUT);
    }
  }

  @Test
  void deadlineCrossingWhileClosingLastEmptyPageCannotReturnZero() {
    var clock = new ManualRequestClock();
    var scope = clock.scope(Duration.ofSeconds(10));
    var source = new ControlledSource(page(List.of(), true),
        () -> clock.advance(Duration.ofSeconds(20)));
    try (scope; var executor = new ExistsExecutor(source, REQUEST, 1, scope)) {
      var failure = assertThrows(CompletionException.class, () -> executor.execute().join());
      assertCategory(failure, BackendFailure.Category.TIMEOUT);
    }
  }

  @Test
  void explicitCloseDuringPositivePageCleanupCannotReturnOne() {
    var scope = RequestScope.withoutDeadline();
    var executorReference = new AtomicReference<ExistsExecutor>();
    var source = new ControlledSource(page(List.of(live()), true), () -> executorReference.get().close());
    try (scope; var executor = new ExistsExecutor(source, REQUEST, 1, scope)) {
      executorReference.set(executor);
      var failure = assertThrows(CompletionException.class, () -> executor.execute().join());
      assertCategory(failure, BackendFailure.Category.CANCELLED);
    }
  }

  @Test
  void scopeCancellationDuringLastPageCleanupCannotReturnZero() {
    var scope = new RequestScope(Duration.ofSeconds(10).toNanos(), () -> 0);
    var source = new ControlledSource(page(List.of(), true), scope::close);
    try (scope; var executor = new ExistsExecutor(source, REQUEST, 1, scope)) {
      var failure = assertThrows(CompletionException.class, () -> executor.execute().join());
      assertCategory(failure, BackendFailure.Category.CANCELLED);
      assertEquals(true, scope.isClosed());
    }
  }

  @Test
  void cleanupFailureRemainsPrimaryWhenDeadlineCrossesDuringCleanup() {
    var clock = new ManualRequestClock();
    var scope = clock.scope(Duration.ofSeconds(10));
    var cleanupFailure = new IllegalStateException("synthetic cleanup failure");
    var source = new ControlledSource(page(List.of(live()), true), () -> {
      clock.advance(Duration.ofSeconds(20));
      throw cleanupFailure;
    });
    try (scope; var executor = new ExistsExecutor(source, REQUEST, 1, scope)) {
      var failure = assertThrows(CompletionException.class, () -> executor.execute().join());
      assertSame(cleanupFailure, failure.getCause());
    }
  }

  private static void assertCategory(CompletionException failure, BackendFailure.Category expected) {
    var backend = assertInstanceOf(BackendFailure.class, failure.getCause());
    assertEquals(expected, backend.category());
  }

  private static StoragePage page(List<StoredRecord> records, boolean exhausted) {
    return new StoragePage(records, exhausted,
        new StoragePage.Cost(records.size(), records.stream().mapToLong(StoredRecord::retainedBytes).sum(), 1));
  }

  private static StoredRecord live() {
    return StoredRecord.live(ByteKey.copyOf(new byte[] {1}),
        new StorageMetadata(1, 7, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
  }

  private static final class ControlledSource implements ScanSource {
    private final StoragePage page;
    private final Runnable onClose;

    private ControlledSource(StoragePage page, Runnable onClose) {
      this.page = page;
      this.onClose = onClose;
    }

    @Override
    public ScanOperation startScan(ScanRequest request) {
      return new ScanOperation() {
        private boolean closed;

        @Override public CompletionStage<StoragePage> completion() {
          return CompletableFuture.completedFuture(page);
        }

        @Override public synchronized void close() {
          if (closed) return;
          closed = true;
          onClose.run();
        }
      };
    }
  }
}
