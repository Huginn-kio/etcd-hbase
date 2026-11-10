package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.PhysicalScanProgress;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanCleanupFailure;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.hbase.error.HBaseFailures;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/** Finite physical pages. Callback threads never block on ResultScanner.next(). */
final class HBasePageReader {
  static final long MAX_RECORD_BYTES = StorageMessages.MAX_VALUE_BYTES + 2L * Short.MAX_VALUE + 8192;
  private final HBaseRecordStore store;
  private final StorageTableBinding binding;
  private final RequestScope scope;
  private final Executor worker;

  HBasePageReader(HBaseRecordStore store, StorageTableBinding binding, RequestScope scope, Executor worker) {
    this.store = store;
    this.binding = binding;
    this.scope = scope;
    this.worker = worker;
  }

  CompletableFuture<StoragePage> read(ScanRequest request) {
    return start(request).completion().toCompletableFuture();
  }

  ScanOperation start(ScanRequest request) {
    var task = new PageTask(request);
    scope.trackPhysicalProducer(task.cleanupComplete);
    task.registration = scope.registerScopedCleanup(task::closeResources, task.cleanupComplete);
    try {
      worker.execute(task);
    } catch (RejectedExecutionException rejected) {
      task.finish(null, new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
    // Caller cancellation cannot complete the authoritative cleanup future early.
    return task;
  }

  private final class PageTask implements Runnable, ScanOperation {
    private final ScanRequest request;
    private final CompletableFuture<StoragePage> outcome = new CompletableFuture<>();
    private RequestScope.ScopedCleanup registration;
    private ResultScanner scanner;
    private boolean started;
    private boolean cancelled;
    private boolean completing;
    private final CompletableFuture<Void> ownedScannerCloseComplete = new CompletableFuture<>();
    private final CompletableFuture<Void> executionComplete = new CompletableFuture<>();
    private final CompletableFuture<Void> cleanupComplete =
        CompletableFuture.allOf(ownedScannerCloseComplete, executionComplete);

    PageTask(ScanRequest request) { this.request = request; }

    @Override
    public CompletionStage<StoragePage> completion() { return outcome.minimalCompletionStage(); }

    @Override
    public void run() {
      synchronized (this) {
        if (cancelled) return;
        started = true;
      }
      StoragePage page = null;
      Throwable failure = null;
      try {
        checkActive();
        KeyRange physical = physicalRange();
        if (physical.isEmpty()) page = new StoragePage(List.of(), true, new StoragePage.Cost(0, 0, 0));
        else {
          ResultScanner opened = store.table().getScanner(scan(physical));
          boolean closeLateScanner;
          synchronized (this) {
            closeLateScanner = cancelled;
            if (!closeLateScanner) scanner = opened;
          }
          if (closeLateScanner) closeLateScanner(opened);
          checkActive();
          page = collect(opened);
        }
      } catch (Exception problem) {
        failure = problem;
      } catch (Error fatal) {
        failure = fatal;
        throw fatal;
      } finally {
        finish(page, failure);
      }
    }

    private void closeLateScanner(ResultScanner opened) {
      try {
        opened.close();
      } catch (RuntimeException | Error failure) {
        executionComplete.completeExceptionally(failure);
        throw failure;
      }
    }

    private KeyRange physicalRange() {
      KeyRange physical = binding.codec().physicalRange(request.bucket(), request.range());
      if (physical.isEmpty() || request.after().isEmpty()) return physical;
      KeyRange afterGroup = binding.codec().physicalRange(request.bucket(), KeyRange.point(request.after().get()));
      if (request.direction() == ScanDirection.DESCENDING) {
        ByteKey resume = afterGroup.lowerInclusive().orElseThrow();
        Optional<ByteKey> upper = physical.upperExclusive();
        return KeyRange.interval(physical.lowerInclusive(),
            Optional.of(upper.isEmpty() || resume.compareTo(upper.orElseThrow()) < 0 ? resume : upper.orElseThrow()));
      }
      ByteKey resume = afterGroup.upperExclusive().orElseThrow();
      ByteKey lower = physical.lowerInclusive().orElseThrow();
      return KeyRange.interval(Optional.of(resume.compareTo(lower) > 0 ? resume : lower), physical.upperExclusive());
    }

    private Scan scan(KeyRange physical) throws java.io.IOException {
      var scan = new Scan().setCaching(1).setMaxResultSize(request.maximumBytes())
          .setAllowPartialResults(false).setScanMetricsEnabled(true)
          .setLimit(request.maximumPhysicalRows() + 1).addColumn(binding.family(), HBaseRecordStore.METADATA);
      configureDirection(scan, physical);
      if (request.physicalProgress().isPresent()) {
        var progress = request.physicalProgress().orElseThrow();
        validatePhysicalProgress(progress, physical);
        scan.withStartRow(progress.lastPhysicalRow().bytes(), false);
      }
      if (binding.layout() != StorageTableBinding.Layout.HISTORY) scan.setTimeRange(0, request.revisionUpperBound());
      if (request.projection() == ReadProjection.FULL) scan.addColumn(binding.family(), HBaseRecordStore.VALUE);
      return scan;
    }

    private void configureDirection(Scan scan, KeyRange physical) {
      if (request.direction() == ScanDirection.DESCENDING) {
        scan.setReversed(true);
        physical.upperExclusive().ifPresent(upper -> scan.withStartRow(upper.bytes(), false));
        physical.lowerInclusive().ifPresent(lower -> scan.withStopRow(lower.bytes(), true));
      } else {
        physical.lowerInclusive().ifPresent(lower -> scan.withStartRow(lower.bytes(), true));
        physical.upperExclusive().ifPresent(upper -> scan.withStopRow(upper.bytes(), false));
      }
    }

    private void validatePhysicalProgress(PhysicalScanProgress progress, KeyRange physical) {
      ByteKey row = progress.lastPhysicalRow();
      var decoded = binding.codec().decode(row.bytes());
      boolean wrongSource = request.direction() != ScanDirection.DESCENDING
          || binding.layout() != StorageTableBinding.Layout.HISTORY || !physical.contains(row)
          || binding.codec().bucket(decoded.key()) != request.bucket()
          || !progress.physicalSourceId().equals(Optional.of(store.scanSourceId()));
      if (wrongSource || decoded.revision().isEmpty()
          || decoded.revision().getAsLong() <= request.revisionUpperBound()) {
        throw new IllegalArgumentException("invalid history physical progress");
      }
    }

    private StoragePage collect(ResultScanner opened) throws Exception {
      return new PageCollector(opened).collect();
    }

    private final class PageCollector {
      private final ResultScanner scanner;
      private final List<StoredRecord> records = new ArrayList<>();
      private StoredRecord candidate;
      private ByteKey group;
      private long retained;
      private long physicalRows;
      private long cellBytes;

      private PageCollector(ResultScanner scanner) { this.scanner = scanner; }

      private StoragePage collect() throws Exception {
        Result row;
        while ((row = scanner.next()) != null) {
          checkActive();
          account(row);
          StoragePage completed = accept(row);
          if (completed != null) return completed;
        }
        checkActive();
        if (candidate != null && !append(records, candidate, retained)) return finish(false);
        return finish(true);
      }

      private void account(Result row) throws BackendFailure {
        physicalRows++;
        for (var cell : row.rawCells()) {
          cellBytes += KeyValueUtil.length(cell.getRowLength(), cell.getFamilyLength(),
              cell.getQualifierLength(), cell.getValueLength(), cell.getTagsLength(), true);
        }
        if (physicalRows > request.maximumPhysicalRows()
            || cellBytes > 4 * request.maximumBytes() + MAX_RECORD_BYTES) throw resourceLimit();
      }

      private StoragePage accept(Result row) throws Exception {
        var decoded = binding.codec().decode(row.getRow());
        if (group != null && !group.equals(decoded.key()) && candidate != null) {
          if (!append(records, candidate, retained)) return finish(false);
          retained += candidate.retainedBytes();
          if (records.size() == request.maximumRows()) return finish(false);
          candidate = null;
        }
        group = decoded.key();
        if (request.after().isPresent() && request.direction().compare(group, request.after().get()) <= 0) return null;
        if (decoded.revision().isPresent() && decoded.revision().getAsLong() > request.revisionUpperBound()) {
          return acceptFuture(row);
        }
        if (candidate == null || request.direction() == ScanDirection.ASCENDING) {
          candidate = store.decode(row, request.projection());
        }
        if (atReversePhysicalLimit()) {
          append(records, candidate, retained);
          return finish(false);
        }
        return null;
      }

      private StoragePage acceptFuture(Result row) throws BackendFailure {
        if (!atReversePhysicalLimit()) return null;
        if (!records.isEmpty()) return finish(false);
        if (row.getRow().length > request.maximumBytes()) throw resourceLimit();
        var progress = PhysicalScanProgress.after(request, ByteKey.copyOf(row.getRow()), store.scanSourceId());
        return page(records, false, scanner, physicalRows, cellBytes, Optional.of(progress));
      }

      private boolean atReversePhysicalLimit() {
        return request.direction() == ScanDirection.DESCENDING && physicalRows == request.maximumPhysicalRows();
      }

      private StoragePage finish(boolean exhausted) {
        return page(records, exhausted, scanner, physicalRows, cellBytes);
      }
    }

    private boolean append(List<StoredRecord> records, StoredRecord record, long retained) throws BackendFailure {
      if (record.retainedBytes() > request.maximumBytes()) throw resourceLimit();
      if (retained + record.retainedBytes() > request.maximumBytes()) return false;
      records.add(record);
      return true;
    }

    private StoragePage page(List<StoredRecord> records, boolean exhausted, ResultScanner opened, long rows, long bytes) {
      return page(records, exhausted, opened, rows, bytes, Optional.empty());
    }

    private StoragePage page(List<StoredRecord> records, boolean exhausted, ResultScanner opened, long rows, long bytes,
        Optional<PhysicalScanProgress> progress) {
      var metrics = opened.getScanMetrics();
      var observed = Map.of(StoragePage.Observation.SERVER_ROWS_SCANNED, metrics.countOfRowsScanned.get(),
          StoragePage.Observation.BLOCK_BYTES_SCANNED, metrics.countOfBlockBytesScanned.get(),
          StoragePage.Observation.FS_BYTES, metrics.bytesReadFromFs.get(),
          StoragePage.Observation.BLOCK_CACHE_BYTES, metrics.bytesReadFromBlockCache.get(),
          StoragePage.Observation.MEMSTORE_BYTES, metrics.bytesReadFromMemstore.get(),
          StoragePage.Observation.SCANNER_RETRIES, metrics.countOfRPCRetries.get());
      return new StoragePage(records, exhausted, new StoragePage.Cost(rows, bytes, metrics.countOfRPCcalls.get(), observed), progress);
    }

    private void checkActive() throws TimeoutException {
      if (scope.isDeadlineExceeded()) throw new TimeoutException();
      synchronized (this) {
        if (cancelled || scope.isClosed()) throw new CancellationException();
      }
    }

    private void finish(StoragePage page, Throwable original) {
      boolean cancelledBeforeCompletion;
      synchronized (this) {
        cancelledBeforeCompletion = cancelled;
        completing = true;
      }
      Throwable failure = original;
      Error fatalCleanup = null;
      try {
        registration.close();
      } catch (Exception cleanupFailure) {
        if (failure == null) failure = cleanupFailure;
        else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      } catch (Error fatal) {
        fatalCleanup = fatal;
        if (failure == null) failure = fatal;
        else if (failure != fatal) failure.addSuppressed(fatal);
      }
      Throwable operationFailure = failure;
      executionComplete.complete(null);
      // A scope may already be closing the scanner on another thread. Completion follows the
      // actual close, so a scheduler cannot release its physical permit while close still runs.
      cleanupComplete.whenComplete((ignored, cleanupFailure) -> {
        if (cleanupFailure instanceof CompletionException wrapped && wrapped.getCause() != null) {
          cleanupFailure = wrapped.getCause();
        }
        Throwable terminal = operationFailure;
        if (terminal == null) terminal = cleanupFailure;
        else if (cleanupFailure != null && terminal != cleanupFailure) terminal.addSuppressed(cleanupFailure);
        if (terminal == null && scope.isDeadlineExceeded()) terminal = new TimeoutException();
        if (terminal == null && (cancelledBeforeCompletion || scope.isClosed())) terminal = new CancellationException();
        if (terminal == null) outcome.complete(page);
        else {
          BackendFailure classified = HBaseFailures.classify(terminal);
          // Retire failed cleanup capacity without replacing the original operation's cause.
          if (cleanupFailure != null) classified.addSuppressed(new ScanCleanupFailure(cleanupFailure));
          outcome.completeExceptionally(classified);
        }
      });
      if (fatalCleanup != null) throw fatalCleanup;
    }

    @Override
    public void close() {
      try {
        registration.close();
      } catch (RuntimeException failure) {
        throw failure;
      } catch (Exception failure) {
        throw new IllegalStateException("scan registration cleanup failed", failure);
      }
    }

    private void closeResources() {
      ResultScanner owned;
      boolean beforeStart;
      synchronized (this) {
        cancelled = true;
        owned = scanner;
        scanner = null;
        beforeStart = !started && !completing;
      }
      try {
        if (owned != null) owned.close();
        ownedScannerCloseComplete.complete(null);
      } catch (RuntimeException | Error failure) {
        ownedScannerCloseComplete.completeExceptionally(failure);
        throw failure;
      }
      if (beforeStart) {
        executionComplete.complete(null);
        outcome.completeExceptionally(HBaseFailures.classify(new CancellationException()));
      }
    }
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
