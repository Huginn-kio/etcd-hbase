package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanCleanupFailure;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Physical point-read accounting. Callers separately own any retained output allocation. */
final class PointRead {
  private PointRead() {}

  static CompletableFuture<Optional<StoredRecord>> get(RecordStore store, ByteKey key, long upper,
      ReadProjection projection, BudgetManager.Account account) {
    return get(store, key, upper, projection, account, true);
  }

  /** Allows a multi-key aggregate to defer its single final pin check until every value is held. */
  static CompletableFuture<Optional<StoredRecord>> get(RecordStore store, ByteKey key, long upper,
      ReadProjection projection, BudgetManager.Account account, boolean finishRead) {
    BudgetManager.Lease io = null;
    BudgetManager.Lease work = null;
    boolean scannerBacked = false;
    var result = new CompletableFuture<Optional<StoredRecord>>();
    try {
      io = account.tryRetain(BudgetManager.Memory.IO, account.reservation().io()).orElseThrow(PointRead::resourceLimit);
      scannerBacked = store.pointReadUsesScanner();
      int scanner = scannerBacked ? 1 : 0;
      work = account.tryAcquire(new BudgetManager.Work(0, 1, scanner, scanner, 0))
          .orElseThrow(PointRead::resourceLimit);
      account.chargePoint();
      var ownedIo = io;
      var ownedWork = work;
      boolean ownsScanner = scannerBacked;
      CompletableFuture<Optional<StoredRecord>> producer = store instanceof BudgetedRecordStore budgeted
          ? budgeted.get(key, upper, projection, account)
          : store.get(key, upper, projection);
      producer.whenComplete((record, failure) ->
          complete(result, record, failure, ownedWork, ownedIo, account, ownsScanner));
    } catch (BackendFailure | RuntimeException failure) {
      complete(result, null, failure, work, io, account, scannerBacked);
    } catch (Error fatal) {
      complete(result, null, fatal, work, io, account, scannerBacked);
      throw fatal;
    }
    // A dependent cancellation never settles or cancels the backend producer.
    CompletableFuture<Optional<StoredRecord>> read = result.copy();
    return finishRead
        ? read.thenCompose(record -> store.finishRead().thenApply(ignored -> record))
        : read;
  }

  private static void complete(CompletableFuture<Optional<StoredRecord>> result, Optional<StoredRecord> record,
      Throwable failure, BudgetManager.Lease work, BudgetManager.Lease io, BudgetManager.Account account,
      boolean scannerBacked) {
    Throwable terminal = failure;
    Error fatal = null;
    for (var lease : new BudgetManager.Lease[] {work, io}) {
      try {
        if (lease == work && work != null && scannerBacked && ScanCleanupFailure.isPresent(failure)) work.retire();
        else if (lease != null) lease.close();
      }
      catch (RuntimeException | Error cleanup) {
        if (terminal == null) terminal = cleanup;
        else if (terminal != cleanup) terminal.addSuppressed(cleanup);
        if (cleanup instanceof Error error) fatal = error;
      }
    }
    if (terminal == null) {
      try { account.requireActive(); }
      catch (BackendFailure inactive) { terminal = inactive; }
    }
    if (terminal != null) result.completeExceptionally(terminal);
    else if (record == null) result.completeExceptionally(new IllegalStateException("point read returned no projection"));
    else result.complete(record);
    if (fatal != null) throw fatal;
  }

  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
}
