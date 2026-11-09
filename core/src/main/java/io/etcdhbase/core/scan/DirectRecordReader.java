package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** No cache or coalescing: each accepted read uses its bound, authorized backend port. */
public final class DirectRecordReader implements RecordReader {
  private final RecordStore store;
  private final BudgetManager.Account account;
  private final long maximumResultBytes;

  public DirectRecordReader(RecordStore store, BudgetManager.Account account, long maximumResultBytes) {
    this.store = Objects.requireNonNull(store, "store");
    this.account = Objects.requireNonNull(account, "account");
    if (maximumResultBytes < 1 || maximumResultBytes > account.reservation().output()) {
      throw new IllegalArgumentException("point result bound exceeds its reserved output capacity");
    }
    this.maximumResultBytes = maximumResultBytes;
  }

  @Override
  public CompletableFuture<PointResult> read(ByteKey key, long upper, ReadProjection projection) {
    return read(key, upper, projection, true);
  }

  /**
   * Retains an intermediate point result without ending its shared read view. The caller must
   * finish the store's read successfully after preparing all dependent data and before sealing
   * or exposing that data. The returned point result still owns its output allocation.
   */
  public CompletableFuture<PointResult> readForPreparation(
      ByteKey key, long upper, ReadProjection projection) {
    return read(key, upper, projection, false);
  }

  private CompletableFuture<PointResult> read(ByteKey key, long upper, ReadProjection projection,
      boolean finishRead) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(projection, "projection");
    if (upper <= 0) return CompletableFuture.failedFuture(new IllegalArgumentException("revision must be positive"));
    BudgetManager.Lease output;
    try {
      output = account.tryRetain(BudgetManager.Memory.OUTPUT, maximumResultBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    } catch (BackendFailure failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var owner = new CompletableFuture<PointResult>();
    var response = owner.copy();
    owner.thenAccept(actual -> response.whenComplete((delivered, failure) -> {
      // Cancellation, timeout or a caller-supplied fallback may win before the real result arrives.
      if (actual != delivered) actual.close();
    }));
    CompletableFuture<Optional<StoredRecord>> operation;
    try { operation = PointRead.get(store, key, upper, projection, account, finishRead); }
    catch (Error fatal) {
      throw settleFailureAfterCleanup(owner, fatal, output);
    }
    operation.whenComplete((record, failure) -> {
      if (failure != null) {
        Error fatal = settleFailureAfterCleanup(owner, failure, output);
        if (fatal != null) throw fatal;
        return;
      }
      PointResult result = null;
      try {
        validate(record, key, upper, projection);
        account.requireActive();
        result = new PointResult(record, output, account.scope());
        account.requireActive();
        owner.complete(result);
      } catch (BackendFailure | RuntimeException invalid) {
        Error fatal = settleFailureAfterCleanup(owner, invalid, cleanupFor(result, output));
        if (fatal != null) throw fatal;
      } catch (Error fatal) {
        throw settleFailureAfterCleanup(owner, fatal, cleanupFor(result, output));
      }
    });
    return response;
  }

  private static AutoCloseable cleanupFor(PointResult result, BudgetManager.Lease output) {
    return result == null ? output : result;
  }

  /** Settles the public operation with its original failure before propagating fatal cleanup errors. */
  private static Error settleFailureAfterCleanup(CompletableFuture<PointResult> owner, Throwable failure,
      AutoCloseable cleanup) {
    Throwable primary = unwrap(failure);
    Error fatal = primary instanceof Error error ? error : null;
    try {
      cleanup.close();
    } catch (Exception | Error cleanupFailure) {
      if (cleanupFailure != primary) primary.addSuppressed(cleanupFailure);
      if (fatal == null && cleanupFailure instanceof Error error) fatal = error;
    }
    owner.completeExceptionally(primary);
    return fatal;
  }

  private static Throwable unwrap(Throwable failure) {
    while (failure instanceof CompletionException && failure.getCause() != null) {
      Throwable cause = failure.getCause();
      transferSuppressed(failure, cause);
      failure = cause;
    }
    return failure;
  }

  // CompletionException is a transport wrapper; its cleanup diagnostics must survive unwrapping.
  private static void transferSuppressed(Throwable wrapper, Throwable cause) {
    for (Throwable suppressed : wrapper.getSuppressed()) {
      if (suppressed == cause) continue;
      boolean alreadyPresent = false;
      for (Throwable existing : cause.getSuppressed()) {
        if (existing == suppressed) {
          alreadyPresent = true;
          break;
        }
      }
      if (!alreadyPresent) cause.addSuppressed(suppressed);
    }
  }

  private void validate(Optional<StoredRecord> record, ByteKey key, long upper, ReadProjection projection) throws BackendFailure {
    if (record.isEmpty()) return;
    var value = record.get();
    if (value.retainedBytes() > maximumResultBytes) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    boolean keyOrRevisionMismatch =
        !value.key().equals(key) || value.metadata().modificationRevision() > upper;
    boolean metadataProjectionMismatch = projection == ReadProjection.METADATA && value.hasValue();
    boolean liveFullProjectionMismatch = projection == ReadProjection.FULL
        && value.metadata().state() == StorageMetadata.State.LIVE && !value.hasValue();
    boolean projectionMismatch = metadataProjectionMismatch || liveFullProjectionMismatch;
    if (keyOrRevisionMismatch || projectionMismatch) {
      throw new IllegalStateException("point result violates its requested version or projection");
    }
  }
}
