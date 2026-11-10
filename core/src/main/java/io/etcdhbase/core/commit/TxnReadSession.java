package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.BudgetedRecordStore;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.ToIntFunction;

/** Owns all pinned read views derived from a single transaction control barrier. */
public final class TxnReadSession implements AutoCloseable {
  private enum State { ACTIVE, FINISHING, FINISHED, CLOSED }
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("txn-read-cleanup").start(task);

  private final RequestScope scope;
  private final ByteKey authorizationScopeDigest;
  private final long currentRevision;
  private final long barrierSequence;
  private final boolean readyForMutation;
  private final Map<Long, ReadViewRecordStore> stores;
  private final Map<Long, RecordStore> readers;
  private State state = State.ACTIVE;
  private CompletableFuture<Void> finished;

  private TxnReadSession(ReadViewService.TransactionViews captured, RecordStore physical,
      ManifestCatalog manifests, ToIntFunction<ByteKey> bucketSelector, int bucketCount,
      ByteKey authorizationScopeDigest, RequestScope scope) {
    this.scope = scope;
    this.authorizationScopeDigest = authorizationScopeDigest;
    this.currentRevision = captured.currentRevision();
    this.barrierSequence = captured.barrierSequence();
    ReadView current = Objects.requireNonNull(captured.views().get(currentRevision),
        "current transaction view").view();
    this.readyForMutation = current.selectedDecision().isEmpty()
        && current.finalizedSequence() == current.barrierSequence();
    Map<Long, ReadViewRecordStore> opened = new LinkedHashMap<>();
    Map<Long, RecordStore> exposed = new LinkedHashMap<>();
    for (var entry : captured.views().entrySet()) {
      var store = new ReadViewRecordStore(entry.getValue(), physical, manifests,
          bucketSelector, bucketCount, scope);
      opened.put(entry.getKey(), store);
      exposed.put(entry.getKey(), new SessionReader(store));
    }
    stores = Map.copyOf(opened);
    readers = Map.copyOf(exposed);
  }

  /** Revision 0 denotes the captured current revision; all requested targets share one barrier. */
  public static CompletableFuture<TxnReadSession> open(ReadViewService service, List<Long> revisions,
      ByteKey authorizationScopeDigest, RecordStore physical, ManifestCatalog manifests,
      ToIntFunction<ByteKey> bucketSelector, int bucketCount, RequestScope scope) {
    Objects.requireNonNull(service, "service");
    Objects.requireNonNull(revisions, "revisions");
    Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
    Objects.requireNonNull(physical, "physical");
    Objects.requireNonNull(manifests, "manifests");
    Objects.requireNonNull(bucketSelector, "bucketSelector");
    Objects.requireNonNull(scope, "scope");
    if (bucketCount <= 0 || bucketCount > 4096) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid transaction bucket count"));
    }
    return service.openForTransaction(revisions, authorizationScopeDigest, scope).thenApply(captured -> {
      try {
        var session = new TxnReadSession(captured, physical, manifests, bucketSelector, bucketCount,
            authorizationScopeDigest, scope);
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          throw new java.util.concurrent.CompletionException(inactive(scope));
        }
        return session;
      } catch (RuntimeException | Error failure) {
        try { scope.cancelAsync(CLEANUP); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        throw failure;
      }
    });
  }

  public long currentRevision() { return currentRevision; }
  public long barrierSequence() { return barrierSequence; }
  public boolean belongsTo(RequestScope expectedScope) { return scope == expectedScope; }
  public boolean authorizes(ByteKey expectedDigest) { return authorizationScopeDigest.equals(expectedDigest); }
  public boolean readyForMutation() { return readyForMutation; }

  /** Child readers may complete their own operations but cannot release a transaction pin. */
  public synchronized RecordStore reader(long revision) {
    if (state != State.ACTIVE || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new IllegalStateException("transaction read session is not active");
    }
    RecordStore reader = readers.get(revision == 0 ? currentRevision : revision);
    if (reader == null) throw new IllegalArgumentException("revision was not captured by the transaction");
    return reader;
  }

  /** The caller retains all child outputs until this final protection check has succeeded. */
  public CompletableFuture<Void> finish() {
    CompletableFuture<Void> completion;
    synchronized (this) {
      if (finished != null) return finished.copy();
      if (state != State.ACTIVE || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive(scope));
      }
      state = State.FINISHING;
      finished = new CompletableFuture<>();
      completion = finished;
    }
    var checks = stores.values().stream().map(TxnReadSession::finishStore).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(checks).whenComplete((ignored, failure) -> {
      Throwable outcome = failure == null ? null : CompletionFailures.unwrapCompletion(failure);
      synchronized (TxnReadSession.this) {
        if (outcome == null) {
          // Claim successful completion while request cancellation is excluded. Otherwise a pin
          // can finish after the scope closes and an abandoned transaction can report success.
          if (state != State.FINISHING || !scope.startIfOpen(() -> state = State.FINISHED)) {
            outcome = inactive(scope);
            state = State.CLOSED;
          }
        } else {
          state = State.CLOSED;
        }
      }
      if (outcome != null) {
        try { scope.cancelAsync(CLEANUP); }
        catch (RuntimeException | Error cleanup) { if (cleanup != outcome) outcome.addSuppressed(cleanup); }
        completion.completeExceptionally(outcome);
      } else {
        completion.complete(null);
      }
    });
    return completion.copy();
  }

  private static CompletableFuture<Void> finishStore(ReadViewRecordStore store) {
    try { return Objects.requireNonNull(store.finishRead(), "read completion"); }
    catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
  }

  /** Aborts an unfinished transaction; the scope retains pins until physical producers drain. */
  @Override public void close() {
    synchronized (this) {
      if (state == State.FINISHED || state == State.CLOSED) return;
      state = State.CLOSED;
    }
    scope.cancelAsync(CLEANUP);
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private final class SessionReader implements RecordStore, BudgetedRecordStore {
    private final ReadViewRecordStore delegate;
    private SessionReader(ReadViewRecordStore delegate) { this.delegate = delegate; }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection) { return delegate.get(key, upper, projection); }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection, BudgetManager.Account account) {
      return delegate.get(key, upper, projection, account);
    }
    @Override public ScanOperation startScan(ScanRequest request) { return delegate.startScan(request); }
    @Override public ScanOperation startScan(ScanRequest request, BudgetManager.Account account) {
      return delegate.startScan(request, account);
    }
    @Override public boolean pointReadUsesScanner() { return delegate.pointReadUsesScanner(); }
    @Override public CompletableFuture<Void> finishRead() {
      synchronized (TxnReadSession.this) {
        return state == State.ACTIVE && !scope.isClosed() && !scope.isDeadlineExceeded()
            ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(inactive(scope));
      }
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) { return delegate.put(record); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      return delegate.compareAndSet(key, expected, replacement);
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return delegate.putBatch(records);
    }
  }
}
