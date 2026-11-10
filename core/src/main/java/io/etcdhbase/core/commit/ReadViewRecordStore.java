package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.BudgetedRecordStore;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.PhysicalScanProgress;
import io.etcdhbase.core.storage.ScanCheckpoint;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

/** Read-only physical-store view that overlays one pinned selected publication before query logic. */
public final class ReadViewRecordStore implements RecordStore, BudgetedRecordStore, AutoCloseable {
  private final PinnedReadView pinned;
  private final RecordStore physical;
  private final ManifestCatalog manifests;
  private final ToIntFunction<ByteKey> bucketSelector;
  private final int bucketCount;
  private final RequestScope scope;
  private final PinnedReadView.ReaderLease readerLease;
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private final Set<ScanOperation> activeScans = Collections.newSetFromMap(new IdentityHashMap<>());
  private record ReverseScanOwner(BudgetManager.Account account, BudgetManager.Lease lease) {}
  private final java.util.Map<UUID, ReverseScanOwner> reverseScanOwners = new HashMap<>();
  private CompletableFuture<Void> finishFuture;
  private boolean closed;
  private boolean readerReleased;
  private int inFlight;
  private Throwable closeFailure;

  public ReadViewRecordStore(PinnedReadView pinned, RecordStore physical,
      ManifestCatalog manifests, ToIntFunction<ByteKey> bucketSelector, int bucketCount,
      RequestScope scope) {
    this.pinned = Objects.requireNonNull(pinned, "pinned");
    this.physical = Objects.requireNonNull(physical, "physical");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.bucketSelector = Objects.requireNonNull(bucketSelector, "bucketSelector");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (bucketCount <= 0 || bucketCount > 4096) {
      throw new IllegalArgumentException("invalid read view bucket count");
    }
    this.bucketCount = bucketCount;
    this.readerLease = pinned.retainReader();
    scope.registerScopedCleanup(this, termination);
  }

  @Override
  public CompletableFuture<Optional<StoredRecord>> get(
      ByteKey key, long revisionUpperBound, ReadProjection projection) {
    return get(key, revisionUpperBound, projection, null);
  }

  @Override
  public boolean pointReadUsesScanner() {
    return physical.pointReadUsesScanner();
  }

  ReadView protectedView() {
    if (!isActive()) throw new IllegalStateException("read view is not active");
    return pinned.view();
  }

  @Override
  public CompletableFuture<Optional<StoredRecord>> get(ByteKey key, long revisionUpperBound,
      ReadProjection projection, BudgetManager.Account account) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(projection, "projection");
    if (account != null && !account.belongsTo(scope)) {
      return failed(new IllegalArgumentException("read budget belongs to another request scope"));
    }
    if (revisionUpperBound <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("revision bound must be positive"));
    }
    if (!beginOperation()) return failed(inactiveFailure());
    CompletableFuture<Optional<StoredRecord>> read;
    try {
      read = pinned.renew(scope)
          .thenCompose(ignored -> getAtView(key, revisionUpperBound, projection, account));
    } catch (RuntimeException | Error failure) {
      // beginOperation already incremented the lease; settle it before rethrowing any fatal Error.
      completeAfterSynchronousFailure(failure);
      if (failure instanceof Error fatal) throw fatal;
      return failed(failure);
    }
    return read.whenComplete((ignored, failure) -> completeOperation()).copy();
  }

  private CompletableFuture<Optional<StoredRecord>> getAtView(
      ByteKey key, long revisionUpperBound, ReadProjection projection, BudgetManager.Account account) {
    if (!isActive()) return failed(inactiveFailure());
    ReadView view = pinned.view();
    long upper = Math.min(revisionUpperBound, view.targetRevision());
    if (!includesSelectedCandidate(view, upper)) {
      return physicalGet(key, Math.min(upper, view.stableRevision()), projection);
    }
    int bucket = bucketSelector.applyAsInt(key);
    if (bucket < 0 || bucket >= bucketCount) {
      return CompletableFuture.failedFuture(new IllegalStateException("key maps outside the configured buckets"));
    }
    ManifestRoot root = view.selectedRoot().orElseThrow();
    return manifests.candidatesInRange(root, bucket, KeyRange.point(key), Optional.empty(), 2,
        Long.MAX_VALUE, scope, this::isInactive, account).thenCompose(slice -> {
          try (slice) {
            if (!isActive()) return failed(inactiveFailure());
            if (slice.candidates().isEmpty()) {
              return physicalGet(key, Math.min(upper, view.stableRevision()), projection);
            }
            PreparedCandidate candidate = slice.candidates().getFirst();
            return CompletableFuture.completedFuture(Optional.of(project(candidate.record(), projection)));
          }
        });
  }

  @Override
  public synchronized ScanOperation startScan(ScanRequest request) {
    return startScan(request, null);
  }

  @Override
  public synchronized ScanOperation startScan(ScanRequest request, BudgetManager.Account account) {
    Objects.requireNonNull(request, "request");
    if (account != null && !account.belongsTo(scope)) {
      return failedOperation(new IllegalArgumentException("scan budget belongs to another request scope"));
    }
    if (!beginOperation()) return failedOperation(inactiveFailure());
    return track(new DeferredScanOperation(request, account), request);
  }

  private ScanOperation startScanAfterRenew(ScanRequest request, BudgetManager.Account account) {
    if (!isActive()) return failedOperation(inactiveFailure());
    if (request.direction() == ScanDirection.DESCENDING) {
      try { registerReverseScan(request, account); }
      catch (BackendFailure | RuntimeException failure) { return failedOperation(failure); }
    }
    if (request.bucket() >= bucketCount) {
      return failedOperation(new IllegalArgumentException("scan bucket exceeds the configured bucket count"));
    }
    ReadView view = pinned.view();
    long upper = Math.min(request.revisionUpperBound(), view.targetRevision());
    long stableUpper = Math.min(upper, view.stableRevision());
    ScanRequest baseRequest = withUpperBound(request, stableUpper);
    if (!includesSelectedCandidate(view, upper) || request.range().isEmpty()) {
      try {
        return physical.startScan(baseRequest);
      } catch (RuntimeException failure) {
        return failedOperation(failure);
      }
    }
    int bucket = request.bucket();
    ManifestRoot root = view.selectedRoot().orElseThrow();
    ScanOperation base = null;
    try {
      base = physical.startScan(baseRequest);
      int maximumCandidates = Math.min(request.maximumRows(), 1024);
      var stopped = new AtomicBoolean();
      java.util.function.Supplier<CompletableFuture<ManifestCatalog.CandidateSlice>> loadCandidates = () ->
          manifests.candidatesInRange(root, bucket, request.range(), request.after(), maximumCandidates,
              ManifestCatalog.MAX_RANGE_CANDIDATE_BYTES, scope, stopped::get, account, request.direction());
      var candidates = request.direction() == ScanDirection.DESCENDING
          ? base.completion().toCompletableFuture().thenCompose(page -> {
            if (stopped.get() || !isActive()) return CompletableFuture.failedFuture(inactiveFailure());
            return page.physicalProgress().isPresent()
                ? CompletableFuture.completedFuture(ManifestCatalog.CandidateSlice.empty()) : loadCandidates.get();
          }) : loadCandidates.get();
      return new OverlayScanOperation(base, candidates, request, stopped);
    } catch (RuntimeException | Error failure) {
      // Close the already-open physical producer before preserving a setup Error as fatal.
      if (base != null) {
        try {
          base.close();
        } catch (RuntimeException | Error cleanup) {
          // Keep the setup failure primary and attach close failure so an original Error stays fatal.
          if (failure != cleanup) failure.addSuppressed(cleanup);
        }
      }
      if (failure instanceof Error fatal) throw fatal;
      return failedOperation(failure);
    }
  }

  @Override
  public CompletableFuture<Void> finishRead() {
    synchronized (this) {
      if (finishFuture != null) return finishFuture.copy();
      if (closed || inFlight != 0 || !activeScans.isEmpty()) {
        return CompletableFuture.failedFuture(inactiveFailure());
      }
      closed = true;
      finishFuture = new CompletableFuture<>();
    }
    CompletableFuture<Void> completion;
    try {
      completion = pinned.complete(scope);
    } catch (RuntimeException | Error failure) {
      // Keep finishRead's asynchronous contract and still release the reader lease below.
      completion = CompletableFuture.failedFuture(failure);
    }
    completion.whenComplete((ignored, failure) -> {
      PinnedReadView.ReaderLease release = null;
      synchronized (ReadViewRecordStore.this) {
        if (!readerReleased) {
          readerReleased = true;
          release = readerLease;
        }
      }
      if (release != null) {
        try {
          releaseReader(release);
        } catch (RuntimeException | Error closeFailure) {
          // Settle finishFuture so callers observe this release failure or the original failure
          // with the release failure suppressed.
          if (failure == null) failure = closeFailure;
          else if (failure != closeFailure) failure.addSuppressed(closeFailure);
        }
      }
      if (failure == null) finishFuture.complete(null);
      else finishFuture.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
    });
    return finishFuture.copy();
  }

  @Override
  public void close() {
    List<ScanOperation> scans;
    PinnedReadView.ReaderLease release = null;
    synchronized (this) {
      if (closed) return;
      closed = true;
      scans = List.copyOf(activeScans);
      if (inFlight == 0 && !readerReleased) {
        readerReleased = true;
        release = readerLease;
      }
    }
    Error fatal = null;
    RuntimeException closeError = null;
    // Continue closing every scan after an Error, then release the lease before rethrowing a fatal cause.
    for (ScanOperation scan : scans) {
      try {
        scan.close();
      } catch (RuntimeException | Error failure) {
        recordCloseFailure(failure);
        if (failure instanceof Error error && fatal == null) fatal = error;
        else if (failure instanceof RuntimeException runtime && closeError == null) closeError = runtime;
      }
    }
    if (release != null) {
      try {
        releaseReader(release);
      } catch (RuntimeException | Error failure) {
        if (failure instanceof Error error && fatal == null) fatal = error;
        else if (failure instanceof RuntimeException runtime && closeError == null) closeError = runtime;
      }
    }
    if (fatal != null) throw fatal;
    if (closeError != null) throw closeError;
  }

  @Override
  public CompletableFuture<Void> put(StoredRecord record) {
    return readOnlyFailure();
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(
      ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
    return readOnlyFailure();
  }

  @Override
  public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
    return readOnlyFailure();
  }

  private CompletableFuture<Optional<StoredRecord>> physicalGet(
      ByteKey key, long upper, ReadProjection projection) {
    if (!isActive()) return failed(inactiveFailure());
    return physical.get(key, upper, projection).thenApply(record -> {
      if (!isActive()) throw new CompletionException(inactiveFailure());
      if (record.isPresent()
          && record.orElseThrow().metadata().modificationRevision() > upper) {
        throw new IllegalStateException("physical point read exceeded its revision upper bound");
      }
      return record;
    });
  }

  private boolean includesSelectedCandidate(ReadView view, long upper) {
    return view.includesSelectedKeyChanges()
        && view.selectedDecision().orElseThrow().revision() <= upper;
  }

  private boolean isActive() {
    synchronized (this) {
      return !closed && pinned.isOpen() && !scope.isClosed() && !scope.isDeadlineExceeded();
    }
  }

  private boolean isInactive() {
    return !isActive();
  }

  private boolean beginOperation() {
    synchronized (this) {
      if (closed || !pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) return false;
      inFlight = Math.addExact(inFlight, 1);
      return true;
    }
  }

  private ScanOperation track(ScanOperation operation, ScanRequest request) {
    synchronized (this) {
      activeScans.add(operation);
    }
    // A consumer may immediately finish the view from its scan callback. Publish completion
    // only after this store has retired the scan and its operation lease.
    var completion = operation.completion().thenApply(page -> logicalProgress(page, request)).whenComplete((ignored, failure) -> {
      synchronized (ReadViewRecordStore.this) {
        activeScans.remove(operation);
      }
      completeOperation();
    });
    boolean stop;
    synchronized (this) { stop = closed; }
    if (stop) {
      try {
        operation.close();
      } catch (RuntimeException | Error failure) {
        // Record close failure for finishRead; the owner must still observe a fatal Error now.
        recordCloseFailure(failure);
        if (failure instanceof Error fatal) throw fatal;
      }
    }
    return new ScanOperation() {
      @Override public CompletionStage<StoragePage> completion() {
        return completion.toCompletableFuture().minimalCompletionStage();
      }
      @Override public void close() { operation.close(); }
    };
  }

  private void registerReverseScan(ScanRequest request, BudgetManager.Account account) throws BackendFailure {
    synchronized (this) {
      var existing = reverseScanOwners.get(request.viewId());
      if (existing != null) {
        if (existing.account() != account) throw new IllegalArgumentException("scan account changed during a read view");
        if (closed || !pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) throw inactiveFailure();
        return;
      }
      if (request.checkpoint().isPresent() || request.physicalProgress().isPresent()) {
        throw new IllegalArgumentException("scan checkpoint belongs to another read view");
      }
    }
    if (account == null) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    var lease = account.tryRetain(BudgetManager.Memory.HEAD, 192)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    boolean keep = false;
    boolean wrongAccount = false;
    synchronized (this) {
      if (!closed) {
        var existing = reverseScanOwners.get(request.viewId());
        if (existing == null) {
          reverseScanOwners.put(request.viewId(), new ReverseScanOwner(account, lease));
          keep = true;
        } else if (existing.account() != account) {
          wrongAccount = true;
        }
      }
    }
    if (!keep) lease.close();
    if (wrongAccount) throw new IllegalArgumentException("scan account changed during a read view");
    if (!isActive()) throw inactiveFailure();
  }

  private StoragePage logicalProgress(StoragePage page, ScanRequest request) {
    if (page.physicalProgress().isEmpty()) return page;
    var progress = page.physicalProgress().orElseThrow();
    long stableUpper = Math.min(Math.min(request.revisionUpperBound(), pinned.view().targetRevision()),
        pinned.view().stableRevision());
    var expected = withUpperBound(request, stableUpper);
    if (!progress.viewId().equals(expected.viewId()) || progress.bucket() != expected.bucket()
        || progress.revision() != stableUpper || progress.direction() != expected.direction()
        || !progress.logicalCheckpoint().equals(expected.checkpoint())) {
      throw new IllegalStateException("physical progress differs from the pinned read view");
    }
    var logical = new PhysicalScanProgress(request.viewId(), request.bucket(), request.revisionUpperBound(),
        request.direction(), request.checkpoint(), progress.lastPhysicalRow(), progress.physicalSourceId());
    return new StoragePage(page.records(), page.exhausted(), page.cost(), Optional.of(logical));
  }

  private void completeOperation() {
    PinnedReadView.ReaderLease release = null;
    synchronized (this) {
      if (inFlight <= 0) throw new IllegalStateException("read view operation count underflow");
      inFlight--;
      if (closed && inFlight == 0 && !readerReleased) {
        readerReleased = true;
        release = readerLease;
      }
    }
    if (release != null) releaseReader(release);
  }

  private void completeAfterSynchronousFailure(Throwable failure) {
    try {
      completeOperation();
    } catch (RuntimeException | Error cleanup) {
      // Preserve the initiating failure; a fatal cleanup Error takes precedence over a runtime failure.
      if (cleanup == failure) return;
      if (failure instanceof Error) {
        failure.addSuppressed(cleanup);
      } else if (cleanup instanceof Error fatal) {
        fatal.addSuppressed(failure);
        throw fatal;
      } else {
        failure.addSuppressed(cleanup);
      }
    }
  }

  private void recordCloseFailure(Throwable failure) {
    synchronized (this) {
      if (closeFailure == null) closeFailure = failure;
      else if (closeFailure != failure) closeFailure.addSuppressed(failure);
    }
  }

  private void releaseReader(PinnedReadView.ReaderLease release) {
    List<BudgetManager.Lease> owners;
    synchronized (this) {
      owners = reverseScanOwners.values().stream().map(ReverseScanOwner::lease).toList();
      reverseScanOwners.clear();
    }
    for (var owner : owners) {
      try { owner.close(); }
      catch (RuntimeException | Error failure) { recordCloseFailure(failure); }
    }
    try {
      release.close();
    } catch (RuntimeException | Error closeError) {
      // Record the fatal release failure, settle the termination signal, then rethrow it below.
      recordCloseFailure(closeError);
    }
    // RequestScope owns PinnedReadView; finishing one view must not close sibling views in it.
    pinned.termination().whenComplete((ignored, pinFailure) -> {
      Throwable failure = pinFailure;
      synchronized (ReadViewRecordStore.this) {
        if (closeFailure != null) {
          if (failure == null) failure = closeFailure;
          else if (failure != closeFailure) failure.addSuppressed(closeFailure);
        }
      }
      if (failure == null) termination.complete(null);
      else termination.completeExceptionally(failure);
      if (failure instanceof Error fatal) throw fatal;
    });
  }

  private static ScanRequest withUpperBound(ScanRequest request, long upper) {
    var checkpoint = request.checkpoint().map(previous -> new ScanCheckpoint(previous.bucket(), previous.kind(),
        previous.lower(), previous.upper(), upper, previous.direction(), previous.viewId(), previous.key()));
    var progress = request.physicalProgress().map(previous -> new PhysicalScanProgress(previous.viewId(),
        previous.bucket(), upper, previous.direction(), checkpoint, previous.lastPhysicalRow(), previous.physicalSourceId()));
    return new ScanRequest(request.bucket(), request.range(), checkpoint, upper, request.projection(),
        request.maximumRows(), request.maximumBytes(), request.maximumPhysicalRows(), request.direction(),
        request.viewId(), progress);
  }

  private static StoredRecord project(StoredRecord record, ReadProjection projection) {
    return projection == ReadProjection.FULL || !record.hasValue()
        ? record : StoredRecord.metadataOnly(record.key(), record.metadata());
  }

  private static BackendFailure inactiveFailure() {
    return new BackendFailure(BackendFailure.Category.CANCELLED);
  }

  private static <T> CompletableFuture<T> readOnlyFailure() {
    return CompletableFuture.failedFuture(
        new UnsupportedOperationException("a pinned read view is read-only"));
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private static ScanOperation failedOperation(Throwable failure) {
    return new ScanOperation() {
      @Override public CompletionStage<StoragePage> completion() {
        return CompletableFuture.failedFuture(failure);
      }
      @Override public void close() {}
    };
  }

  private final class DeferredScanOperation implements ScanOperation {
    private final ScanRequest request;
    private final BudgetManager.Account account;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean delegateClosed = new AtomicBoolean();
    private final CompletableFuture<StoragePage> result = new CompletableFuture<>();
    private volatile ScanOperation delegate;
    private volatile Throwable closeFailure;

    private DeferredScanOperation(ScanRequest request, BudgetManager.Account account) {
      this.request = request;
      this.account = account;
      CompletableFuture<Void> renewal;
      try {
        renewal = pinned.renew(scope);
      } catch (RuntimeException | Error failure) {
        // The returned scan operation is the observer; its completion also settles inFlight.
        result.completeExceptionally(failure);
        return;
      }
      renewal.whenComplete((ignored, failure) -> {
        if (failure != null) {
          result.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
          return;
        }
        if (stopped.get() || !isActive()) {
          result.completeExceptionally(inactiveFailure());
          return;
        }
        ScanOperation opened;
        try {
          opened = startScanAfterRenew(request, account);
        } catch (RuntimeException | Error openFailure) {
          // Preserve the async scan contract; track() observes this result to release its lease.
          result.completeExceptionally(openFailure);
          return;
        }
        delegate = opened;
        if (stopped.get()) closeDelegate();
        opened.completion().whenComplete((page, operationFailure) -> {
          Throwable terminal = operationFailure == null ? closeFailure
              : CompletionFailures.unwrapCompletion(operationFailure);
          if (terminal == null && (stopped.get() || !isActive())) terminal = inactiveFailure();
          if (closeFailure != null && terminal != closeFailure) terminal.addSuppressed(closeFailure);
          if (terminal == null) result.complete(page);
          else result.completeExceptionally(terminal);
        });
      });
    }

    @Override
    public CompletionStage<StoragePage> completion() {
      return result.minimalCompletionStage();
    }

    @Override
    public void close() {
      stopped.set(true);
      closeDelegate();
    }

    private void closeDelegate() {
      ScanOperation current = delegate;
      if (current == null || !delegateClosed.compareAndSet(false, true)) return;
      try {
        current.close();
      } catch (RuntimeException | Error failure) {
        // The operation completion callback also reports this close failure to its async observer.
        closeFailure = failure;
        if (failure instanceof Error fatal) throw fatal;
      }
    }
  }

  private final class OverlayScanOperation implements ScanOperation {
    private final ScanOperation base;
    private final CompletableFuture<ManifestCatalog.CandidateSlice> candidates;
    private final ScanRequest request;
    private final AtomicBoolean stopped;
    private final AtomicBoolean baseClosed = new AtomicBoolean();
    private final CompletableFuture<StoragePage> result = new CompletableFuture<>();
    private volatile Throwable closeFailure;

    private OverlayScanOperation(ScanOperation base,
        CompletableFuture<ManifestCatalog.CandidateSlice> candidates, ScanRequest request,
        AtomicBoolean stopped) {
      this.base = base;
      this.candidates = candidates;
      this.request = request;
      this.stopped = stopped;
      CompletableFuture<StoragePage> baseResult = base.completion().toCompletableFuture();
      CompletableFuture.allOf(baseResult, candidates).whenComplete((ignored, failure) -> finish(
          baseResult, candidates, failure));
    }

    @Override
    public CompletionStage<StoragePage> completion() {
      return result.minimalCompletionStage();
    }

    @Override
    public void close() {
      stopped.set(true);
      closeBase();
      if (closeFailure instanceof Error fatal) throw fatal;
    }

    private void finish(CompletableFuture<StoragePage> baseResult,
        CompletableFuture<ManifestCatalog.CandidateSlice> candidateResult, Throwable failure) {
      Throwable terminal = failure == null ? null : CompletionFailures.unwrapCompletion(failure);
      StoragePage merged = null;
      ManifestCatalog.CandidateSlice candidateSlice = candidateResult.isDone()
          && !candidateResult.isCompletedExceptionally() && !candidateResult.isCancelled()
          ? candidateResult.getNow(null) : null;
      try {
        if (terminal == null && !stopped.get()) {
          merged = merge(baseResult.join(), candidateResult.join(), request);
        } else if (terminal == null) {
          terminal = inactiveFailure();
        }
      } catch (RuntimeException | Error problem) {
        // Keep RuntimeException and Error observable on result; first settle the scanner and slice
        // ownership, then complete result exceptionally before rethrowing a fatal Error below.
        terminal = CompletionFailures.unwrapCompletion(problem);
      } finally {
        closeBase();
        if (candidateSlice != null) {
          try {
            candidateSlice.close();
          } catch (RuntimeException | Error cleanup) {
            // CandidateSlice attempts every lease and suppresses later cleanup failures. Preserve
            // the operation failure as primary; result below is the asynchronous observer.
            if (terminal == null) terminal = cleanup;
            else if (terminal != cleanup) terminal.addSuppressed(cleanup);
          }
        }
      }
      if (closeFailure != null) {
        if (terminal == null) terminal = closeFailure;
        else if (terminal != closeFailure) terminal.addSuppressed(closeFailure);
      }
      if (terminal == null) result.complete(merged);
      else result.completeExceptionally(terminal);
      if (terminal instanceof Error fatal) throw fatal;
    }

    private void closeBase() {
      if (!baseClosed.compareAndSet(false, true)) return;
      try {
        base.close();
      } catch (RuntimeException | Error failure) {
        // Do not let close skip result settlement. Finish preserves this failure on the result and
        // rethrows it after settlement when it remains the terminal fatal Error.
        closeFailure = failure;
      }
    }
  }

  private static StoragePage merge(StoragePage base,
      ManifestCatalog.CandidateSlice candidates, ScanRequest request) {
    if (base.physicalProgress().isPresent()) {
      return new StoragePage(List.of(), false, mergeCost(base.cost(), candidates), base.physicalProgress());
    }
    List<StoredRecord> records = new ArrayList<>(request.maximumRows());
    ByteKey safeBoundary = commonPrefixBoundary(base, candidates, request.direction());
    int baseIndex = 0;
    int candidateIndex = 0;
    long bytes = 0;
    boolean truncated = false;
    while (baseIndex < base.records().size() || candidateIndex < candidates.candidates().size()) {
      StoredRecord next;
      if (baseIndex == base.records().size()) {
        next = candidates.candidates().get(candidateIndex++).record();
      } else if (candidateIndex == candidates.candidates().size()) {
        next = base.records().get(baseIndex++);
      } else {
        StoredRecord baseRecord = base.records().get(baseIndex);
        StoredRecord candidateRecord = candidates.candidates().get(candidateIndex).record();
        int order = request.direction().compare(baseRecord.key(), candidateRecord.key());
        if (order == 0) {
          baseIndex++;
          next = candidateRecord;
          candidateIndex++;
        } else if (order < 0) {
          next = baseRecord;
          baseIndex++;
        } else {
          next = candidateRecord;
          candidateIndex++;
        }
      }
      if (safeBoundary != null && request.direction().compare(next.key(), safeBoundary) > 0) {
        truncated = true;
        break;
      }
      StoredRecord projected = project(next, request.projection());
      long retained = projected.retainedBytes();
      if (records.size() == request.maximumRows() || retained > request.maximumBytes() - bytes) {
        if (records.isEmpty()) {
          throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        }
        truncated = true;
        break;
      }
      records.add(projected);
      bytes = Math.addExact(bytes, retained);
    }
    boolean exhausted = !truncated && baseIndex == base.records().size()
        && candidateIndex == candidates.candidates().size()
        && base.exhausted() && candidates.exhausted();
    if (records.isEmpty() && !exhausted) {
      throw new IllegalStateException("read view merge failed to make bounded scan progress");
    }
    return new StoragePage(records, exhausted, mergeCost(base.cost(), candidates));
  }

  private static ByteKey commonPrefixBoundary(StoragePage base,
      ManifestCatalog.CandidateSlice candidates, ScanDirection direction) {
    ByteKey boundary = null;
    if (!base.exhausted()) {
      if (base.records().isEmpty()) {
        throw new IllegalStateException("nonexhausted physical page has no resume key");
      }
      boundary = base.records().getLast().key();
    }
    if (!candidates.exhausted()) {
      ByteKey candidateBoundary = candidates.candidates().getLast().record().key();
      if (boundary == null || direction.compare(candidateBoundary, boundary) < 0) {
        boundary = candidateBoundary;
      }
    }
    return boundary;
  }

  private static StoragePage.Cost mergeCost(StoragePage.Cost base,
      ManifestCatalog.CandidateSlice candidates) {
    var observations = new EnumMap<StoragePage.Observation, Long>(StoragePage.Observation.class);
    observations.putAll(base.observations());
    observations.merge(StoragePage.Observation.MANIFEST_DIRECTORY_PAGES,
        (long) candidates.directoryPages(), Math::addExact);
    observations.merge(StoragePage.Observation.MANIFEST_LEAF_PAGES,
        (long) candidates.leafPages(), Math::addExact);
    observations.merge(StoragePage.Observation.MANIFEST_CANDIDATE_RECORDS,
        (long) candidates.candidates().size(), Math::addExact);
    observations.merge(StoragePage.Observation.MANIFEST_CANDIDATE_BYTES,
        candidates.payloadBytes(), Math::addExact);
    return new StoragePage.Cost(base.physicalRows(), base.returnedCellBytes(),
        base.scanRpcCalls(), observations);
  }

}
