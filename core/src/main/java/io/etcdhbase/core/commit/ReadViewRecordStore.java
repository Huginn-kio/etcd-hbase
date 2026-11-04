package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

/** Read-only physical-store view that overlays one pinned selected publication before query logic. */
public final class ReadViewRecordStore implements RecordStore, AutoCloseable {
  private final PinnedReadView pinned;
  private final RecordStore physical;
  private final ManifestCatalog manifests;
  private final ToIntFunction<ByteKey> bucketSelector;
  private final int bucketCount;
  private final RequestScope scope;
  private final PinnedReadView.ReaderLease readerLease;
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private final Set<ScanOperation> activeScans = Collections.newSetFromMap(new IdentityHashMap<>());
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
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(projection, "projection");
    if (revisionUpperBound <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("revision bound must be positive"));
    }
    if (!beginOperation()) return failed(inactiveFailure());
    CompletableFuture<Optional<StoredRecord>> read;
    try {
      synchronized (this) {
        read = getAtView(key, revisionUpperBound, projection);
      }
    } catch (RuntimeException | Error failure) {
      // beginOperation already incremented the lease; settle it before rethrowing any fatal Error.
      completeAfterSynchronousFailure(failure);
      if (failure instanceof Error fatal) throw fatal;
      return failed(failure);
    }
    return read.whenComplete((ignored, failure) -> completeOperation()).copy();
  }

  private CompletableFuture<Optional<StoredRecord>> getAtView(
      ByteKey key, long revisionUpperBound, ReadProjection projection) {
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
        Long.MAX_VALUE, scope, this::isInactive).thenCompose(slice -> {
          if (!isActive()) return failed(inactiveFailure());
          if (slice.candidates().isEmpty()) {
            return physicalGet(key, Math.min(upper, view.stableRevision()), projection);
          }
          PreparedCandidate candidate = slice.candidates().getFirst();
          return CompletableFuture.completedFuture(Optional.of(project(candidate.record(), projection)));
        });
  }

  @Override
  public synchronized ScanOperation startScan(ScanRequest request) {
    Objects.requireNonNull(request, "request");
    if (!beginOperation()) return failedOperation(inactiveFailure());
    if (request.bucket() >= bucketCount) {
      completeOperation();
      return failedOperation(new IllegalArgumentException("scan bucket exceeds the configured bucket count"));
    }
    ReadView view = pinned.view();
    long upper = Math.min(request.revisionUpperBound(), view.targetRevision());
    long stableUpper = Math.min(upper, view.stableRevision());
    ScanRequest baseRequest = withUpperBound(request, stableUpper);
    if (!includesSelectedCandidate(view, upper) || request.range().isEmpty()) {
      try {
        return track(physical.startScan(baseRequest));
      } catch (RuntimeException | Error failure) {
        // startScan has already incremented inFlight; balance it before preserving a fatal Error.
        completeAfterSynchronousFailure(failure);
        if (failure instanceof Error fatal) throw fatal;
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
      var candidates = manifests.candidatesInRange(root, bucket, request.range(), request.after(),
          maximumCandidates, request.maximumBytes(), scope, stopped::get);
      return track(new OverlayScanOperation(base, candidates, request, stopped));
    } catch (RuntimeException | Error failure) {
      if (base != null) {
        try {
          base.close();
        } catch (RuntimeException | Error cleanup) {
          // Keep the setup failure primary and attach close failure so an original Error stays fatal.
          if (failure != cleanup) failure.addSuppressed(cleanup);
        }
      }
      // Finish local ownership even when setup failed after opening the physical scanner.
      completeAfterSynchronousFailure(failure);
      if (failure instanceof Error fatal) throw fatal;
      return failedOperation(failure);
    }
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

  private ScanOperation track(ScanOperation operation) {
    synchronized (this) {
      activeScans.add(operation);
    }
    operation.completion().whenComplete((ignored, failure) -> {
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
      } catch (RuntimeException failure) {
        recordCloseFailure(failure);
      }
    }
    return operation;
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
    Throwable failure;
    try {
      release.close();
    } catch (RuntimeException | Error closeError) {
      // Record the fatal release failure, settle the termination signal, then rethrow it below.
      recordCloseFailure(closeError);
    }
    synchronized (this) { failure = closeFailure; }
    if (failure == null) termination.complete(null);
    else termination.completeExceptionally(failure);
    if (failure instanceof Error fatal) throw fatal;
  }

  private static ScanRequest withUpperBound(ScanRequest request, long upper) {
    return new ScanRequest(request.bucket(), request.range(), request.after(), upper,
        request.projection(), request.maximumRows(), request.maximumBytes(),
        request.maximumPhysicalRows());
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
      Throwable terminal = failure;
      StoragePage merged = null;
      try {
        if (terminal == null && !stopped.get()) {
          merged = merge(baseResult.join(), candidateResult.join(), request);
        } else if (terminal == null) {
          terminal = inactiveFailure();
        }
      } catch (RuntimeException | Error problem) {
        // Settle the result and close the physical scanner before propagating any unwrapped fatal error.
        terminal = CompletionFailures.unwrapCompletion(problem);
      } finally {
        closeBase();
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
        // The completion path must settle the result; retain close failure to attach before fatal propagation.
        closeFailure = failure;
      }
    }
  }

  private static StoragePage merge(StoragePage base,
      ManifestCatalog.CandidateSlice candidates, ScanRequest request) {
    List<StoredRecord> records = new ArrayList<>(request.maximumRows());
    ByteKey safeBoundary = commonPrefixBoundary(base, candidates);
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
        int order = baseRecord.key().compareTo(candidateRecord.key());
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
      if (safeBoundary != null && next.key().compareTo(safeBoundary) > 0) {
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
      ManifestCatalog.CandidateSlice candidates) {
    ByteKey boundary = null;
    if (!base.exhausted()) {
      if (base.records().isEmpty()) {
        throw new IllegalStateException("nonexhausted physical page has no resume key");
      }
      boundary = base.records().getLast().key();
    }
    if (!candidates.exhausted()) {
      ByteKey candidateBoundary = candidates.candidates().getLast().record().key();
      if (boundary == null || candidateBoundary.compareTo(boundary) < 0) {
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
