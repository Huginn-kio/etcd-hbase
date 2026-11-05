package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/** Verifies and streams a sealed candidate/event manifest without collecting transaction data. */
public final class ManifestCatalog {
  /** Maximum retained candidate payload fetched for one range slice, independent of result bytes. */
  static final long MAX_RANGE_CANDIDATE_BYTES = 9L * 1024 * 1024;

  public record Summary(long count, long payloadBytes) {
    public Summary {
      if (count < 0 || payloadBytes < 0) throw new IllegalArgumentException("negative manifest summary");
    }
  }

  /** Bounded, authenticated candidate slice for one physical salt bucket and logical key range. */
  public static final class CandidateSlice implements AutoCloseable {
    private volatile List<PreparedCandidate> candidates;
    private final boolean exhausted;
    private final int directoryPages;
    private final int leafPages;
    private final long payloadBytes;
    private final List<BudgetManager.Lease> leases;
    private final AtomicBoolean closed = new AtomicBoolean();

    public CandidateSlice(List<PreparedCandidate> candidates, boolean exhausted,
        int directoryPages, int leafPages, long payloadBytes) {
      this(candidates, exhausted, directoryPages, leafPages, payloadBytes, List.of());
    }

    private CandidateSlice(List<PreparedCandidate> candidates, boolean exhausted,
        int directoryPages, int leafPages, long payloadBytes, List<BudgetManager.Lease> leases) {
      this.candidates = List.copyOf(candidates);
      if (directoryPages < 0 || leafPages < 0 || payloadBytes < 0) {
        throw new IllegalArgumentException("negative candidate slice accounting");
      }
      this.exhausted = exhausted;
      this.directoryPages = directoryPages;
      this.leafPages = leafPages;
      this.payloadBytes = payloadBytes;
      this.leases = List.copyOf(leases);
    }

    public List<PreparedCandidate> candidates() { return candidates; }
    public boolean exhausted() { return exhausted; }
    public int directoryPages() { return directoryPages; }
    public int leafPages() { return leafPages; }
    public long payloadBytes() { return payloadBytes; }

    /** Releases candidate-buffer ownership after the consumer has projected or transferred it. */
    @Override public void close() {
      if (!closed.compareAndSet(false, true)) return;
      candidates = List.of();
      Throwable failure = null;
      for (BudgetManager.Lease lease : leases) {
        try {
          lease.close();
        } catch (RuntimeException | Error cleanup) {
          if (failure == null) failure = cleanup;
          else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
      }
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof RuntimeException runtime) throw runtime;
    }
  }

  private static final int MAX_CANDIDATE_PAGES = Math.toIntExact(ManifestRoot.MAX_CANDIDATES);
  private static final int MAX_EVENT_PAGES = Math.toIntExact(ManifestRoot.MAX_EVENTS);
  private static final int MAX_CANDIDATE_DIRECTORY_PAGES =
      MAX_CANDIDATE_PAGES * ManifestDirectoryPage.MAX_DEPTH;
  private static final int MAX_EVENT_DIRECTORY_PAGES = MAX_EVENT_PAGES * EventDirectoryPage.MAX_DEPTH;

  private final ManifestRepository repository;

  public ManifestCatalog(ManifestRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
    return repository.readRoot(identity, scope).thenApply(root -> root.map(value -> {
      if (!value.systemIdentity().equals(identity)) {
        throw new IllegalStateException("manifest root identity differs from its durable key");
      }
      return value;
    }));
  }

  public CompletableFuture<Summary> forEachCandidate(ManifestRoot root, RequestScope scope,
      Function<PreparedCandidate, CompletableFuture<Void>> consumer) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(consumer, "consumer");
    var cursor = new CandidateCursor(root, scope, consumer);
    if (root.candidateDirectory().isEmpty()) {
      return root.candidateCount() == 0 && root.totalValueBytes() == 0
          ? CompletableFuture.completedFuture(new Summary(0, 0))
          : CompletableFuture.failedFuture(new IllegalStateException("manifest candidate directory is missing"));
    }
    return cursor.visit(root.candidateDirectory().orElseThrow())
        .thenApply(ignored -> cursor.summary());
  }

  public CompletableFuture<Summary> forEachEvent(ManifestRoot root, RequestScope scope,
      Function<PreparedEvent, CompletableFuture<Void>> consumer) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(consumer, "consumer");
    var cursor = new EventCursor(root, scope, consumer);
    if (root.eventDirectory().isEmpty()) {
      return root.eventCount() == 0 && root.totalEventBytes() == 0
          ? CompletableFuture.completedFuture(new Summary(0, 0))
          : CompletableFuture.failedFuture(new IllegalStateException("manifest event directory is missing"));
    }
    return cursor.visit(root.eventDirectory().orElseThrow()).thenApply(ignored -> cursor.summary());
  }

  /**
   * Reads only candidate leaves that can intersect a bucket/range. The caller resumes after the
   * last returned key; a full slice reports non-exhaustion conservatively until the next read.
   */
  public CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope) {
    return candidatesInRange(root, bucket, range, afterExclusive, maximumCandidates,
        maximumCandidateBytes, scope, () -> false);
  }

  /**
   * Same bounded traversal with a local stop signal for a page operation being closed.
   * maximumCandidateBytes bounds candidate records read; result projection bytes are bounded by
   * the caller after projection and must not be substituted for this read budget.
   */
  public CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope, BooleanSupplier cancelled) {
    return candidatesInRange(root, bucket, range, afterExclusive, maximumCandidates,
        maximumCandidateBytes, scope, cancelled, null);
  }

  /** Same traversal with candidate memory reserved before each immutable value is fetched. */
  public CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope, BooleanSupplier cancelled,
      BudgetManager.Account account) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(afterExclusive, "afterExclusive");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(cancelled, "cancelled");
    if (account != null && !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("candidate budget belongs to another request scope"));
    }
    if (bucket < 0 || maximumCandidates <= 0 || maximumCandidates > 1025
        || maximumCandidateBytes <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid candidate slice bounds"));
    }
    if (range.isEmpty() || root.candidateDirectory().isEmpty()) {
      return CompletableFuture.completedFuture(new CandidateSlice(List.of(), true, 0, 0, 0));
    }
    var callerCancelled = new AtomicBoolean();
    var cursor = new CandidateRangeCursor(root, bucket, range, afterExclusive,
        maximumCandidates, maximumCandidateBytes, scope,
        () -> cancelled.getAsBoolean() || callerCancelled.get(), account);
    var result = new CompletableFuture<CandidateSlice>();
    CompletableFuture<Void> traversal;
    try {
      traversal = cursor.visit(root.candidateDirectory().orElseThrow());
    } catch (RuntimeException | Error failure) {
      result.completeExceptionally(cursor.releaseAfterFailure(failure));
      return result;
    }
    traversal.whenComplete((ignored, failure) -> {
      if (failure != null) {
        result.completeExceptionally(cursor.releaseAfterFailure(unwrap(failure)));
      } else {
        CandidateSlice slice = cursor.slice();
        if (!result.complete(slice)) slice.close();
      }
    });
    result.whenComplete((slice, failure) -> {
      if (result.isCancelled()) callerCancelled.set(true);
    });
    return result;
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable terminal = failure;
    while (terminal instanceof CompletionException && terminal.getCause() != null) {
      terminal = terminal.getCause();
    }
    return terminal;
  }

  private boolean candidateMatchesReference(ManifestRoot root,
      ManifestCandidateReference reference, PreparedCandidate candidate) {
    return candidate.transactionId().equals(root.transactionId())
        && candidate.ownerEpoch() == root.ownerEpoch()
        && candidate.baseSequence() == root.baseSequence()
        && candidate.baseRevision() == root.baseRevision()
        && candidate.systemIdentity().equals(reference.candidateIdentity())
        && repository.candidateBucket(candidate) == reference.bucket()
        && candidate.record().key().equals(reference.logicalKey())
        && candidate.contentDigest().equals(reference.candidateDigest())
        && candidate.valueDigest().equals(reference.valueDigest())
        && candidate.record().metadata().valueLength() == reference.valueLength()
        && (candidate.record().metadata().state() == StorageMetadata.State.TOMBSTONE)
            == reference.tombstone();
  }

  private final class CandidateRangeCursor {
    private final ManifestRoot root;
    private final int bucket;
    private final KeyRange range;
    private final Optional<ByteKey> after;
    private final int maximumCandidates;
    private final long maximumCandidateBytes;
    private final RequestScope scope;
    private final BooleanSupplier cancelled;
    private final BudgetManager.Account account;
    private final List<PreparedCandidate> candidates = new java.util.ArrayList<>();
    private final List<BudgetManager.Lease> leases = new java.util.ArrayList<>();
    private int directoryPages;
    private int leafPages;
    private long payloadBytes;
    private ByteKey lastKey;
    private boolean exhausted = true;

    private CandidateRangeCursor(ManifestRoot root, int bucket, KeyRange range,
        Optional<ByteKey> after, int maximumCandidates, long maximumCandidateBytes,
        RequestScope scope, BooleanSupplier cancelled, BudgetManager.Account account) {
      this.root = root;
      this.bucket = bucket;
      this.range = range;
      this.after = after;
      this.maximumCandidates = maximumCandidates;
      this.maximumCandidateBytes = maximumCandidateBytes;
      this.scope = scope;
      this.cancelled = cancelled;
      this.account = account;
    }

    private CompletableFuture<Void> visit(ManifestChildReference reference) {
      if (stopped()) return CompletableFuture.completedFuture(null);
      if (cancelled.getAsBoolean()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      if (scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      }
      if (!mayIntersect(reference)) return CompletableFuture.completedFuture(null);
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES) {
        return invalid("manifest range traversal exceeds its directory budget");
      }
      return repository.readDirectoryPage(reference.identity(), scope).thenCompose(stored -> {
        ManifestDirectoryPage page = stored.orElseThrow(() -> missing("manifest directory page"));
        if (!page.transactionId().equals(root.transactionId()) || !page.reference().equals(reference)) {
          return invalid("manifest directory page differs from its authenticated range reference");
        }
        return visitChildren(page.children(), 0);
      });
    }

    private CompletableFuture<Void> visitChildren(List<ManifestChildReference> children, int index) {
      if (index == children.size() || stopped()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, index + 1));
    }

    private CompletableFuture<Void> visitLeaf(ManifestChildReference reference) {
      if (++leafPages > MAX_CANDIDATE_PAGES) {
        return invalid("manifest range traversal exceeds its leaf budget");
      }
      return repository.readPage(reference.identity(), scope).thenCompose(stored -> {
        ManifestPage page = stored.orElseThrow(() -> missing("manifest leaf page"));
        if (!page.transactionId().equals(root.transactionId()) || !page.reference().equals(reference)
            || page.bucket() != bucket) {
          return invalid("manifest leaf page differs from its authenticated bucket range");
        }
        return visitCandidates(page.candidates(), 0);
      });
    }

    private CompletableFuture<Void> visitCandidates(
        List<ManifestCandidateReference> references, int index) {
      if (index == references.size() || stopped()) return CompletableFuture.completedFuture(null);
      if (cancelled.getAsBoolean() || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      }
      ManifestCandidateReference reference = references.get(index);
      if (reference.bucket() != bucket) {
        return invalid("manifest leaf contains a candidate from another bucket");
      }
      ByteKey key = reference.logicalKey();
      if (!range.contains(key) || after.filter(previous -> key.compareTo(previous) <= 0).isPresent()) {
        return visitCandidates(references, index + 1);
      }
      if (lastKey != null && lastKey.compareTo(key) >= 0) {
        return invalid("manifest range candidates are duplicated or unordered");
      }
      long retained = reference.retainedBytes();
      if (retained > maximumCandidateBytes - payloadBytes) {
        if (!candidates.isEmpty()) {
          exhausted = false;
          return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (account == null) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      BudgetManager.Lease lease;
      try {
        lease = account.tryRetain(BudgetManager.Memory.CANDIDATE, retained).orElse(null);
      } catch (BackendFailure | RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      if (lease == null) {
        if (!candidates.isEmpty()) {
          exhausted = false;
          return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      try {
        account.chargeCandidateRead(retained);
      } catch (BackendFailure | RuntimeException failure) {
        return failedCandidate(lease, failure);
      }
      CompletableFuture<Optional<PreparedCandidate>> read;
      try {
        read = Objects.requireNonNull(repository.readCandidate(reference.candidateIdentity(), scope),
            "candidate read future");
      } catch (RuntimeException | Error failure) {
        return failedCandidate(lease, failure);
      }
      return read.handle((stored, failure) -> {
        if (failure != null) return failedCandidate(lease, unwrap(failure));
        try {
          if (cancelled.getAsBoolean() || scope.isClosed() || scope.isDeadlineExceeded()) {
            return failedCandidate(lease, new BackendFailure(scope.isDeadlineExceeded()
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
          }
          PreparedCandidate candidate = stored.orElseThrow(() -> missing("private candidate"));
          if (!candidateMatchesReference(root, reference, candidate)
              || candidate.record().retainedBytes() != retained) {
            return failedCandidate(lease, new IllegalStateException(
                "private candidate differs from its authenticated range reference"));
          }
          candidates.add(candidate);
          leases.add(lease);
          payloadBytes = Math.addExact(payloadBytes, retained);
          lastKey = key;
          if (candidates.size() == maximumCandidates) {
            exhausted = false;
            return CompletableFuture.<Void>completedFuture(null);
          }
          return visitCandidates(references, index + 1);
        } catch (RuntimeException | Error problem) {
          return failedCandidate(lease, problem);
        }
      }).thenCompose(stage -> stage);
    }

    private CompletableFuture<Void> failedCandidate(BudgetManager.Lease lease, Throwable failure) {
      releaseCandidateLease(lease, failure);
      return CompletableFuture.failedFuture(failure);
    }

    private void releaseCandidateLease(BudgetManager.Lease lease, Throwable failure) {
      if (scope.isClosed()) {
        // Cancellation can complete the request result before HBase has stopped its physical
        // producer. Keep the candidate-buffer charge until every producer owned by this scope
        // has actually stopped.
        scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) ->
            closeCandidateLease(lease, failure));
        return;
      }
      closeCandidateLease(lease, failure);
    }

    private void closeCandidateLease(BudgetManager.Lease lease, Throwable failure) {
      try {
        lease.close();
      } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
    }

    private boolean mayIntersect(ManifestChildReference reference) {
      if (bucket < reference.firstBucket() || bucket > reference.lastBucket()) return false;
      if (reference.firstBucket() != reference.lastBucket()) return true;
      if (after.filter(previous -> reference.lastKey().compareTo(previous) <= 0).isPresent()) {
        return false;
      }
      if (range.kind() == KeyRange.Kind.POINT) {
        ByteKey key = range.lowerInclusive().orElseThrow();
        return reference.firstKey().compareTo(key) <= 0 && reference.lastKey().compareTo(key) >= 0;
      }
      return range.upperExclusive().map(upper -> reference.firstKey().compareTo(upper) < 0).orElse(true)
          && range.lowerInclusive().map(lower -> reference.lastKey().compareTo(lower) >= 0).orElse(true);
    }

    private boolean stopped() {
      return !exhausted;
    }

    private CandidateSlice slice() {
      List<PreparedCandidate> transferredCandidates = List.copyOf(candidates);
      List<BudgetManager.Lease> transferredLeases = List.copyOf(leases);
      candidates.clear();
      leases.clear();
      return new CandidateSlice(transferredCandidates, exhausted, directoryPages, leafPages,
          payloadBytes, transferredLeases);
    }

    private Throwable releaseAfterFailure(Throwable failure) {
      candidates.clear();
      for (BudgetManager.Lease lease : List.copyOf(leases)) {
        releaseCandidateLease(lease, failure);
      }
      leases.clear();
      return failure;
    }
  }

  private final class CandidateCursor {
    private final ManifestRoot root;
    private final RequestScope scope;
    private final Function<PreparedCandidate, CompletableFuture<Void>> consumer;
    private long count;
    private long valueBytes;
    private ByteKey lastKey;
    private ByteKey firstKey;
    private int lastBucket = -1;
    private int firstBucket = -1;
    private int leafPages;
    private int directoryPages;

    private CandidateCursor(ManifestRoot root, RequestScope scope,
        Function<PreparedCandidate, CompletableFuture<Void>> consumer) {
      this.root = root;
      this.scope = Objects.requireNonNull(scope, "scope");
      this.consumer = consumer;
    }

    private CompletableFuture<Void> visit(ManifestChildReference reference) {
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES) {
        return invalid("manifest directory exceeds its traversal budget");
      }
      return repository.readDirectoryPage(reference.identity(), scope).thenCompose(page -> {
        ManifestDirectoryPage directory = page.orElseThrow(() -> missing("manifest directory page"));
        if (!directory.transactionId().equals(root.transactionId())
            || !directory.reference().equals(reference)) {
          return invalid("manifest directory page does not match its sealed reference");
        }
        return visitChildren(directory.children(), 0);
      });
    }

    private CompletableFuture<Void> visitLeaf(ManifestChildReference reference) {
      if (++leafPages > MAX_CANDIDATE_PAGES) {
        return invalid("manifest leaf pages exceed their traversal budget");
      }
      return repository.readPage(reference.identity(), scope).thenCompose(page -> {
        ManifestPage leaf = page.orElseThrow(() -> missing("manifest leaf page"));
        if (!leaf.transactionId().equals(root.transactionId()) || !leaf.reference().equals(reference)) {
          return invalid("manifest leaf page does not match its sealed reference");
        }
        return visitCandidates(leaf.candidates(), 0);
      });
    }

    private CompletableFuture<Void> visitChildren(List<ManifestChildReference> children, int index) {
      if (index == children.size()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, index + 1));
    }

    private CompletableFuture<Void> visitCandidates(List<ManifestCandidateReference> refs, int index) {
      if (index == refs.size()) return CompletableFuture.completedFuture(null);
      ManifestCandidateReference reference = refs.get(index);
      if (lastBucket > reference.bucket() || lastBucket == reference.bucket() && lastKey != null
          && lastKey.compareTo(reference.logicalKey()) >= 0) {
        return invalid("manifest candidate key order overlaps across pages");
      }
      return repository.readCandidate(reference.candidateIdentity(), scope).thenCompose(value -> {
        PreparedCandidate candidate = value.orElseThrow(() -> missing("private candidate"));
        if (!candidateMatchesReference(root, reference, candidate)) {
          return invalid("private candidate differs from its sealed manifest leaf");
        }
        try {
          count = Math.addExact(count, 1);
          valueBytes = Math.addExact(valueBytes, reference.valueLength());
          if (count > root.candidateCount() || valueBytes > root.totalValueBytes()) {
            return invalid("candidate traversal exceeds sealed root aggregates");
          }
          lastKey = reference.logicalKey();
          lastBucket = reference.bucket();
          if (firstKey == null) {
            firstBucket = reference.bucket();
            firstKey = reference.logicalKey();
          }
        } catch (ArithmeticException overflow) {
          return invalid("candidate traversal aggregate overflowed");
        }
        return consumer.apply(candidate).thenCompose(ignored -> visitCandidates(refs, index + 1));
      });
    }

    private Summary summary() {
      if (count != root.candidateCount() || valueBytes != root.totalValueBytes()
          || leafPages != root.candidateDirectory().orElseThrow().leafPageCount()
          || directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES
          || firstBucket != root.candidateDirectory().orElseThrow().firstBucket()
          || lastBucket != root.candidateDirectory().orElseThrow().lastBucket()
          || !firstKey.equals(root.candidateDirectory().orElseThrow().firstKey())
          || !lastKey.equals(root.candidateDirectory().orElseThrow().lastKey())) {
        throw new IllegalStateException("manifest traversal does not cover sealed candidate aggregates");
      }
      return new Summary(count, valueBytes);
    }
  }

  private final class EventCursor {
    private final ManifestRoot root;
    private final RequestScope scope;
    private final Function<PreparedEvent, CompletableFuture<Void>> consumer;
    private long count;
    private long bytes;
    private long nextOrdinal;
    private int leafPages;
    private int directoryPages;

    private EventCursor(ManifestRoot root, RequestScope scope,
        Function<PreparedEvent, CompletableFuture<Void>> consumer) {
      this.root = root;
      this.scope = Objects.requireNonNull(scope, "scope");
      this.consumer = consumer;
    }

    private CompletableFuture<Void> visit(EventReference reference) {
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_EVENT_DIRECTORY_PAGES) {
        return invalid("event directory exceeds its traversal budget");
      }
      return repository.readEventDirectoryPage(reference.identity(), scope).thenCompose(page -> {
        EventDirectoryPage directory = page.orElseThrow(() -> missing("event directory page"));
        if (!directory.transactionId().equals(root.transactionId())
            || !directory.reference().equals(reference)) {
          return invalid("event directory page does not match its sealed reference");
        }
        return visitChildren(directory.children(), 0);
      });
    }

    private CompletableFuture<Void> visitLeaf(EventReference reference) {
      if (++leafPages > MAX_EVENT_PAGES) {
        return invalid("event leaf pages exceed their traversal budget");
      }
      return repository.readEventPage(reference.identity(), scope).thenCompose(page -> {
        EventPage leaf = page.orElseThrow(() -> missing("event leaf page"));
        if (!leaf.transactionId().equals(root.transactionId()) || !leaf.reference().equals(reference)) {
          return invalid("event leaf page does not match its sealed reference");
        }
        return visitEvents(leaf.events(), 0);
      });
    }

    private CompletableFuture<Void> visitChildren(List<EventReference> children, int index) {
      if (index == children.size()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, index + 1));
    }

    private CompletableFuture<Void> visitEvents(List<PreparedEventReference> refs, int index) {
      if (index == refs.size()) return CompletableFuture.completedFuture(null);
      PreparedEventReference reference = refs.get(index);
      if (reference.ordinal() != nextOrdinal) return invalid("event manifest ordinals contain a gap");
      return repository.readEvent(reference.identity(), scope).thenCompose(value -> {
        PreparedEvent event = value.orElseThrow(() -> missing("prepared event"));
        if (!event.transactionId().equals(root.transactionId()) || event.ownerEpoch() != root.ownerEpoch()
            || event.baseSequence() != root.baseSequence() || event.baseRevision() != root.baseRevision()
            || !event.systemIdentity().equals(reference.identity())
            || event.ordinal() != reference.ordinal() || !event.digest().equals(reference.digest())
            || event.retainedBytes() != reference.encodedBytes()) {
          return invalid("prepared event differs from its sealed manifest reference");
        }
        count = Math.addExact(count, 1);
        bytes = Math.addExact(bytes, reference.encodedBytes());
        nextOrdinal++;
        if (count > root.eventCount() || bytes > root.totalEventBytes()) {
          return invalid("event traversal exceeds sealed root aggregates");
        }
        return consumer.apply(event).thenCompose(ignored -> visitEvents(refs, index + 1));
      });
    }

    private Summary summary() {
      if (count != root.eventCount() || bytes != root.totalEventBytes()
          || leafPages != root.eventDirectory().orElseThrow().leafPageCount()
          || directoryPages > MAX_EVENT_DIRECTORY_PAGES) {
        throw new IllegalStateException("manifest traversal does not cover sealed event aggregates");
      }
      return new Summary(count, bytes);
    }
  }

  private static IllegalStateException missing(String item) {
    return new IllegalStateException(item + " referenced by a sealed manifest is missing");
  }

  private static <T> CompletableFuture<T> invalid(String message) {
    return CompletableFuture.failedFuture(new IllegalStateException(message));
  }
}
