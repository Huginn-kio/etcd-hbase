package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Reads one authenticated sealed event at a time. The caller must separately prove that the
 * decision was selected and keep its event history protected for this cursor's lifetime.
 */
public final class SealedEventCursor implements AutoCloseable {
  private final ManifestRepository repository;
  private final ManifestRoot root;
  private final RequestScope scope;
  private long nextOrdinal;
  private long bytes;
  private boolean inFlight;
  private boolean closed;

  public SealedEventCursor(ManifestRepository repository, CommitDecision decision,
      ManifestRoot root, RequestScope scope) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.root = Objects.requireNonNull(root, "root");
    this.scope = Objects.requireNonNull(scope, "scope");
    EventRevisionIndex.fromSelected(decision, root);
  }

  /** Each call consumes a bounded proof budget; callers can pause by delaying the next call. */
  public synchronized CompletableFuture<Optional<PreparedEvent>> next(
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(budget, "budget");
    if (!budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event budget belongs to another scope"));
    }
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(inactive());
    }
    if (inFlight) return CompletableFuture.failedFuture(new IllegalStateException("event read already in flight"));
    if (nextOrdinal == root.eventCount()) return CompletableFuture.completedFuture(Optional.empty());
    inFlight = true;
    long ordinal = nextOrdinal;
    CompletableFuture<Optional<PreparedEvent>> result = readReference(
        root.eventDirectory().orElseThrow(), ordinal, budget)
        .thenCompose(reference -> readEvent(reference, budget))
        .thenApply(this::accept);
    return result.whenComplete((ignored, failure) -> clearInFlight());
  }

  private CompletableFuture<PreparedEventReference> readReference(EventReference reference,
      long ordinal, ManifestCatalog.ProofReadBudget budget) {
    if (ordinal < reference.firstOrdinal() || ordinal > reference.lastOrdinal()) {
      return CompletableFuture.failedFuture(new IllegalStateException("event ordinal is outside sealed range"));
    }
    if (reference.level() == 0) {
      return budget.read(ManifestCatalog.maximumProofReadPageBytes(),
          () -> repository.readEventPage(reference.identity(), scope)).thenApply(found -> {
        EventPage page = found.orElseThrow(() -> new IllegalStateException("sealed event page is missing"));
        if (!page.transactionId().equals(root.transactionId()) || !page.reference().equals(reference)) {
          throw new IllegalStateException("event page differs from its sealed reference");
        }
        return page.events().get(Math.toIntExact(ordinal - page.firstOrdinal()));
      });
    }
    return budget.read(ManifestCatalog.maximumProofReadPageBytes(),
        () -> repository.readEventDirectoryPage(reference.identity(), scope)).thenCompose(found -> {
      EventDirectoryPage directory = found.orElseThrow(
          () -> new IllegalStateException("sealed event directory is missing"));
      if (!directory.transactionId().equals(root.transactionId()) || !directory.reference().equals(reference)) {
        return CompletableFuture.failedFuture(new IllegalStateException("event directory differs from its sealed reference"));
      }
      EventReference child = directory.children().stream()
          .filter(item -> ordinal >= item.firstOrdinal() && ordinal <= item.lastOrdinal())
          .findFirst().orElseThrow(() -> new IllegalStateException("sealed event directory has an ordinal gap"));
      return readReference(child, ordinal, budget);
    });
  }

  private CompletableFuture<PreparedEvent> readEvent(PreparedEventReference reference,
      ManifestCatalog.ProofReadBudget budget) {
    return budget.read(ManifestCatalog.maximumProofReadPageBytes(),
        () -> repository.readEvent(reference.identity(), scope)).thenApply(found -> {
      PreparedEvent event = found.orElseThrow(() -> new IllegalStateException("sealed event is missing"));
      if (!event.transactionId().equals(root.transactionId()) || event.ownerEpoch() != root.ownerEpoch()
          || event.baseSequence() != root.baseSequence() || event.baseRevision() != root.baseRevision()
          || !event.systemIdentity().equals(reference.identity()) || event.ordinal() != reference.ordinal()
          || !event.digest().equals(reference.digest()) || event.retainedBytes() != reference.encodedBytes()) {
        throw new IllegalStateException("event differs from its sealed reference");
      }
      return event;
    });
  }

  private synchronized Optional<PreparedEvent> accept(PreparedEvent event) {
    if (closed || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new java.util.concurrent.CompletionException(inactive());
    }
    if (event.ordinal() != nextOrdinal) throw new IllegalStateException("event ordinals are not contiguous");
    long total = Math.addExact(bytes, event.retainedBytes());
    long next = Math.addExact(nextOrdinal, 1);
    if (total > root.totalEventBytes() || next == root.eventCount() && total != root.totalEventBytes()) {
      throw new IllegalStateException("event bytes differ from sealed manifest aggregates");
    }
    bytes = total;
    nextOrdinal = next;
    return Optional.of(event);
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private synchronized void clearInFlight() { inFlight = false; }

  @Override public synchronized void close() { closed = true; }
}
