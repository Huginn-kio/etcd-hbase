package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Sequential, bounded writer for private candidates, ordered events, and Merkle directories. */
public final class ManifestPreparation {
  /** Queue for nested continuations currently draining on this thread. */
  private static final ThreadLocal<ArrayDeque<Runnable>> CONTINUATION_QUEUE =
      new ThreadLocal<>();
  /** Runs queued continuations iteratively on the completing thread. */
  private static final Executor CONTINUATION_EXECUTOR = command -> {
    ArrayDeque<Runnable> queue = CONTINUATION_QUEUE.get();
    if (queue != null) {
      queue.addLast(command);
      return;
    }
    queue = new ArrayDeque<>();
    CONTINUATION_QUEUE.set(queue);
    try {
      queue.addLast(command);
      Runnable next;
      while ((next = queue.pollFirst()) != null) {
        next.run();
      }
    } finally {
      queue.clear();
      CONTINUATION_QUEUE.remove();
    }
  };

  public record Limits(long maxCandidates, long maxValueBytes, long maxEvents, long maxEventBytes) {
    public Limits(long maxCandidates, long maxValueBytes) {
      this(maxCandidates, maxValueBytes, ManifestRoot.MAX_EVENTS, ManifestRoot.MAX_EVENT_BYTES);
    }

    public Limits {
      if (maxCandidates < 0 || maxCandidates > ManifestRoot.MAX_CANDIDATES
          || maxValueBytes < 0 || maxValueBytes > ManifestRoot.MAX_VALUE_BYTES
          || maxEvents < 0 || maxEvents > ManifestRoot.MAX_EVENTS
          || maxEventBytes < 0 || maxEventBytes > ManifestRoot.MAX_EVENT_BYTES) {
        throw new IllegalArgumentException("manifest limits exceed the storage contract");
      }
    }
  }

  private final ManifestRepository repository;
  private final Limits limits;

  public ManifestPreparation(ManifestRepository repository, Limits limits) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.limits = Objects.requireNonNull(limits, "limits");
  }

  public CompletableFuture<ManifestRoot> prepare(
      TransactionId transactionId,
      long ownerEpoch,
      long baseSequence,
      long baseRevision,
      Iterator<PreparedCandidate> candidates,
      RequestScope scope) {
    return prepare(transactionId, ownerEpoch, baseSequence, baseRevision, candidates,
        java.util.Collections.emptyIterator(), scope);
  }

  public CompletableFuture<ManifestRoot> prepare(
      TransactionId transactionId,
      long ownerEpoch,
      long baseSequence,
      long baseRevision,
      Iterator<PreparedCandidate> candidates,
      Iterator<PreparedEvent> events,
      RequestScope scope) {
    Objects.requireNonNull(candidates, "candidates");
    Objects.requireNonNull(events, "events");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid manifest base"));
    }
    return new Writer(transactionId, ownerEpoch, baseSequence, baseRevision, candidates, events, scope).next();
  }

  private final class Writer {
    private final TransactionId transactionId;
    private final long ownerEpoch;
    private final long baseSequence;
    private final long baseRevision;
    private final Iterator<PreparedCandidate> input;
    private final Iterator<PreparedEvent> eventInput;
    private final RequestScope scope;
    private final List<ManifestCandidateReference> leaf = new ArrayList<>(ManifestPage.MAX_ENTRIES);
    private final List<List<ManifestChildReference>> levels = new ArrayList<>(ManifestDirectoryPage.MAX_DEPTH + 1);
    private long count;
    private long valueBytes;
    private int leafPageIndex;
    private int directoryPageIndex;
    private int leafBytes = 64;
    private int leafBucket = -1;
    private int previousBucket = -1;
    private ByteKey previousKey;

    private Writer(TransactionId transactionId, long ownerEpoch, long baseSequence, long baseRevision,
        Iterator<PreparedCandidate> input, Iterator<PreparedEvent> eventInput, RequestScope scope) {
      this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
      this.ownerEpoch = ownerEpoch;
      this.baseSequence = baseSequence;
      this.baseRevision = baseRevision;
      this.input = input;
      this.eventInput = eventInput;
      this.scope = Objects.requireNonNull(scope, "scope");
      for (int level = 0; level <= ManifestDirectoryPage.MAX_DEPTH; level++) {
        levels.add(new ArrayList<>(ManifestDirectoryPage.MAX_CHILDREN));
      }
    }

    private CompletableFuture<ManifestRoot> next() {
      try {
        if (scope.isClosed()) return failed(new BackendFailure(BackendFailure.Category.CANCELLED));
        if (scope.isDeadlineExceeded()) return failed(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (!input.hasNext()) return flushLeaf().thenCompose(ignored -> finishDirectories());
        PreparedCandidate candidate = Objects.requireNonNull(input.next(), "candidate");
        int bucket = repository.candidateBucket(candidate);
        validateCandidate(candidate, bucket);
        int bytes = referenceBytes(candidate);
        if (!leaf.isEmpty() && (leaf.size() == ManifestPage.MAX_ENTRIES
            || leafBucket != bucket || leafBytes + bytes > ManifestPage.MAX_PAGE_BYTES)) {
          return flushLeaf().thenCompose(ignored -> accept(candidate, bucket));
        }
        return accept(candidate, bucket);
      } catch (RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
    }

    private CompletableFuture<ManifestRoot> accept(PreparedCandidate candidate, int bucket) {
      checkCandidateBudget(candidate);
      return repository.createCandidate(candidate, scope)
          .thenComposeAsync(result -> {
        requireCreatedOrIdentical(result, "candidate");
        ManifestCandidateReference reference = ManifestCandidateReference.from(candidate, bucket);
        leaf.add(reference);
        leafBytes = Math.addExact(leafBytes, referenceBytes(candidate));
        leafBucket = bucket;
        count = Math.addExact(count, 1);
        valueBytes = Math.addExact(valueBytes, reference.valueLength());
        previousBucket = bucket;
        previousKey = candidate.record().key();
        return next();
      }, CONTINUATION_EXECUTOR);
    }

    private CompletableFuture<Void> flushLeaf() {
      if (leaf.isEmpty()) return CompletableFuture.completedFuture(null);
      ManifestPage page = ManifestPage.create(transactionId, leafPageIndex++, List.copyOf(leaf));
      return repository.createPage(page, scope).thenCompose(result -> {
        requireCreatedOrIdentical(result, "manifest page");
        leaf.clear();
        leafBytes = 64;
        leafBucket = -1;
        return addReference(page.reference(), 0);
      });
    }

    private CompletableFuture<Void> addReference(ManifestChildReference reference, int level) {
      if (level > ManifestDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      List<ManifestChildReference> buffer = levels.get(level);
      buffer.add(reference);
      if (buffer.size() < ManifestDirectoryPage.MAX_CHILDREN) {
        return CompletableFuture.completedFuture(null);
      }
      return writeDirectory(level, buffer).thenCompose(parent -> addReference(parent.reference(), level + 1));
    }

    private CompletableFuture<ManifestDirectoryPage> writeDirectory(
        int childLevel, List<ManifestChildReference> references) {
      if (childLevel >= ManifestDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      ManifestDirectoryPage page = ManifestDirectoryPage.create(transactionId,
          directoryPageIndex++, childLevel + 1, List.copyOf(references));
      return repository.createDirectoryPage(page, scope).thenApply(result -> {
        requireCreatedOrIdentical(result, "manifest directory page");
        references.clear();
        return page;
      });
    }

    private CompletableFuture<ManifestRoot> finishDirectories() {
      return finishLevel(0).thenCompose(candidateRoot ->
          new EventWriter(transactionId, ownerEpoch, baseSequence, baseRevision, eventInput, scope)
              .write().thenCompose(events -> {
        ManifestRoot root = ManifestRoot.create(transactionId, ownerEpoch, baseSequence, baseRevision,
            count, valueBytes, Optional.ofNullable(candidateRoot), events.count(), events.bytes(),
            Optional.ofNullable(events.root()));
        return repository.seal(root, scope).thenApply(result -> {
          requireCreatedOrIdentical(result, "sealed manifest root");
          return root;
        });
      }));
    }

    private CompletableFuture<ManifestChildReference> finishLevel(int level) {
      if (level > ManifestDirectoryPage.MAX_DEPTH) {
        boolean anyRemaining = levels.stream().anyMatch(buffer -> !buffer.isEmpty());
        return anyRemaining
            ? failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT))
            : CompletableFuture.completedFuture(null);
      }
      List<ManifestChildReference> current = levels.get(level);
      if (current.isEmpty()) return finishLevel(level + 1);
      boolean higherHasEntries = false;
      for (int index = level + 1; index < levels.size(); index++) {
        higherHasEntries |= !levels.get(index).isEmpty();
      }
      if (current.size() == 1 && !higherHasEntries) {
        return CompletableFuture.completedFuture(current.getFirst());
      }
      return writeDirectory(level, current).thenCompose(page -> {
        if (level + 1 == levels.size()) {
          return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        }
        levels.get(level + 1).add(page.reference());
        return finishLevel(level + 1);
      });
    }

    private void validateCandidate(PreparedCandidate candidate, int bucket) {
      if (!candidate.transactionId().equals(transactionId) || candidate.ownerEpoch() != ownerEpoch
          || candidate.baseSequence() != baseSequence || candidate.baseRevision() != baseRevision
          || bucket < 0) {
        throw new IllegalArgumentException("candidate belongs to a different commit attempt");
      }
      if (previousBucket > bucket || previousBucket == bucket && previousKey != null
          && previousKey.compareTo(candidate.record().key()) >= 0) {
        throw new IllegalArgumentException("candidates must be sorted by bucket and logical key");
      }
    }

    private void checkCandidateBudget(PreparedCandidate candidate) {
      long nextCount;
      long nextValueBytes;
      try {
        nextCount = Math.addExact(count, 1);
        nextValueBytes = Math.addExact(valueBytes, candidate.record().metadata().valueLength());
      } catch (ArithmeticException overflow) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (nextCount > limits.maxCandidates() || nextValueBytes > limits.maxValueBytes()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
    }

    private int referenceBytes(PreparedCandidate candidate) {
      long bytes = 64L + 5L * Integer.BYTES + candidate.record().key().length()
          + candidate.systemIdentity().length() + 64L + 1;
      if (bytes > ManifestPage.MAX_PAGE_BYTES) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      return (int) bytes;
    }
  }

  private final class EventWriter {
    private final TransactionId transactionId;
    private final long ownerEpoch;
    private final long baseSequence;
    private final long baseRevision;
    private final Iterator<PreparedEvent> input;
    private final RequestScope scope;
    private final List<PreparedEventReference> leaf = new ArrayList<>(EventPage.MAX_EVENTS);
    private final List<List<EventReference>> levels = new ArrayList<>(EventDirectoryPage.MAX_DEPTH + 1);
    private long count;
    private long totalBytes;
    private long nextOrdinal;
    private int pageBytes = 64;
    private int eventPageIndex;
    private int directoryPageIndex;

    private EventWriter(TransactionId transactionId, long ownerEpoch, long baseSequence,
        long baseRevision, Iterator<PreparedEvent> input, RequestScope scope) {
      this.transactionId = transactionId;
      this.ownerEpoch = ownerEpoch;
      this.baseSequence = baseSequence;
      this.baseRevision = baseRevision;
      this.input = input;
      this.scope = scope;
      for (int level = 0; level <= EventDirectoryPage.MAX_DEPTH; level++) {
        levels.add(new ArrayList<>(EventDirectoryPage.MAX_CHILDREN));
      }
    }

    private CompletableFuture<EventSummary> write() { return next(); }

    private CompletableFuture<EventSummary> next() {
      try {
        if (scope.isClosed()) return failed(new BackendFailure(BackendFailure.Category.CANCELLED));
        if (scope.isDeadlineExceeded()) return failed(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (!input.hasNext()) return flushLeaf().thenCompose(ignored -> finishDirectories());
        PreparedEvent event = Objects.requireNonNull(input.next(), "event");
        validate(event);
        int referenceBytes = 64 + event.systemIdentity().length() + 32 + Integer.BYTES;
        if (!leaf.isEmpty() && (leaf.size() == EventPage.MAX_EVENTS
            || pageBytes + referenceBytes > EventPage.MAX_PAGE_BYTES)) {
          return flushLeaf().thenCompose(ignored -> accept(event));
        }
        return accept(event);
      } catch (RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
    }

    private CompletableFuture<EventSummary> accept(PreparedEvent event) {
      return repository.createEvent(event, scope).thenComposeAsync(result -> {
        requireCreatedOrIdentical(result, "event");
        PreparedEventReference reference = PreparedEventReference.from(event);
        leaf.add(reference);
        pageBytes = Math.addExact(pageBytes,
            64 + reference.identity().length() + reference.digest().length() + Integer.BYTES);
        count = Math.addExact(count, 1);
        totalBytes = Math.addExact(totalBytes, reference.encodedBytes());
        nextOrdinal++;
        return next();
      }, CONTINUATION_EXECUTOR);
    }

    private CompletableFuture<Void> flushLeaf() {
      if (leaf.isEmpty()) return CompletableFuture.completedFuture(null);
      EventPage page = EventPage.create(transactionId, eventPageIndex++, List.copyOf(leaf));
      return repository.createEventPage(page, scope).thenCompose(result -> {
        requireCreatedOrIdentical(result, "event page");
        leaf.clear();
        pageBytes = 64;
        return addReference(page.reference(), 0);
      });
    }

    private CompletableFuture<Void> addReference(EventReference reference, int level) {
      if (level > EventDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      List<EventReference> buffer = levels.get(level);
      buffer.add(reference);
      if (buffer.size() < EventDirectoryPage.MAX_CHILDREN) {
        return CompletableFuture.completedFuture(null);
      }
      return writeDirectory(level, buffer)
          .thenCompose(parent -> addReference(parent.reference(), level + 1));
    }

    private CompletableFuture<EventDirectoryPage> writeDirectory(
        int childLevel, List<EventReference> references) {
      if (childLevel >= EventDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      EventDirectoryPage page = EventDirectoryPage.create(transactionId,
          directoryPageIndex++, childLevel + 1, List.copyOf(references));
      return repository.createEventDirectoryPage(page, scope).thenApply(result -> {
        requireCreatedOrIdentical(result, "event directory page");
        references.clear();
        return page;
      });
    }

    private CompletableFuture<EventSummary> finishDirectories() {
      return finishLevel(0).thenApply(root -> new EventSummary(count, totalBytes, root));
    }

    private CompletableFuture<EventReference> finishLevel(int level) {
      if (level > EventDirectoryPage.MAX_DEPTH) {
        boolean remaining = levels.stream().anyMatch(buffer -> !buffer.isEmpty());
        return remaining ? failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT))
            : CompletableFuture.completedFuture(null);
      }
      List<EventReference> current = levels.get(level);
      if (current.isEmpty()) return finishLevel(level + 1);
      boolean higherHasEntries = false;
      for (int index = level + 1; index < levels.size(); index++) {
        higherHasEntries |= !levels.get(index).isEmpty();
      }
      if (current.size() == 1 && !higherHasEntries) {
        return CompletableFuture.completedFuture(current.getFirst());
      }
      return writeDirectory(level, current).thenCompose(page -> {
        if (level + 1 == levels.size()) return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        levels.get(level + 1).add(page.reference());
        return finishLevel(level + 1);
      });
    }

    private void validate(PreparedEvent event) {
      if (!event.transactionId().equals(transactionId) || event.ownerEpoch() != ownerEpoch
          || event.baseSequence() != baseSequence || event.baseRevision() != baseRevision
          || event.ordinal() != nextOrdinal) {
        throw new IllegalArgumentException("events must be contiguous and belong to this commit attempt");
      }
      long nextCount;
      long nextBytes;
      try {
        nextCount = Math.addExact(count, 1);
        nextBytes = Math.addExact(totalBytes, event.retainedBytes());
      } catch (ArithmeticException overflow) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (nextCount > limits.maxEvents() || nextBytes > limits.maxEventBytes()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
    }
  }

  private record EventSummary(long count, long bytes, EventReference root) {}

  private static void requireCreatedOrIdentical(SystemRecordStore.CreateResult result, String kind) {
    if (result == SystemRecordStore.CreateResult.CONFLICT) {
      throw new IllegalStateException(kind + " identity already exists with different bytes");
    }
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }
}
