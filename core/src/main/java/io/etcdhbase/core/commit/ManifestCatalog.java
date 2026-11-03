package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Verifies and streams a sealed candidate/event manifest without collecting transaction data. */
public final class ManifestCatalog {
  public record Summary(long count, long payloadBytes) {
    public Summary {
      if (count < 0 || payloadBytes < 0) throw new IllegalArgumentException("negative manifest summary");
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
        if (!candidate.transactionId().equals(root.transactionId())
            || candidate.ownerEpoch() != root.ownerEpoch()
            || candidate.baseSequence() != root.baseSequence()
            || candidate.baseRevision() != root.baseRevision()
            || !candidate.systemIdentity().equals(reference.candidateIdentity())
            || repository.candidateBucket(candidate) != reference.bucket()
            || !candidate.record().key().equals(reference.logicalKey())
            || !candidate.contentDigest().equals(reference.candidateDigest())
            || !candidate.valueDigest().equals(reference.valueDigest())
            || candidate.record().metadata().valueLength() != reference.valueLength()
            || (candidate.record().metadata().state()
                == io.etcdhbase.core.storage.StorageMetadata.State.TOMBSTONE) != reference.tombstone()) {
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
