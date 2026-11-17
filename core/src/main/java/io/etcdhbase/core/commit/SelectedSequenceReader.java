package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** Reads the next selected sequence under a durable event-history pin. */
public final class SelectedSequenceReader {
  private final EventHistoryPinManager pins;
  private final CommitControlStore control;
  private final SelectedDecisionReader decisions;
  private final ManifestRepository manifests;
  private final EventRevisionIndexRepository indexes;
  private volatile long observedPublishedSequence;

  public SelectedSequenceReader(EventHistoryPinManager pins, CommitControlStore control,
      CommitDecisionRepository decisions, ManifestRepository manifests,
      EventRevisionIndexRepository indexes) {
    this.pins = Objects.requireNonNull(pins, "pins");
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = new SelectedDecisionReader(control, decisions);
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.indexes = Objects.requireNonNull(indexes, "indexes");
  }

  /** A KV-changing sequence owns an event cursor; metadata-only sequences have none. */
  public record Selected(CommitDecision decision, Optional<ProtectedEventCursor> events,
      Optional<EventRevisionIndex> index) {
    public Selected {
      Objects.requireNonNull(decision, "decision");
      Objects.requireNonNull(events, "events");
      Objects.requireNonNull(index, "index");
      if (events.isPresent() != decision.containsKeyChanges()
          || index.isPresent() != decision.containsKeyChanges()) {
        throw new IllegalArgumentException("event cursor differs from selected decision");
      }
    }
  }

  /** The metadata-only sequence prefix through which the reader proved no KV event is missing. */
  public record ReadAhead(long metadataThroughSequence, Optional<Selected> nextEvent) {
    public ReadAhead {
      Objects.requireNonNull(nextEvent, "nextEvent");
      if (metadataThroughSequence < 0) {
        throw new IllegalArgumentException("metadata sequence watermark must be non-negative");
      }
      nextEvent.ifPresent(selected -> {
        if (selected.decision().sequence() != Math.addExact(metadataThroughSequence, 1)
            || !selected.decision().containsKeyChanges()) {
          throw new IllegalArgumentException("event decision does not follow the metadata watermark");
        }
      });
    }
  }

  private record LocatedEvent(long sequence, Optional<EventRevisionIndex> index) {
    private LocatedEvent {
      Objects.requireNonNull(index, "index");
      if (sequence <= 0) throw new IllegalArgumentException("event sequence must be positive");
    }
  }

  /**
   * The caller supplies the last complete revision and the next sequence from its own verified
   * checkpoint. The scope belongs exclusively to this step. Empty means no sequence was selected
   * at the captured barrier, not that an archived decision or event was missing.
   */
  public CompletableFuture<Optional<Selected>> open(long sequence, long baseRevision,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return open(sequence, baseRevision, authorizationDigest, scope, budget, Optional.empty());
  }

  /**
   * Finds the next event-bearing decision without reading every intervening metadata-only sequence.
   * An unchanged authoritative revision proves that the selected prefix contains no KV events;
   * otherwise the complete revision index must locate the next event before the prefix is skipped.
   */
  public CompletableFuture<ReadAhead> openNext(long completedSequence, long completedRevision,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (completedSequence < 0 || completedRevision <= 0
        || authorizationDigest.length() != 32 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event read-ahead request"));
    }
    CompletableFuture<ReadAhead> readAhead = readControl(scope, budget).thenCompose(state -> readAhead(state, completedSequence,
        completedRevision, authorizationDigest, scope, budget));
    readAhead.whenComplete((ignored, failure) -> {
      if (readAhead.isCancelled()) scope.cancel();
    });
    return readAhead;
  }

  /** Reads the authoritative control state through the same bounded proof-read path as replay. */
  public CompletableFuture<CommitState> readPublishedState(RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (!budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "event replay budget belongs to another scope"));
    }
    return readControl(scope, budget);
  }

  /**
   * Resolves the durable event sequence for one published KV revision. Absence is accepted only
   * for the selected, not-yet-finalized control publication; a finalized missing index fails closed.
   */
  public CompletableFuture<Long> sequenceForRevision(long revision, CommitState barrier,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(barrier, "barrier");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (revision <= 1 || revision > barrier.publishedRevision()
        || !barrier.supportsEventRevisionIndex() || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "invalid published event revision lookup"));
    }
    return readIndex(revision, scope, budget).thenApply(index -> {
      if (index.isPresent()) {
        CommitDecision decision = index.orElseThrow().decision();
        if (decision.revision() != revision || decision.baseRevision() != revision - 1
            || decision.sequence() > barrier.publishedSequence()) {
          throw new IllegalStateException("event revision index is outside its control barrier");
        }
        return decision.sequence();
      }
      if (barrier.finalizedRevision() < revision) {
        return pendingEventSequence(barrier, revision - 1, revision);
      }
      throw new IllegalStateException("finalized event revision index is missing");
    });
  }

  private CompletableFuture<ReadAhead> readAhead(CommitState state, long completedSequence,
      long completedRevision, ByteKey authorizationDigest, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    try {
      validateControl(state, completedRevision, Optional.empty());
      if (state.publishedSequence() < completedSequence
          || state.publishedRevision() < completedRevision) {
        throw new IllegalStateException("event tailer cursor is ahead of the control barrier");
      }
      if (state.publishedSequence() == completedSequence) {
        if (state.publishedRevision() != completedRevision) {
          throw new IllegalStateException("event tailer revision differs at an equal sequence");
        }
        return CompletableFuture.completedFuture(new ReadAhead(completedSequence, Optional.empty()));
      }
      if (state.publishedRevision() == completedRevision) {
        if (state.publication().filter(PublicationSlot::containsKeyChanges).isPresent()) {
          throw new IllegalStateException("control revision hides a selected KV change");
        }
        return CompletableFuture.completedFuture(
            new ReadAhead(state.publishedSequence(), Optional.empty()));
      }
      return openNextEvent(completedSequence, completedRevision, authorizationDigest, scope, budget);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<ReadAhead> openNextEvent(long completedSequence, long completedRevision,
      ByteKey authorizationDigest, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    long nextRevision = nextRevision(completedRevision);
    long minimumSequence = Math.addExact(completedSequence, 1);
    var requirement = new EventHistoryPinManager.Requirement(nextRevision, minimumSequence,
        authorizationDigest, Optional.empty(), Optional.empty());
    return withPin(requirement, scope, (protectedCursor, pin) -> protectedCursor.renew()
        .thenCompose(ignored -> readControl(scope, budget))
        .thenCompose(current -> {
          validateControl(current, completedRevision, Optional.empty());
          return readIndex(nextRevision, scope, budget).thenCompose(index ->
              protectedCursor.renew().thenApply(ignored -> locateNextEvent(index, current,
                  completedSequence, completedRevision, nextRevision)));
        })
        .thenCompose(located -> read(located.sequence(), completedRevision, protectedCursor,
            pin, requirement, scope, budget, Optional.empty(), located.index()).thenApply(found -> {
              Selected selected = found.orElseThrow(
                  () -> new IllegalStateException("indexed event is not selected by the control barrier"));
              if (selected.decision().sequence() != located.sequence()
                  || selected.decision().baseRevision() != completedRevision
                  || !selected.decision().containsKeyChanges()) {
                throw new IllegalStateException("selected next event differs from its locator");
              }
              return new ReadAhead(located.sequence() - 1, Optional.of(selected));
            })));
  }

  private LocatedEvent locateNextEvent(Optional<EventRevisionIndex> index, CommitState state,
      long completedSequence, long completedRevision, long nextRevision) {
    long eventSequence;
    if (index.isPresent()) {
      EventRevisionIndex located = index.orElseThrow();
      CommitDecision decision = located.decision();
      if (decision.revision() != nextRevision || decision.baseRevision() != completedRevision
          || decision.sequence() <= completedSequence
          || decision.sequence() > state.publishedSequence()) {
        throw new IllegalStateException("next event index is outside the captured control barrier");
      }
      eventSequence = decision.sequence();
    } else if (state.finalizedRevision() < nextRevision) {
      eventSequence = pendingEventSequence(state, completedRevision, nextRevision);
    } else {
      throw new IllegalStateException("finalized next event revision index is missing");
    }
    if (eventSequence <= completedSequence) {
      throw new IllegalStateException("next event sequence is behind the tailer cursor");
    }
    return new LocatedEvent(eventSequence, index);
  }

  private long pendingEventSequence(CommitState state, long completedRevision, long nextRevision) {
    PublicationSlot slot = state.publication().orElseThrow(
        () -> new IllegalStateException("selected next event has no revision index or publication slot"));
    if (state.finalizedRevision() != completedRevision || !slot.containsKeyChanges()
        || slot.baseRevision() != completedRevision || slot.revision() != nextRevision
        || slot.sequence() != state.publishedSequence()) {
      throw new IllegalStateException("pending publication cannot prove the next event gap");
    }
    return slot.sequence();
  }

  /** Latest selected sequence observed by this reader; it is a lag watermark, not a read barrier. */
  public long observedPublishedSequence() { return observedPublishedSequence; }

  /** Seeds the lag watermark from the pinned barrier used to create a tailer. */
  public void observePublishedSequence(long sequence) {
    if (sequence < 0) throw new IllegalArgumentException("published sequence must not be negative");
    observedPublishedSequence = Math.max(observedPublishedSequence, sequence);
  }

  /** Reads one captured legacy sequence; the caller must authenticate the persisted checkpoint. */
  public CompletableFuture<Optional<Selected>> openLegacy(EventIndexBackfillCheckpoint checkpoint,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    if (checkpoint.complete()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event backfill is already complete"));
    }
    if (!checkpoint.registryIdentity().equals(pins.registryIdentity())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event backfill registry differs"));
    }
    return open(checkpoint.nextSequence(), checkpoint.currentRevision(), authorizationDigest,
        scope, budget, Optional.of(checkpoint));
  }

  private CompletableFuture<Optional<Selected>> open(long sequence, long baseRevision,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget,
      Optional<EventIndexBackfillCheckpoint> legacy) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (sequence <= 0 || baseRevision <= 0 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid selected sequence request"));
    }
    var requirement = new EventHistoryPinManager.Requirement(nextRevision(baseRevision), sequence,
        authorizationDigest, Optional.empty(), Optional.empty());
    return withPin(requirement, scope, (protectedCursor, pin) ->
        read(sequence, baseRevision, protectedCursor, pin, requirement, scope, budget, legacy,
            Optional.empty()));
  }

  private <T> CompletableFuture<T> withPin(EventHistoryPinManager.Requirement requirement,
      RequestScope scope,
      BiFunction<ProtectedEventCursor, EventHistoryPinManager.Pin, CompletableFuture<T>> operation) {
    var output = new CompletableFuture<T>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    pins.register(requirement, scope).whenComplete((pin, failure) -> {
      if (failure != null) {
        output.completeExceptionally(failure);
        return;
      }
      var protectedCursor = new ProtectedEventCursor(pin, scope);
      if (output.isDone()) {
        protectedCursor.close();
        return;
      }
      CompletableFuture<T> reading;
      try {
        reading = Objects.requireNonNull(operation.apply(protectedCursor, pin), "pinned event read");
      } catch (RuntimeException | Error problem) {
        protectedCursor.close();
        output.completeExceptionally(problem);
        if (problem instanceof Error fatal) throw fatal;
        return;
      }
      reading.whenComplete((value, readFailure) -> {
        if (readFailure != null) {
          protectedCursor.close();
          output.completeExceptionally(readFailure);
        } else if (!output.complete(value)) {
          protectedCursor.close();
        }
      });
    });
    return output;
  }

  private CompletableFuture<Optional<Selected>> read(long sequence, long baseRevision,
      ProtectedEventCursor protectedCursor, EventHistoryPinManager.Pin pin,
      EventHistoryPinManager.Requirement pinRequirement, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget, Optional<EventIndexBackfillCheckpoint> legacy,
      Optional<EventRevisionIndex> locatedIndex) {
    return protectedCursor.renew().thenCompose(ignored -> readControl(scope, budget)).thenCompose(state -> {
      validateControl(state, baseRevision, legacy);
      if (sequence > state.publishedSequence()) return closeEmpty(protectedCursor);
      return decisions.read(sequence, scope, budget).thenCompose(found -> {
        CommitDecision decision = found.orElseThrow(
            () -> new IllegalStateException("selected decision disappeared from the control barrier"));
        if (decision.sequence() != sequence || decision.baseRevision() != baseRevision) {
          throw new IllegalStateException("selected sequence is not contiguous with the cursor");
        }
        return budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
            () -> manifests.readRootWithSize(decision.manifestRoot(), scope)).thenCompose(foundRoot -> {
          ManifestRoot root = foundRoot.orElseThrow(
              () -> new IllegalStateException("selected manifest root is missing"));
          validateRoot(decision, root);
          CompletableFuture<Void> rootProtection = decision.containsKeyChanges()
              ? promoteEventRoot(decision, root, pin, pinRequirement, protectedCursor, scope)
              : CompletableFuture.completedFuture(null);
          return rootProtection.thenCompose(ignored -> readSelectedIndex(decision, root, scope, budget,
              legacy, locatedIndex)).thenCompose(ignored -> protectedCursor.renew())
              .thenCompose(ignored -> readControl(scope, budget))
              .thenCompose(latest -> {
                validateControl(latest, baseRevision, legacy);
                if (decision.sequence() > latest.publishedSequence()) {
                  throw new IllegalStateException("selected sequence moved behind the control barrier");
                }
                if (!decision.containsKeyChanges()) {
                  protectedCursor.close();
                  return protectedCursor.releaseCompletion().thenApply(released ->
                      Optional.of(new Selected(decision, Optional.empty(), Optional.empty()))).toCompletableFuture();
                }
                protectedCursor.attach(new SealedEventCursor(manifests, decision, root, scope));
                return CompletableFuture.completedFuture(Optional.of(new Selected(decision,
                    Optional.of(protectedCursor), Optional.of(EventRevisionIndex.fromSelected(decision, root)))));
              });
        });
      });
    });
  }

  private CompletableFuture<Void> promoteEventRoot(CommitDecision decision, ManifestRoot root,
      EventHistoryPinManager.Pin pin, EventHistoryPinManager.Requirement previous,
      ProtectedEventCursor protectedCursor, RequestScope scope) {
    EventReference eventRoot = EventRevisionIndex.fromSelected(decision, root).events();
    var promoted = new EventHistoryPinManager.Requirement(previous.revision(), previous.sequence(),
        previous.authorizationDigest(), Optional.of(decision.transactionId()), Optional.of(eventRoot));
    return protectedCursor.renew().thenCompose(ignored ->
        pin.promoteRoot(previous, promoted, scope));
  }

  private CompletableFuture<Void> readSelectedIndex(CommitDecision decision, ManifestRoot root,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget,
      Optional<EventIndexBackfillCheckpoint> legacy, Optional<EventRevisionIndex> locatedIndex) {
    if (!decision.containsKeyChanges()) return CompletableFuture.completedFuture(null);
    if (locatedIndex.isPresent()) {
      locatedIndex.orElseThrow().verifySelected(decision, root);
      return CompletableFuture.completedFuture(null);
    }
    return verifyIndex(decision, root, scope, budget, legacy.isPresent());
  }

  private CompletableFuture<Void> verifyIndex(CommitDecision decision, ManifestRoot root,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget, boolean legacy) {
    return readIndex(decision.revision(), scope, budget).thenCompose(index -> {
      if (index.isPresent()) {
        index.orElseThrow().verifySelected(decision, root);
        return CompletableFuture.completedFuture(null);
      }
      if (legacy) return CompletableFuture.completedFuture(null);
      return readControl(scope, budget).thenCompose(state -> {
        if (state.finalizedSequence() < decision.sequence()) {
          if (state.publication().filter(decision::matches).isEmpty()) {
            throw new IllegalStateException("unfinalized event no longer matches the selected slot");
          }
          return CompletableFuture.completedFuture(null);
        }
        // The first lookup may have raced the finalizer's index write. Re-read after F.
        return readIndex(decision.revision(), scope, budget).thenAccept(afterFinalization -> {
          EventRevisionIndex stored = afterFinalization.orElseThrow(
              () -> new IllegalStateException("finalized event revision index is missing"));
          stored.verifySelected(decision, root);
        });
      });
    });
  }

  private CompletableFuture<Optional<EventRevisionIndex>> readIndex(long revision,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
        () -> indexes.readWithSize(revision, scope));
  }

  private CompletableFuture<CommitState> readControl(RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(), () -> control.readWithSize(scope))
        .thenApply(snapshot -> {
          CommitState state = snapshot.orElseThrow(
              () -> new IllegalStateException("authoritative commit control is missing")).state();
          observePublishedSequence(state.publishedSequence());
          return state;
        });
  }

  private static void validateControl(CommitState state, long baseRevision,
      Optional<EventIndexBackfillCheckpoint> legacy) {
    if (!state.supportsEventRevisionIndex() || baseRevision > state.publishedRevision()) {
      throw new IllegalStateException("sequence cursor is outside the control barrier");
    }
    if (nextRevision(baseRevision) < state.compactRevision()) {
      throw new IllegalStateException("event history has been compacted");
    }
    if (legacy.isPresent() && (state.finalizedSequence() < legacy.orElseThrow().sequence()
        || state.ownerEpoch() < legacy.orElseThrow().ownerEpoch()
        || state.compactRevision() != legacy.orElseThrow().compactRevision())) {
      throw new IllegalStateException("captured event backfill boundary changed");
    }
  }

  private static long nextRevision(long baseRevision) {
    return baseRevision == Long.MAX_VALUE ? Long.MAX_VALUE : baseRevision + 1;
  }

  private static void validateRoot(CommitDecision decision, ManifestRoot root) {
    if (!decision.transactionId().equals(root.transactionId())
        || decision.sequence() - 1 != root.baseSequence()
        || decision.baseRevision() != root.baseRevision()
        || !decision.manifestRoot().equals(root.systemIdentity())
        || !decision.manifestDigest().equals(root.digest())
        || decision.containsKeyChanges() != root.containsKeyChanges()
        || !decision.containsKeyChanges() && (root.eventCount() != 0 || root.eventDirectory().isPresent())) {
      throw new IllegalStateException("selected decision differs from its sealed manifest");
    }
  }

  private static CompletableFuture<Optional<Selected>> closeEmpty(ProtectedEventCursor pin) {
    pin.close();
    return pin.releaseCompletion().thenApply(ignored -> Optional.<Selected>empty()).toCompletableFuture();
  }
}
