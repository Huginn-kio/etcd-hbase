package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** One bounded, shared live-event reader for a single table-group authorization domain. */
public final class SharedEventTailer implements AutoCloseable {
  public enum Step { WAITING, METADATA, OPENED, BUFFERED, PUBLISHED }
  public enum Termination { CANCELLED, RESOURCE_LIMIT, TAILER_CLOSED }

  // Covers each subscription, queue, index registration and collection/tree nodes.
  static final long SUBSCRIPTION_INDEX_ENTRY_OVERHEAD_BYTES = 512;

  /** Last complete cursor position immediately before the subscription was installed. */
  public record SubscriptionStart(Subscription subscription, long sequence, long revision) {
    public SubscriptionStart { Objects.requireNonNull(subscription, "subscription"); }
  }

  /** A bounded read result whose shared-state changes have not yet been applied. */
  public static final class StepProposal {
    private final SharedEventTailer owner;
    private final long expectedSequence;
    private final long expectedRevision;
    private final long sequenceThrough;
    private final Step step;
    private final RequestScope scope;
    private final Optional<SelectedSequenceReader.Selected> selected;
    private final ProtectedEventCursor expectedCursor;
    private final CommitDecision expectedDecision;
    private final ProtectedEventCursor cursor;
    private final CommitDecision decision;
    private final PreparedEvent event;
    private final AtomicBoolean consumed = new AtomicBoolean();

    private StepProposal(SharedEventTailer owner, long expectedSequence, long expectedRevision,
        long sequenceThrough, Step step, RequestScope scope,
        Optional<SelectedSequenceReader.Selected> selected,
        ProtectedEventCursor expectedCursor, CommitDecision expectedDecision,
        ProtectedEventCursor cursor, CommitDecision decision, PreparedEvent event) {
      this.owner = owner;
      this.expectedSequence = expectedSequence;
      this.expectedRevision = expectedRevision;
      this.sequenceThrough = sequenceThrough;
      this.step = step;
      this.scope = scope;
      this.selected = selected;
      this.expectedCursor = expectedCursor;
      this.expectedDecision = expectedDecision;
      this.cursor = cursor;
      this.decision = decision;
      this.event = event;
    }
  }

  private final SelectedSequenceReader source;
  private final ByteKey authorizationDigest;
  private final BudgetManager.Account memory;
  private final int maximumSubscriptions;
  private final int maximumRevisionEvents;
  private final long maximumRevisionBytes;
  private final Set<Subscription> subscriptions = new HashSet<>();
  private final EventSubscriptionIndex<Subscription> subscriptionIndex = new EventSubscriptionIndex<>();
  private final List<Buffered> buffered = new ArrayList<>();
  private long sequence;
  private long revision;
  private long bufferedBytes;
  private int lastPublishedEventCount;
  private long lastPublishedEventBytes;
  private ProtectedEventCursor active;
  private RequestScope activeScope;
  private CommitDecision activeDecision;
  private RequestScope inFlightScope;
  private boolean busy;
  private boolean closed;

  /** Starts immediately after a previously verified complete sequence/revision checkpoint. */
  public SharedEventTailer(SelectedSequenceReader source, ByteKey authorizationDigest,
      BudgetManager.Account memory, long completedSequence, long completedRevision,
      int maximumSubscriptions, int maximumRevisionEvents, long maximumRevisionBytes) {
    this.source = Objects.requireNonNull(source, "source");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.memory = Objects.requireNonNull(memory, "memory");
    if (authorizationDigest.length() != 32 || completedSequence < 0
        || completedRevision <= 0 || maximumSubscriptions < 1 || maximumRevisionEvents < 1
        || maximumRevisionBytes < 1 || maximumRevisionBytes > ManifestRoot.MAX_EVENT_BYTES) {
      throw new IllegalArgumentException("invalid shared tailer checkpoint or limits");
    }
    this.sequence = completedSequence;
    this.revision = completedRevision;
    this.maximumSubscriptions = maximumSubscriptions;
    this.maximumRevisionEvents = maximumRevisionEvents;
    this.maximumRevisionBytes = maximumRevisionBytes;
  }

  public synchronized long completedSequence() { return sequence; }
  public synchronized long completedRevision() { return revision; }
  public synchronized boolean isClosed() { return closed; }
  public synchronized int subscriptionCount() { return subscriptions.size(); }
  public synchronized int lastPublishedEventCount() { return lastPublishedEventCount; }
  public synchronized long lastPublishedEventBytes() { return lastPublishedEventBytes; }
  public long observedPublishedSequence() { return source.observedPublishedSequence(); }

  /** The caller authorizes the range and arranges any history replay before live delivery. */
  public synchronized Subscription subscribe(KeyRange range, BudgetManager.Account account,
      int maximumEvents, long maximumBytes) throws BackendFailure {
    return subscribeWithCheckpoint(range, account, maximumEvents, maximumBytes).subscription();
  }

  /** Installs the subscription and captures the live cursor under the same lock as publication. */
  public synchronized SubscriptionStart subscribeWithCheckpoint(KeyRange range, BudgetManager.Account account,
      int maximumEvents, long maximumBytes) throws BackendFailure {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(account, "account");
    if (closed || subscriptions.size() >= maximumSubscriptions || maximumEvents < 1 || maximumBytes < 1) {
      throw new IllegalStateException("shared tailer cannot admit another subscription");
    }
    long indexBytes = subscriptionIndexMemoryBytes(range);
    BudgetManager.Lease indexMemory = account.tryRetain(BudgetManager.Memory.OUTPUT, indexBytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    var subscription = new Subscription(range, account, maximumEvents, maximumBytes, indexMemory);
    try {
      subscriptions.add(subscription);
      subscriptionIndex.add(subscription, range);
      return new SubscriptionStart(subscription, sequence, revision);
    } catch (RuntimeException | Error failure) {
      subscription.stop(Termination.CANCELLED);
      throw failure;
    }
  }

  private static long subscriptionIndexMemoryBytes(KeyRange range) {
    long bytes = SUBSCRIPTION_INDEX_ENTRY_OVERHEAD_BYTES;
    if (range.lowerInclusive().isPresent()) {
      bytes = Math.addExact(bytes, range.lowerInclusive().orElseThrow().length());
    }
    if (range.kind() == KeyRange.Kind.RANGE && range.upperExclusive().isPresent()) {
      bytes = Math.addExact(bytes, range.upperExclusive().orElseThrow().length());
    }
    return bytes;
  }

  /** Reads at most one selected sequence or event without advancing shared tailer state. */
  public CompletableFuture<StepProposal> prepareStep(RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    ProtectedEventCursor cursor;
    long expectedSequence;
    long expectedRevision;
    CommitDecision decision;
    synchronized (this) {
      if (closed || busy || !budget.belongsTo(scope) || active != null && activeScope != scope) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("shared tailer is closed, busy, or has another scope"));
      }
      busy = true;
      inFlightScope = scope;
      cursor = active;
      decision = activeDecision;
      expectedSequence = sequence;
      expectedRevision = revision;
    }
    CompletableFuture<StepProposal> work;
    try {
      work = cursor == null
          ? open(expectedSequence, expectedRevision, scope, budget)
          : readEvent(expectedSequence, expectedRevision, scope, cursor, decision, budget);
    } catch (RuntimeException | Error failure) {
      failPreparation(scope);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    var response = new CompletableFuture<StepProposal>();
    response.whenComplete((ignored, failure) -> {
      if (response.isCancelled()) {
        scope.cancel();
        close();
      }
    });
    work.whenComplete((proposal, failure) -> {
      if (failure != null) {
        failPreparation(scope);
        response.completeExceptionally(failure);
      } else if (!response.complete(proposal)) {
        discardStep(proposal);
      }
    });
    return response;
  }

  /** Applies a proposal after its caller has revalidated the owner and cursor on the owner lane. */
  public CompletableFuture<Step> applyStep(StepProposal proposal) {
    Objects.requireNonNull(proposal, "proposal");
    if (proposal.owner != this || !proposal.consumed.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("step proposal is foreign or consumed"));
    }
    ProtectedEventCursor completedCursor = null;
    Step result;
    try {
      synchronized (this) {
        requireCurrent(proposal);
        result = apply(proposal);
        if (proposal.step == Step.PUBLISHED) completedCursor = proposal.cursor;
        busy = false;
        inFlightScope = null;
      }
    } catch (BackendFailure failure) {
      return failApply(proposal, failure);
    } catch (RuntimeException | Error failure) {
      return failApply(proposal, failure);
    }
    if (completedCursor == null) return CompletableFuture.completedFuture(result);
    completedCursor.close();
    return completedCursor.releaseCompletion().thenApply(ignored -> result).toCompletableFuture();
  }

  private CompletableFuture<Step> failApply(StepProposal proposal, Throwable failure) {
    return releaseProposal(proposal).handle((ignored, releaseFailure) -> {
      if (releaseFailure != null && failure != releaseFailure) failure.addSuppressed(
          io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(releaseFailure));
      throw new java.util.concurrent.CompletionException(failure);
    });
  }

  /** Discards a stale proposal and closes the cursor so the last complete revision can be reread. */
  public CompletableFuture<Void> discardStep(StepProposal proposal) {
    Objects.requireNonNull(proposal, "proposal");
    if (proposal.owner != this || !proposal.consumed.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    return releaseProposal(proposal);
  }

  private CompletableFuture<Void> releaseProposal(StepProposal proposal) {
    Set<ProtectedEventCursor> cursors = Collections.newSetFromMap(new IdentityHashMap<>());
    proposal.selected.flatMap(SelectedSequenceReader.Selected::events).ifPresent(cursors::add);
    if (proposal.cursor != null) cursors.add(proposal.cursor);
    synchronized (this) {
      if (inFlightScope == proposal.scope) {
        busy = false;
        inFlightScope = null;
      }
    }
    for (ProtectedEventCursor cursor : cursors) cursor.close();
    close();
    if (cursors.isEmpty()) return CompletableFuture.completedFuture(null);
    CompletableFuture<?>[] releases = cursors.stream()
        .map(cursor -> cursor.releaseCompletion().toCompletableFuture())
        .toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(releases);
  }

  /** Compatibility helper for callers that do not need to separate reads from owner-lane state changes. */
  public CompletableFuture<Step> step(RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return prepareStep(scope, budget).thenCompose(this::applyStep);
  }

  private CompletableFuture<StepProposal> open(long expectedSequence, long expectedRevision,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return source.openNext(expectedSequence, expectedRevision, authorizationDigest, scope, budget)
        .thenApply(readAhead -> readAhead.nextEvent().isEmpty()
            ? metadataProposal(expectedSequence, expectedRevision,
                readAhead.metadataThroughSequence(), scope)
            : selectedProposal(expectedSequence, expectedRevision,
                readAhead.metadataThroughSequence(), scope, readAhead.nextEvent().orElseThrow()));
  }

  private StepProposal metadataProposal(long expectedSequence, long expectedRevision,
      long sequenceThrough, RequestScope scope) {
    Step step = sequenceThrough == expectedSequence ? Step.WAITING : Step.METADATA;
    return proposal(expectedSequence, expectedRevision, sequenceThrough, step, scope,
        Optional.empty(), null, null, null, null, null);
  }

  private StepProposal selectedProposal(long expectedSequence, long expectedRevision,
      long sequenceThrough, RequestScope scope, SelectedSequenceReader.Selected selected) {
    return proposal(expectedSequence, expectedRevision, sequenceThrough, Step.OPENED, scope,
        Optional.of(selected), null, null, selected.events().orElseThrow(), selected.decision(), null);
  }

  private CompletableFuture<StepProposal> readEvent(long expectedSequence, long expectedRevision,
      RequestScope scope, ProtectedEventCursor cursor, CommitDecision decision,
      ManifestCatalog.ProofReadBudget budget) {
    return cursor.next(budget).thenCompose(found -> {
      if (found.isPresent()) {
        return CompletableFuture.completedFuture(proposal(expectedSequence, expectedRevision,
            expectedSequence, Step.BUFFERED, scope, Optional.empty(), cursor, decision,
            cursor, decision, found.orElseThrow()));
      }
      return CompletableFuture.completedFuture(proposal(expectedSequence, expectedRevision,
          expectedSequence, Step.PUBLISHED, scope, Optional.empty(), cursor, decision,
          cursor, decision, null));
    });
  }

  private StepProposal proposal(long expectedSequence, long expectedRevision, long sequenceThrough,
      Step step, RequestScope scope, Optional<SelectedSequenceReader.Selected> selected,
      ProtectedEventCursor expectedCursor, CommitDecision expectedDecision,
      ProtectedEventCursor cursor, CommitDecision decision, PreparedEvent event) {
    if (sequenceThrough < expectedSequence) {
      throw new IllegalArgumentException("event tailer proposal moves its sequence backwards");
    }
    return new StepProposal(this, expectedSequence, expectedRevision, sequenceThrough, step, scope,
        selected, expectedCursor, expectedDecision, cursor, decision, event);
  }

  private void requireCurrent(StepProposal proposal) {
    if (closed || !busy || inFlightScope != proposal.scope || sequence != proposal.expectedSequence
        || revision != proposal.expectedRevision || active != proposal.expectedCursor
        || activeDecision != proposal.expectedDecision) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private Step apply(StepProposal proposal) throws BackendFailure {
    return switch (proposal.step) {
      case WAITING -> Step.WAITING;
      case METADATA -> {
        if (proposal.selected.isPresent() || proposal.sequenceThrough == proposal.expectedSequence
            || proposal.expectedCursor != null || proposal.expectedDecision != null
            || proposal.cursor != null || proposal.decision != null || proposal.event != null) {
          throw new java.util.concurrent.CompletionException(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        sequence = proposal.sequenceThrough;
        yield Step.METADATA;
      }
      case OPENED -> {
        CommitDecision decision = proposal.decision;
        requireEventAfterMetadata(decision, proposal.expectedSequence,
            proposal.sequenceThrough, proposal.expectedRevision);
        sequence = proposal.sequenceThrough;
        active = proposal.selected.orElseThrow().events().orElseThrow();
        activeScope = proposal.scope;
        activeDecision = decision;
        yield Step.OPENED;
      }
      case BUFFERED -> {
        requireOpen();
        PreparedEvent event = proposal.event;
        if (buffered.size() >= maximumRevisionEvents
            || event.retainedBytes() > maximumRevisionBytes - bufferedBytes) {
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        BudgetManager.Lease lease = memory.tryRetain(BudgetManager.Memory.OUTPUT, event.retainedBytes())
            .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        try {
          buffered.add(new Buffered(event, lease));
        } catch (RuntimeException | Error failure) {
          lease.close();
          throw failure;
        }
        bufferedBytes += event.retainedBytes();
        yield Step.BUFFERED;
      }
      case PUBLISHED -> finishRevision(proposal);
    };
  }

  private static void requireEventAfterMetadata(CommitDecision decision, long completedSequence,
      long metadataThroughSequence, long revision) {
    if (decision == null || metadataThroughSequence < completedSequence
        || decision.sequence() != Math.addExact(metadataThroughSequence, 1)
        || decision.baseRevision() != revision || !decision.containsKeyChanges()) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private Step finishRevision(StepProposal proposal) {
    requireOpen();
    if (proposal.cursor != active || proposal.decision != activeDecision) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    try {
      List<Subscription> matches = new ArrayList<>();
      for (Buffered event : buffered) {
        matches.clear();
        subscriptionIndex.forEachMatch(event.event().current().key(), matches::add);
        for (Subscription subscription : matches) subscription.offer(event, activeDecision);
      }
    } catch (RuntimeException | Error failure) {
      close();
      throw failure;
    }
    sequence = activeDecision.sequence();
    revision = activeDecision.revision();
    lastPublishedEventCount = buffered.size();
    lastPublishedEventBytes = bufferedBytes;
    releaseBuffered();
    active = null;
    activeScope = null;
    activeDecision = null;
    return Step.PUBLISHED;
  }

  private void failPreparation(RequestScope scope) {
    synchronized (this) {
      if (inFlightScope == scope) {
        busy = false;
        inFlightScope = null;
      }
    }
    scope.cancel();
    close();
  }

  private void requireOpen() {
    if (closed) throw new IllegalStateException("shared tailer is closed");
  }

  private void releaseBuffered() {
    for (Buffered entry : buffered) entry.memory().close();
    buffered.clear();
    bufferedBytes = 0;
  }

  @Override public synchronized void close() {
    if (closed) return;
    closed = true;
    if (inFlightScope != null) inFlightScope.cancel();
    if (active != null) active.close();
    releaseBuffered();
    for (Subscription subscription : List.copyOf(subscriptions)) subscription.stop(Termination.TAILER_CLOSED);
  }

  private record Buffered(PreparedEvent event, BudgetManager.Lease memory) {}

  public final class Subscription implements AutoCloseable {
    private final KeyRange range;
    private final BudgetManager.Account account;
    private final int maximumEvents;
    private final long maximumBytes;
    private final BudgetManager.Lease indexMemory;
    private final ArrayDeque<Delivery> pending = new ArrayDeque<>();
    private long pendingBytes;
    private Termination termination;

    private Subscription(KeyRange range, BudgetManager.Account account, int maximumEvents, long maximumBytes,
        BudgetManager.Lease indexMemory) {
      this.range = range;
      this.account = account;
      this.maximumEvents = maximumEvents;
      this.maximumBytes = maximumBytes;
      this.indexMemory = indexMemory;
    }

    private void offer(Buffered item, CommitDecision decision) {
      PreparedEvent event = item.event();
      long eventBytes = event.retainedBytes();
      if (pending.size() >= maximumEvents || eventBytes > maximumBytes - pendingBytes) {
        stop(Termination.RESOURCE_LIMIT);
        return;
      }
      BudgetManager.Lease retained;
      try {
        retained = account.tryRetain(BudgetManager.Memory.OUTPUT, eventBytes)
            .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      } catch (BackendFailure failure) {
        stop(Termination.RESOURCE_LIMIT);
        return;
      }
      try {
        pending.addLast(new Delivery(decision.sequence(), decision.revision(), event, retained));
      } catch (RuntimeException | Error failure) {
        retained.close();
        throw failure;
      }
      pendingBytes += eventBytes;
    }

    /** The receiver owns the returned delivery and must close it after its last use. */
    public Optional<Delivery> poll() {
      synchronized (SharedEventTailer.this) {
        Delivery delivery = pending.pollFirst();
        if (delivery == null) return Optional.empty();
        pendingBytes -= delivery.event().retainedBytes();
        return Optional.of(delivery);
      }
    }

    public Optional<Termination> termination() {
      synchronized (SharedEventTailer.this) { return Optional.ofNullable(termination); }
    }

    @Override public void close() {
      synchronized (SharedEventTailer.this) { stop(Termination.CANCELLED); }
    }

    private void stop(Termination reason) {
      if (termination != null) return;
      termination = reason;
      subscriptions.remove(this);
      subscriptionIndex.remove(this, range);
      while (!pending.isEmpty()) pending.removeFirst().close();
      pendingBytes = 0;
      indexMemory.close();
    }
  }

  public static final class Delivery implements AutoCloseable {
    private final long sequence;
    private final long revision;
    private final PreparedEvent event;
    private final BudgetManager.Lease memory;

    private Delivery(long sequence, long revision, PreparedEvent event, BudgetManager.Lease memory) {
      this.sequence = sequence;
      this.revision = revision;
      this.event = event;
      this.memory = memory;
    }

    public long sequence() { return sequence; }
    public long revision() { return revision; }
    public PreparedEvent event() { return event; }
    @Override public void close() { memory.close(); }
  }
}
