package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** One bounded, shared live-event reader for a single table-group authorization domain. */
public final class SharedEventTailer implements AutoCloseable {
  public enum Step { WAITING, METADATA, OPENED, BUFFERED, PUBLISHED }
  public enum Termination { CANCELLED, RESOURCE_LIMIT, TAILER_CLOSED }

  /** Last complete cursor position immediately before the subscription was installed. */
  public record SubscriptionStart(Subscription subscription, long sequence, long revision) {
    public SubscriptionStart { Objects.requireNonNull(subscription, "subscription"); }
  }

  private final SelectedSequenceReader source;
  private final ByteKey authorizationDigest;
  private final BudgetManager.Account memory;
  private final int maximumSubscriptions;
  private final int maximumRevisionEvents;
  private final long maximumRevisionBytes;
  private final Set<Subscription> subscriptions = new HashSet<>();
  private final List<Buffered> buffered = new ArrayList<>();
  private long sequence;
  private long revision;
  private long bufferedBytes;
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
    if (authorizationDigest.length() != 32 || completedSequence < 0 || completedSequence == Long.MAX_VALUE
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

  /** The caller authorizes the range and arranges any history replay before live delivery. */
  public synchronized Subscription subscribe(KeyRange range, BudgetManager.Account account,
      int maximumEvents, long maximumBytes) {
    return subscribeWithCheckpoint(range, account, maximumEvents, maximumBytes).subscription();
  }

  /** Installs the subscription and captures the live cursor under the same lock as publication. */
  public synchronized SubscriptionStart subscribeWithCheckpoint(KeyRange range, BudgetManager.Account account,
      int maximumEvents, long maximumBytes) {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(account, "account");
    if (closed || subscriptions.size() >= maximumSubscriptions || maximumEvents < 1 || maximumBytes < 1) {
      throw new IllegalStateException("shared tailer cannot admit another subscription");
    }
    var subscription = new Subscription(range, account, maximumEvents, maximumBytes);
    subscriptions.add(subscription);
    return new SubscriptionStart(subscription, sequence, revision);
  }

  /** Performs at most one sequence open or one event read; callers schedule the next bounded step. */
  public synchronized CompletableFuture<Step> step(RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (closed || busy || !budget.belongsTo(scope) || active != null && activeScope != scope) {
      return CompletableFuture.failedFuture(new IllegalStateException("shared tailer is closed, busy, or has another scope"));
    }
    busy = true;
    inFlightScope = scope;
    CompletableFuture<Step> work;
    try {
      work = active == null ? open(scope, budget) : readEvent(budget);
    } catch (RuntimeException | Error failure) {
      busy = false;
      inFlightScope = null;
      scope.cancel();
      close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    var response = new CompletableFuture<Step>();
    response.whenComplete((ignored, failure) -> {
      if (response.isCancelled()) {
        scope.cancel();
        close();
      }
    });
    work.whenComplete((result, failure) -> {
      synchronized (SharedEventTailer.this) {
        busy = false;
        inFlightScope = null;
      }
      if (failure != null) {
        close();
        response.completeExceptionally(failure);
      } else if (!response.complete(result)) {
        close();
      }
    });
    return response;
  }

  private CompletableFuture<Step> open(RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    long expectedSequence = Math.addExact(sequence, 1);
    return source.open(expectedSequence, revision, authorizationDigest, scope, budget).thenApply(found -> {
      synchronized (this) {
        if (closed) {
          found.flatMap(SelectedSequenceReader.Selected::events).ifPresent(ProtectedEventCursor::close);
          throw new IllegalStateException("shared tailer is closed");
        }
        if (found.isEmpty()) return Step.WAITING;
        var selected = found.orElseThrow();
        if (selected.events().isEmpty()) {
          sequence = selected.decision().sequence();
          return Step.METADATA;
        }
        active = selected.events().orElseThrow();
        activeScope = scope;
        activeDecision = selected.decision();
        return Step.OPENED;
      }
    });
  }

  private CompletableFuture<Step> readEvent(ManifestCatalog.ProofReadBudget budget) {
    ProtectedEventCursor cursor = active;
    return cursor.next(budget).thenCompose(found -> {
      if (found.isPresent()) {
        synchronized (this) {
          requireOpen();
          PreparedEvent event = found.orElseThrow();
          if (buffered.size() >= maximumRevisionEvents
              || event.retainedBytes() > maximumRevisionBytes - bufferedBytes) {
            return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
          }
          BudgetManager.Lease lease;
          try {
            lease = memory.tryRetain(BudgetManager.Memory.OUTPUT, event.retainedBytes())
                .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
          } catch (BackendFailure failure) {
            return CompletableFuture.failedFuture(failure);
          }
          buffered.add(new Buffered(event, lease));
          bufferedBytes += event.retainedBytes();
          return CompletableFuture.completedFuture(Step.BUFFERED);
        }
      }
      cursor.close();
      return cursor.releaseCompletion().thenApply(ignored -> finishRevision()).toCompletableFuture();
    });
  }

  private synchronized Step finishRevision() {
    requireOpen();
    try {
      for (Subscription subscription : List.copyOf(subscriptions)) subscription.offer(buffered, activeDecision);
    } catch (RuntimeException | Error failure) {
      close();
      throw failure;
    }
    sequence = activeDecision.sequence();
    revision = activeDecision.revision();
    releaseBuffered();
    active = null;
    activeScope = null;
    activeDecision = null;
    return Step.PUBLISHED;
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
    private final ArrayDeque<Delivery> pending = new ArrayDeque<>();
    private long pendingBytes;
    private Termination termination;

    private Subscription(KeyRange range, BudgetManager.Account account, int maximumEvents, long maximumBytes) {
      this.range = range;
      this.account = account;
      this.maximumEvents = maximumEvents;
      this.maximumBytes = maximumBytes;
    }

    private void offer(List<Buffered> events, CommitDecision decision) {
      List<Delivery> additions = new ArrayList<>();
      long bytes = 0;
      try {
        for (Buffered item : events) {
          PreparedEvent event = item.event();
          if (!range.contains(event.current().key())) continue;
          bytes = Math.addExact(bytes, event.retainedBytes());
          if (additions.size() >= maximumEvents - pending.size() || bytes > maximumBytes - pendingBytes) {
            throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
          }
          var retained = account.tryRetain(BudgetManager.Memory.OUTPUT, event.retainedBytes())
              .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
          additions.add(new Delivery(decision.sequence(), decision.revision(), event, retained));
        }
      } catch (BackendFailure failure) {
        additions.forEach(Delivery::close);
        stop(Termination.RESOURCE_LIMIT);
        return;
      } catch (RuntimeException | Error failure) {
        additions.forEach(Delivery::close);
        throw failure;
      }
      pending.addAll(additions);
      pendingBytes += bytes;
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
      while (!pending.isEmpty()) pending.removeFirst().close();
      pendingBytes = 0;
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
