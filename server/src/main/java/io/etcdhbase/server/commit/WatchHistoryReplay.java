package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ProtectedEventCursor;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Bounded, inclusive replay of selected Watch revisions behind one durable history pin. */
final class WatchHistoryReplay implements AutoCloseable {
  record OpenResult(long revision, long sequence, long compactRevision,
      Optional<WatchHistoryReplay> replay) {
    OpenResult {
      if (revision < 0 || sequence < 0 || compactRevision < 0) {
        throw new IllegalArgumentException("invalid Watch replay open result");
      }
      Objects.requireNonNull(replay, "replay");
    }
  }

  private static final int MAX_PAGES_PER_REVISION = 512;
  private static final long MAX_BYTES_PER_REVISION = 64L << 20;
  private static final int MAX_TOTAL_REVISIONS = 4096;
  private static final int MAX_TOTAL_PAGES = 16_384;
  private static final long MAX_TOTAL_PROOF_BYTES = 512L << 20;
  private static final int MAX_EVENTS_PER_REVISION = 4096;
  private static final long MAX_REVISION_EVENT_BYTES = 16L << 20;
  private static final String RESOURCE_LIMIT_REASON =
      "etcd-hbase: Watch history replay resource limit exceeded";
  private static final String UNAVAILABLE_REASON =
      "etcd-hbase: Watch history replay is unavailable";

  private final String tableGroup;
  private final KeyRange range;
  private final long barrierRevision;
  private final long barrierSequence;
  private final long authorizationRevision;
  private final ByteKey authorization;
  private final EventHistoryPinManager pins;
  private final SelectedSequenceReader selected;
  private final BudgetManager budgets;
  private final BudgetManager.Account watchAccount;
  private final RequestScope pinScope = RequestScope.withoutDeadline();
  private final ArrayDeque<WatchStream.Delivery> pending = new ArrayDeque<>();

  private EventHistoryPinManager.Pin pin;
  private long nextRevision;
  private int readRevisions;
  private int readPages;
  private long readBytes;
  private boolean loading;
  private boolean complete;
  private boolean closed;
  private String termination;
  private Runnable wakeup = () -> {};
  private CompletableFuture<?> inFlight;
  private RequestScope activeReadScope;

  private WatchHistoryReplay(String tableGroup, KeyRange range, long nextRevision,
      long barrierRevision, long barrierSequence, long authorizationRevision,
      ByteKey authorization, EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount) {
    this.tableGroup = tableGroup;
    this.range = range;
    this.barrierRevision = barrierRevision;
    this.barrierSequence = barrierSequence;
    this.authorizationRevision = authorizationRevision;
    this.authorization = authorization;
    this.pins = pins;
    this.selected = selected;
    this.budgets = budgets;
    this.watchAccount = watchAccount;
    this.nextRevision = nextRevision;
  }

  static CompletableFuture<OpenResult> open(String tableGroup, KeyRange range,
      long startRevision, ControlSnapshot barrier, long barrierSequence, ByteKey authorization,
      EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount) {
    Objects.requireNonNull(barrier, "barrier");
    Objects.requireNonNull(authorization, "authorization");
    if (startRevision <= 0 || barrier.state().publishedRevision() < startRevision
        || barrierSequence != barrier.state().publishedSequence()
        || authorization.length() != 32) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid Watch replay barrier"));
    }
    // Revision 1 is the initialized empty store; the first KV-changing revision is 2.
    long firstEventRevision = Math.max(2, startRevision);
    if (firstEventRevision > barrier.state().publishedRevision()) {
      var replay = new WatchHistoryReplay(tableGroup, range, barrier.state().publishedRevision() + 1,
          barrier.state().publishedRevision(), barrierSequence, startRevision, authorization,
          pins, selected, budgets, watchAccount);
      replay.complete = true;
      return CompletableFuture.completedFuture(new OpenResult(
          barrier.state().publishedRevision(), barrierSequence, barrier.state().compactRevision(),
          Optional.of(replay)));
    }

    RequestScope lookupScope = RequestScope.withoutDeadline();
    BudgetManager.Account lookupAccount;
    try {
      lookupAccount = budgets.admitReserved(tableGroup, lookupScope,
          LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY, LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);
    } catch (BackendFailure failure) {
      lookupScope.close();
      return CompletableFuture.failedFuture(failure);
    }
    var lookupBudget = new ManifestCatalog.ProofReadBudget(
        lookupScope, lookupAccount, MAX_PAGES_PER_REVISION, MAX_BYTES_PER_REVISION);
    var replay = new WatchHistoryReplay(tableGroup, range, firstEventRevision,
        barrier.state().publishedRevision(), barrierSequence, startRevision, authorization,
        pins, selected, budgets, watchAccount);

    CompletableFuture<Long> located = replay.readSequence(firstEventRevision, barrier.state(),
        lookupScope, lookupBudget);
    located.whenComplete((ignored, failure) -> {
      try { lookupAccount.close(); } finally { lookupScope.close(); }
    });
    CompletableFuture<OpenResult> opened = located.thenCompose(sequence -> {
      var requirement = new EventHistoryPinManager.Requirement(firstEventRevision, sequence,
          authorization, Optional.empty(), Optional.empty());
      return pins.register(requirement, replay.pinScope).thenCompose(registered -> {
        replay.pin = registered;
        return registered.renew(replay.pinScope)
            .thenCompose(ignored -> replay.withReadScope(
                (scope, budget) -> replay.readControl(scope, budget)))
            .thenCompose(current -> {
              if (current.publishedRevision() < barrier.state().publishedRevision()) {
                return CompletableFuture.failedFuture(
                    new BackendFailure(BackendFailure.Category.CANCELLED));
              }
              if (current.compactRevision() > startRevision) {
                replay.completeCompactedOpen();
                return CompletableFuture.completedFuture(new OpenResult(
                    current.publishedRevision(), current.publishedSequence(),
                    current.compactRevision(), Optional.empty()));
              }
              return replay.withReadScope((scope, budget) -> replay.readSequence(
                  firstEventRevision, current, scope, budget)).thenApply(verified -> {
                    if (verified.longValue() != sequence || verified > barrierSequence) {
                      throw new CompletionException(new IllegalStateException(
                          "Watch replay sequence changed while registering its history pin"));
                    }
                    return new OpenResult(barrier.state().publishedRevision(), barrierSequence,
                        barrier.state().compactRevision(), Optional.of(replay));
                  });
            });
      });
    });
    opened.whenComplete((value, failure) -> { if (failure != null) replay.releasePin(); });
    return opened;
  }

  synchronized void setWakeup(Runnable wakeup) {
    this.wakeup = wakeup == null ? () -> {} : wakeup;
  }

  synchronized Optional<WatchStream.Delivery> poll() {
    if (closed) return Optional.empty();
    WatchStream.Delivery delivery = pending.pollFirst();
    if (delivery != null) return Optional.of(delivery);
    if (!complete && termination == null && !loading) startNextRevision();
    return Optional.empty();
  }

  synchronized Optional<String> terminationReason() {
    return Optional.ofNullable(termination);
  }

  synchronized boolean complete() { return complete; }

  private synchronized void startNextRevision() {
    if (closed || complete || termination != null || loading) return;
    if (nextRevision > barrierRevision) {
      complete = true;
      releasePin();
      return;
    }
    if (readRevisions >= MAX_TOTAL_REVISIONS || readPages >= MAX_TOTAL_PAGES
        || readBytes >= MAX_TOTAL_PROOF_BYTES) {
      terminate(RESOURCE_LIMIT_REASON);
      return;
    }
    loading = true;
    long revision = nextRevision;
    CompletableFuture<List<WatchStream.Delivery>> read = withReadScope((scope, budget) ->
        pin.renew(pinScope).thenCompose(ignored -> readControl(scope, budget))
            .thenCompose(state -> {
              if (state.compactRevision() > authorizationRevision
                  || state.publishedRevision() < barrierRevision) {
                throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
              }
              return readSequence(revision, state, scope, budget).thenCompose(sequence ->
                  sequence <= barrierSequence
                      ? readRevision(revision, sequence, scope, budget)
                      : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
            }));
    inFlight = read;
    read.whenComplete((events, failure) -> finishRevision(revision, events, failure, read));
  }

  private void finishRevision(long revision, List<WatchStream.Delivery> events,
      Throwable failure, CompletableFuture<?> read) {
    Runnable signal;
    synchronized (this) {
      if (inFlight == read) inFlight = null;
      loading = false;
      if (closed) {
        closeDeliveries(events);
        return;
      }
      if (failure != null) {
        terminate(reason(failure));
        signal = wakeup;
      } else {
        pending.addAll(Objects.requireNonNull(events, "replayed events"));
        nextRevision = Math.addExact(revision, 1);
        readRevisions++;
        signal = wakeup;
      }
    }
    signal.run();
  }

  private CompletableFuture<Long> readSequence(long revision, CommitState state,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return selected.sequenceForRevision(revision, state, scope, budget);
  }

  private CompletableFuture<CommitState> readControl(RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    return selected.readPublishedState(scope, budget);
  }

  private CompletableFuture<List<WatchStream.Delivery>> readRevision(long revision, long sequence,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    if (revision <= 1) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "event revisions must follow the initialized revision"));
    }
    return selected.open(sequence, revision - 1, authorization, scope, budget).thenCompose(found -> {
      var chosen = found.orElseThrow(() -> new CompletionException(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      if (chosen.decision().sequence() != sequence || chosen.decision().revision() != revision
          || chosen.decision().baseRevision() != revision - 1 || chosen.events().isEmpty()
          || chosen.index().isEmpty()) {
        chosen.events().ifPresent(ProtectedEventCursor::close);
        return CompletableFuture.failedFuture(new CompletionException(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      }
      EventReference reference = chosen.index().orElseThrow().events();
      if (reference.itemCount() > MAX_EVENTS_PER_REVISION
          || reference.totalBytes() > MAX_REVISION_EVENT_BYTES) {
        chosen.events().orElseThrow().close();
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      var deliveries = new ArrayList<WatchStream.Delivery>();
      long[] retainedBytes = {0};
      long[] observed = {0};
      CompletableFuture<Void> reads = CompletableFuture.completedFuture(null);
      for (long ordinal = 0; ordinal < reference.itemCount(); ordinal++) {
        long expectedOrdinal = ordinal;
        reads = reads.thenCompose(ignored -> chosen.events().orElseThrow().next(budget)
            .thenAccept(event -> {
              PreparedEvent prepared = event.orElseThrow(() -> new CompletionException(
                  new IllegalStateException("sealed Watch revision ended before its event count")));
              if (prepared.ordinal() != expectedOrdinal
                  || prepared.current().metadata().modificationRevision() != revision) {
                throw new CompletionException(new IllegalStateException(
                    "sealed Watch revision is not in contiguous ordinal order"));
              }
              observed[0]++;
              if (!range.contains(prepared.current().key())) return;
              long bytes = prepared.retainedBytes();
              if (deliveries.size() >= MAX_EVENTS_PER_REVISION
                  || bytes > MAX_REVISION_EVENT_BYTES - retainedBytes[0]) {
                throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
              }
              BudgetManager.Lease lease;
              try {
                lease = watchAccount.tryRetain(BudgetManager.Memory.OUTPUT, bytes)
                    .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
              } catch (BackendFailure failure) {
                throw new CompletionException(failure);
              }
              retainedBytes[0] += bytes;
              try {
                deliveries.add(new WatchStream.Delivery(sequence, revision, prepared, lease::close));
              } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
              }
            }));
      }
      reads = reads.thenCompose(ignored -> chosen.events().orElseThrow().next(budget)
          .thenAccept(extra -> {
            if (extra.isPresent() || observed[0] != reference.itemCount()) {
              throw new CompletionException(new IllegalStateException(
                  "sealed Watch revision event count differs from its authenticated root"));
            }
          }));
      var result = new CompletableFuture<List<WatchStream.Delivery>>();
      reads.whenComplete((ignored, failure) -> {
        ProtectedEventCursor cursor = chosen.events().orElseThrow();
        cursor.close();
        cursor.releaseCompletion().whenComplete((released, releaseFailure) -> {
          Throwable problem = failure == null ? null : unwrap(failure);
          if (releaseFailure != null) {
            Throwable release = unwrap(releaseFailure);
            if (problem == null) problem = release;
            else if (problem != release) problem.addSuppressed(release);
          }
          if (problem != null) {
            closeDeliveries(deliveries);
            result.completeExceptionally(problem);
          } else {
            result.complete(List.copyOf(deliveries));
          }
        });
      });
      return result;
    });
  }

  private <T> CompletableFuture<T> withReadScope(
      java.util.function.BiFunction<RequestScope, ManifestCatalog.ProofReadBudget,
          CompletableFuture<T>> operation) {
    RequestScope scope = RequestScope.withoutDeadline();
    BudgetManager.Account account;
    try {
      account = budgets.admitReserved(tableGroup, scope,
          LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY, LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);
    } catch (BackendFailure failure) {
      scope.close();
      return CompletableFuture.failedFuture(failure);
    }
    var budget = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_PAGES_PER_REVISION, MAX_BYTES_PER_REVISION);
    CompletableFuture<T> work;
    synchronized (this) {
      if (closed) {
        account.close();
        scope.close();
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      activeReadScope = scope;
    }
    try {
      work = Objects.requireNonNull(operation.apply(scope, budget), "Watch replay read future");
    } catch (RuntimeException | Error failure) {
      account.close();
      scope.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    var result = new CompletableFuture<T>();
    work.whenComplete((value, failure) -> {
      readPages = Math.addExact(readPages, budget.pages());
      readBytes = Math.addExact(readBytes, budget.bytes());
      Throwable cause = failure == null ? null : unwrap(failure);
      if (failure != null) scope.cancel(); else scope.close();
      scope.physicalProducersCompletion().whenComplete((ignored, drainFailure) -> {
        Throwable settledCleanupFailure = null;
        try { account.close(); } catch (RuntimeException | Error problem) { settledCleanupFailure = problem; }
        if (drainFailure != null) {
          Throwable drain = unwrap(drainFailure);
          if (settledCleanupFailure == null) settledCleanupFailure = drain;
          else if (settledCleanupFailure != drain) settledCleanupFailure.addSuppressed(drain);
        }
        synchronized (WatchHistoryReplay.this) {
          if (activeReadScope == scope) activeReadScope = null;
        }
        Throwable settledFailure = cause;
        if (settledCleanupFailure != null) {
          if (settledFailure == null) settledFailure = settledCleanupFailure;
          else if (settledFailure != settledCleanupFailure) settledFailure.addSuppressed(settledCleanupFailure);
        }
        if (settledFailure == null) result.complete(value);
        else result.completeExceptionally(settledFailure);
      });
    });
    result.whenComplete((ignored, failure) -> { if (result.isCancelled()) scope.cancel(); });
    return result;
  }

  private void terminate(String reason) {
    if (termination != null || closed) return;
    termination = reason;
    closeDeliveries(pending);
    pending.clear();
    releasePin();
  }

  private void releasePin() {
    EventHistoryPinManager.Pin current = pin;
    pin = null;
    if (current != null) current.close();
    pinScope.close();
  }

  private void completeCompactedOpen() {
    synchronized (this) { complete = true; }
    releasePin();
  }

  private static String reason(Throwable failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof BackendFailure backendFailure
        && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
      return RESOURCE_LIMIT_REASON;
    }
    return UNAVAILABLE_REASON;
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException
        || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static void closeDeliveries(Iterable<WatchStream.Delivery> deliveries) {
    for (WatchStream.Delivery delivery : deliveries) {
      try { delivery.close(); } catch (RuntimeException ignored) { /* release remaining leases */ }
    }
  }

  @Override
  public void close() {
    CompletableFuture<?> active;
    synchronized (this) {
      if (closed) return;
      closed = true;
      closeDeliveries(pending);
      pending.clear();
      active = inFlight;
      if (active == null) releasePin();
      if (activeReadScope != null) activeReadScope.cancel();
    }
    if (active != null) {
      pinScope.cancel();
      active.whenComplete((ignored, failure) -> releasePin());
    }
  }
}
