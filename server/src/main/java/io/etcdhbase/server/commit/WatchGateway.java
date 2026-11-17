package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/** Adapts a table-group's shared tailer and per-watch budgets to the Watch RPC stream controller. */
final class WatchGateway {
  private static final long ACTIVE_WATCH_STATE_BYTES = 512;
  private record EventPosition(long sequence, long ordinal) implements Comparable<EventPosition> {
    private EventPosition {
      if (sequence < 0 || ordinal < 0) throw new IllegalArgumentException("invalid Watch event position");
    }

    @Override public int compareTo(EventPosition other) {
      int sequenceOrder = Long.compare(sequence, other.sequence);
      return sequenceOrder != 0 ? sequenceOrder : Long.compare(ordinal, other.ordinal);
    }
  }

  private final String tableGroup;
  private final M3TableGroupRuntime.Pending pending;
  private final CommitControlStore control;
  private final EventHistoryPinManager historyPins;
  private final SelectedSequenceReader selectedEvents;
  private final io.etcdhbase.core.bytes.ByteKey authorization;
  private final BudgetManager budgets;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final long clusterId;
  private final long memberId;
  private final int maximumResponseBytes;

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets,
      Executor worker, ScheduledExecutorService timers, long clusterId, long memberId,
      int maximumResponseBytes) {
    this(tableGroup, pending, control, budgets, worker, timers, clusterId, memberId,
        maximumResponseBytes, null, null, null);
  }

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets, Executor worker, ScheduledExecutorService timers,
      long clusterId, long memberId, int maximumResponseBytes,
      EventHistoryPinManager historyPins, SelectedSequenceReader selectedEvents,
      io.etcdhbase.core.bytes.ByteKey authorization) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.pending = Objects.requireNonNull(pending, "pending");
    this.control = Objects.requireNonNull(control, "control");
    this.historyPins = historyPins;
    this.selectedEvents = selectedEvents;
    this.authorization = authorization;
    if ((historyPins == null || selectedEvents == null || authorization == null)
        && !(historyPins == null && selectedEvents == null && authorization == null)) {
      throw new IllegalArgumentException("Watch replay dependencies must be provided together");
    }
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    if (tableGroup.isBlank() || clusterId == 0 || memberId == 0 || maximumResponseBytes < 1) {
      throw new IllegalArgumentException("invalid Watch gateway identity or limits");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumResponseBytes = maximumResponseBytes;
  }

  Function<StreamObserver<Rpc.WatchResponse>, StreamObserver<Rpc.WatchRequest>> handler() {
    return responseObserver -> {
      RequestScope scope = RequestScopeInterceptor.currentScope();
      return new WatchStream(responseObserver, scope, worker, timers, () -> openSession(scope),
          clusterId, memberId, maximumResponseBytes);
    };
  }

  private CompletionStage<WatchStream.Session> openSession(RequestScope scope) {
    SharedEventTailer current = pending.eventTailer().orElse(null);
    CompletionStage<SharedEventTailer> ready = current == null
        ? pending.eventTailerReady() : CompletableFuture.completedFuture(current);
    return ready.thenApply(tailer -> {
      if (scope.isCancelled() || tailer.isClosed()) {
        throw new java.util.concurrent.CompletionException(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return new TailerSession(tailer, scope);
    });
  }

  private final class TailerSession implements WatchStream.Session {
    private final SharedEventTailer tailer;
    private final RequestScope streamScope;

    private TailerSession(SharedEventTailer tailer, RequestScope streamScope) {
      this.tailer = tailer;
      this.streamScope = streamScope;
    }

    @Override public long completedRevision() { return tailer.completedRevision(); }

    @Override
    public CompletionStage<Long> currentRevision() {
      if (tailer.isClosed() || streamScope.isCancelled()) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      CompletionStage<Optional<ControlSnapshot>> latest;
      try {
        latest = Objects.requireNonNull(control.read(streamScope), "Watch revision snapshot future");
      } catch (RuntimeException | Error failure) {
        return CompletableFuture.failedFuture(failure);
      }
      return latest.thenApply(snapshot -> {
        if (streamScope.isCancelled() || tailer.isClosed()) throw unavailable();
        return snapshot.map(ControlSnapshot::state)
            .map(state -> state.publishedRevision())
            .orElseThrow(WatchGateway::unavailable);
      });
    }

    @Override
    public CompletionStage<WatchStream.OpenedWatch> subscribe(KeyRange range,
        Rpc.WatchCreateRequest request)
        throws BackendFailure {
      if (tailer.isClosed() || streamScope.isCancelled()) {
        throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      }
      long retainedStateBytes = retainedStateBytes(range);
      var reservation = new BudgetManager.Reservation(retainedStateBytes, 1, 1,
          maximumResponseBytes, 0);
      BudgetManager.Account account = budgets.admit(tableGroup, streamScope, reservation);
      BudgetManager.Lease stateMemory = null;
      SharedEventTailer.SubscriptionStart start;
      try {
        stateMemory = account.tryRetain(BudgetManager.Memory.HEAD, retainedStateBytes)
            .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        start = tailer.subscribeWithCheckpoint(range, account,
            WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH, maximumResponseBytes,
            request.getStartRevision());
      } catch (BackendFailure | RuntimeException | Error failure) {
        closeAfterFailure(stateMemory, failure);
        closeAfterFailure(account, failure);
        throw failure;
      }

      SharedEventTailer.Subscription subscription = start.subscription();
      BudgetManager.Lease retainedStateMemory = stateMemory;
      CompletionStage<Optional<ControlSnapshot>> barrier;
      try {
        barrier = Objects.requireNonNull(control.read(streamScope), "Watch control snapshot future");
      } catch (RuntimeException | Error failure) {
        closeAfterFailure(subscription, failure);
        closeAfterFailure(retainedStateMemory, failure);
        closeAfterFailure(account, failure);
        throw failure;
      }
      return barrier
          .thenCompose(snapshot -> openedWatch(range, snapshot, start, subscription, account,
              retainedStateMemory, request))
          .whenComplete((opened, failure) -> {
            if (failure == null) return;
            closeAfterFailure(subscription, failure);
            closeAfterFailure(retainedStateMemory, failure);
            closeAfterFailure(account, failure);
          });
    }

    private CompletionStage<WatchStream.OpenedWatch> openedWatch(KeyRange range,
        Optional<ControlSnapshot> snapshot,
        SharedEventTailer.SubscriptionStart start, SharedEventTailer.Subscription subscription,
        BudgetManager.Account account, BudgetManager.Lease stateMemory,
        Rpc.WatchCreateRequest request) {
      if (streamScope.isCancelled() || tailer.isClosed()) {
        throw unavailable();
      }
      var state = snapshot.map(ControlSnapshot::state).orElseThrow(WatchGateway::unavailable);
      long revision = state.publishedRevision();
      if (revision < start.revision()) {
        throw unavailable();
      }
      if (request.getStartRevision() <= 0) {
        subscription.requireRevisionAtLeast(nextRevision(revision));
        return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
            state.publishedSequence(),
            state.compactRevision() == 0 ? -1 : state.compactRevision(),
            new TailerSubscription(subscription, account, stateMemory, null,
                state.publishedSequence())));
      }
      if (request.getStartRevision() <= revision) {
        if (historyPins == null) {
          return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
              state.publishedSequence(),
              state.compactRevision() == 0 ? -1 : state.compactRevision(),
              new TailerSubscription(subscription, account, stateMemory, null,
                  state.publishedSequence())));
        }
        subscription.requireRevisionAtLeast(nextRevision(revision));
        var barrierSnapshot = snapshot.orElseThrow(WatchGateway::unavailable);
        return WatchHistoryReplay.open(tableGroup, range, request.getStartRevision(),
            barrierSnapshot, state.publishedSequence(), authorization, historyPins, selectedEvents,
            budgets, account).thenApply(replay -> new WatchStream.OpenedWatch(revision,
                state.publishedSequence(),
                state.compactRevision() == 0 ? -1 : state.compactRevision(),
                new TailerSubscription(subscription, account, stateMemory, replay,
                    state.publishedSequence()), true));
      }
      return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
          state.publishedSequence(),
          state.compactRevision() == 0 ? -1 : state.compactRevision(),
          new TailerSubscription(subscription, account, stateMemory, null,
              state.publishedSequence())));
    }

  }

  private static java.util.concurrent.CompletionException unavailable() {
    return new java.util.concurrent.CompletionException(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }

  private static long nextRevision(long revision) {
    return revision == Long.MAX_VALUE ? Long.MAX_VALUE : revision + 1;
  }

  static String cancelReason(SharedEventTailer.Termination reason) {
    return switch (reason) {
      case RESOURCE_LIMIT -> "etcd-hbase: Watch subscription resource limit exceeded";
      case TAILER_CLOSED -> "etcd-hbase: Watch event source is unavailable";
      case CANCELLED -> "etcd-hbase: Watch subscription was cancelled";
    };
  }

  private static long retainedStateBytes(KeyRange range) {
    Objects.requireNonNull(range, "range");
    long lowerBoundBytes = range.lowerInclusive().map(bound -> (long) bound.length()).orElse(0L);
    long upperBoundBytes = range.upperExclusive().map(bound -> (long) bound.length()).orElse(0L);
    return Math.addExact(ACTIVE_WATCH_STATE_BYTES, Math.addExact(lowerBoundBytes, upperBoundBytes));
  }

  private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
    if (resource == null) return;
    try {
      resource.close();
    } catch (Exception | Error cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    }
  }

  private static final class TailerSubscription implements WatchStream.Subscription {
    private final SharedEventTailer.Subscription subscription;
    private final BudgetManager.Account account;
    private final BudgetManager.Lease stateMemory;
    private final WatchHistoryReplay history;
    private final long barrierSequence;
    private boolean closed;

    private TailerSubscription(SharedEventTailer.Subscription subscription,
        BudgetManager.Account account, BudgetManager.Lease stateMemory,
        WatchHistoryReplay history, long barrierSequence) {
      this.subscription = subscription;
      this.account = account;
      this.stateMemory = stateMemory;
      this.history = history;
      this.barrierSequence = barrierSequence;
    }

    @Override
    public synchronized Optional<WatchStream.Delivery> poll() {
      if (closed) return Optional.empty();
      if (history != null) {
        Optional<WatchStream.Delivery> replayed = history.poll();
        if (replayed.isPresent()) return replayed;
        if (!history.complete() || history.terminationReason().isPresent()) return Optional.empty();
      }
      Optional<SharedEventTailer.Delivery> polled;
      do {
        polled = subscription.poll();
        if (polled.isEmpty()) return Optional.empty();
        SharedEventTailer.Delivery candidate = polled.orElseThrow();
        // The snapshot barrier is the exclusive cursor (sequence, Long.MAX_VALUE). Revision
        // filtering is only a queue optimization; this identity check owns the handoff.
        var position = new EventPosition(candidate.sequence(), candidate.event().ordinal());
        if (position.compareTo(new EventPosition(barrierSequence, Long.MAX_VALUE)) <= 0) {
          candidate.close();
          polled = Optional.empty();
        }
      } while (polled.isEmpty() && !closed);
      if (closed) {
        polled.ifPresent(SharedEventTailer.Delivery::close);
        return Optional.empty();
      }
      return polled.map(delivery -> new WatchStream.Delivery(
          delivery.sequence(), delivery.revision(), delivery.event(), delivery::close));
    }

    @Override
    public synchronized Optional<String> terminationReason() {
      if (closed) return Optional.empty();
      if (history != null) {
        Optional<String> historyTermination = history.terminationReason();
        if (historyTermination.isPresent()) return historyTermination;
        if (!history.complete()) return Optional.empty();
      }
      return subscription.termination().map(WatchGateway::cancelReason);
    }

    @Override
    public synchronized boolean progressReady(long revision) {
      return !closed && (history == null || history.complete());
    }

    @Override
    public synchronized void setWakeup(Runnable wakeup) {
      if (closed) return;
      if (history != null) history.setWakeup(wakeup);
      subscription.setWakeup(wakeup);
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      Throwable failure = WatchCleanup.closeAndCollect(() -> subscription.setWakeup(null), null);
      if (history != null) {
        failure = WatchCleanup.closeAndCollect(() -> history.setWakeup(null), failure);
        failure = WatchCleanup.closeAndCollect(history::close, failure);
      }
      failure = WatchCleanup.closeAndCollect(subscription::close, failure);
      failure = WatchCleanup.closeAndCollect(stateMemory::close, failure);
      failure = WatchCleanup.closeAndCollect(account::close, failure);
      WatchCleanup.throwIfPresent(failure);
    }
  }
}
