package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.ControlSnapshot;
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

  private final String tableGroup;
  private final M3TableGroupRuntime.Pending pending;
  private final CommitControlStore control;
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
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.pending = Objects.requireNonNull(pending, "pending");
    this.control = Objects.requireNonNull(control, "control");
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
            WatchStream.MAX_PENDING_EVENTS_PER_WATCH, maximumResponseBytes,
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
          .thenApply(snapshot -> openedWatch(snapshot, start, subscription, account,
              retainedStateMemory, request))
          .whenComplete((opened, failure) -> {
            if (failure == null) return;
            closeAfterFailure(subscription, failure);
            closeAfterFailure(retainedStateMemory, failure);
            closeAfterFailure(account, failure);
          });
    }

    private WatchStream.OpenedWatch openedWatch(Optional<ControlSnapshot> snapshot,
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
      }
      return new WatchStream.OpenedWatch(revision,
          state.compactRevision() == 0 ? -1 : state.compactRevision(),
          new TailerSubscription(subscription, account, stateMemory));
    }
  }

  private static java.util.concurrent.CompletionException unavailable() {
    return new java.util.concurrent.CompletionException(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }

  private static long nextRevision(long revision) {
    return revision == Long.MAX_VALUE ? Long.MAX_VALUE : revision + 1;
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
    private boolean liveDeliveryClosed;
    private boolean closed;

    private TailerSubscription(SharedEventTailer.Subscription subscription,
        BudgetManager.Account account, BudgetManager.Lease stateMemory) {
      this.subscription = subscription;
      this.account = account;
      this.stateMemory = stateMemory;
    }

    @Override
    public synchronized Optional<WatchStream.Delivery> poll() {
      if (closed || liveDeliveryClosed) return Optional.empty();
      Optional<SharedEventTailer.Delivery> polled = subscription.poll();
      if (closed || liveDeliveryClosed) {
        polled.ifPresent(SharedEventTailer.Delivery::close);
        return Optional.empty();
      }
      return polled.map(delivery -> new WatchStream.Delivery(
          delivery.revision(), delivery.event(), delivery::close));
    }

    @Override
    public synchronized Optional<String> terminationReason() {
      if (liveDeliveryClosed) return Optional.empty();
      return subscription.termination().map(reason -> switch (reason) {
        case RESOURCE_LIMIT -> "etcd-hbase: Watch subscription resource limit exceeded";
        case TAILER_CLOSED -> "etcd-hbase: Watch event source is unavailable";
        case CANCELLED -> "etcd-hbase: Watch subscription was cancelled";
      });
    }

    @Override
    public synchronized void closeLiveDelivery() {
      if (closed || liveDeliveryClosed) return;
      liveDeliveryClosed = true;
      subscription.close();
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      RuntimeException failure = null;
      try {
        closeLiveDelivery();
      } catch (RuntimeException cleanup) {
        failure = cleanup;
      }
      closed = true;
      try {
        stateMemory.close();
      } catch (RuntimeException cleanup) {
        if (failure == null) failure = cleanup;
        else if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      try {
        account.close();
      } catch (RuntimeException cleanup) {
        if (failure == null) failure = cleanup;
        else if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      if (failure != null) throw failure;
    }
  }
}
