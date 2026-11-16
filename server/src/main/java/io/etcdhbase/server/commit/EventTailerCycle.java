package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Creates one table-group tailer from the durable backfill barrier and advances bounded work. */
final class EventTailerCycle {
  private static final int MAX_REVISION_EVENTS = 4096;
  private static final long MAX_REVISION_BYTES = LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES;
  private static final int MAX_PROOF_PAGES = 4096;
  private static final long MAX_PROOF_BYTES = 512L << 20;

  private final String tableGroup;
  private final TableGroupStores stores;
  private final EventHistoryPinManager pins;
  private final BudgetManager budgets;
  private final ByteKey authorization;
  private final M3TableGroupRuntime.Pending pending;
  private final RequestMetrics metrics;

  EventTailerCycle(String tableGroup, TableGroupStores stores, EventHistoryPinManager pins,
      BudgetManager budgets, ByteKey authorization, M3TableGroupRuntime.Pending pending,
      RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.stores = Objects.requireNonNull(stores, "stores");
    this.pins = Objects.requireNonNull(pins, "pins");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.pending = Objects.requireNonNull(pending, "pending");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || authorization.length() != 32) {
      throw new IllegalArgumentException("invalid event tailer identity");
    }
  }

  CompletableFuture<EventHistoryWorker.CycleResult> run(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    SharedEventTailer current = pending.eventTailer().orElse(null);
    if (current != null) return advance(current, scope);
    return create(scope).thenCompose(created -> {
      if (created.isEmpty()) return idle(scope, null);
      SharedEventTailer installed = pending.installEventTailer(created.orElseThrow());
      if (installed.subscriptionCount() == 0) return idle(scope, installed);
      return advance(installed, scope);
    });
  }

  private CompletableFuture<Optional<SharedEventTailer>> create(RequestScope scope) {
    Child pinScope = new Child(scope);
    BudgetManager.Account readAccount;
    try {
      readAccount = budgets.admitReserved(tableGroup, pinScope.scope,
          LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY,
          LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);
    } catch (BackendFailure failure) {
      close(pinScope, failure);
      return CompletableFuture.failedFuture(failure);
    }
    var proof = new ManifestCatalog.ProofReadBudget(pinScope.scope, readAccount,
        MAX_PROOF_PAGES, MAX_PROOF_BYTES);
    CompletableFuture<Optional<SharedEventTailer>> creation =
        stores.views().openAuxiliary(authorization, pinScope.scope).thenCompose(pinned -> {
      var reader = new EventIndexBackfillReader(stores.records(), stores.manifests());
      return reader.read(pinned, pinScope.scope, proof).thenCompose(checkpoint -> {
        ReadView view = pinned.view();
        pinned.close();
        if (checkpoint.isEmpty()) {
          return CompletableFuture.completedFuture(Optional.<SharedEventTailer>empty());
        }
        EventIndexBackfillCheckpoint completed = checkpoint.orElseThrow();
        return stores.control().read(scope).thenApply(snapshot -> {
          var state = snapshot.orElseThrow(() -> new IllegalStateException("event control is missing")).state();
          if (!completed.complete() || completed.sequence() > view.finalizedSequence()
              || !completed.registryIdentity().equals(pins.registryIdentity())
              || !pins.namespace().equals(state.eventRegistryNamespace())) {
            throw new java.util.concurrent.CompletionException(
                new BackendFailure(BackendFailure.Category.UNAVAILABLE));
          }
          BudgetManager.Account memory;
          try {
            memory = pending.eventTailerMemory(budgets, tableGroup);
          } catch (BackendFailure failure) {
            throw new java.util.concurrent.CompletionException(failure);
          }
          var source = new SelectedSequenceReader(pins, stores.control(), stores.decisions(),
              stores.manifestRepository(), new HBaseEventRevisionIndexRepository(stores.records()));
          source.observePublishedSequence(state.publishedSequence());
          return Optional.of(new SharedEventTailer(source, authorization, memory,
              completed.upperSequence(), completed.upperRevision(), 100_000, MAX_REVISION_EVENTS,
              MAX_REVISION_BYTES));
        });
      }).whenComplete((ignored, failure) -> pinned.close());
    });
    return creation.whenComplete((ignored, failure) -> close(pinScope, failure));
  }

  private static void close(Child child, Throwable primary) {
    try {
      child.close();
    } catch (Exception cleanup) {
      if (primary != null) {
        if (primary != cleanup) primary.addSuppressed(cleanup);
      } else {
        throw new java.util.concurrent.CompletionException(cleanup);
      }
    }
  }

  private CompletableFuture<EventHistoryWorker.CycleResult> advance(SharedEventTailer tailer,
      RequestScope scope) {
    if (tailer.subscriptionCount() == 0) return idle(scope, tailer);
    BudgetManager.Account readAccount;
    try {
      readAccount = budgets.admitReserved(tableGroup, scope, LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY,
          LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);
    } catch (BackendFailure failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var budget = new ManifestCatalog.ProofReadBudget(scope, readAccount, MAX_PROOF_PAGES, MAX_PROOF_BYTES);
    var before = scope.storageCostSnapshot();
    long previousRevision = tailer.completedRevision();
    return advanceOneRevision(tailer, scope, budget, false).thenApply(result -> {
      boolean publishedRevision = tailer.completedRevision() > previousRevision;
      metrics.recordEventTailerCycle(result.progressed(),
          publishedRevision ? tailer.lastPublishedEventCount() : 0,
          publishedRevision ? tailer.lastPublishedEventBytes() : 0,
          scope.storageCostSnapshot().minus(before),
          tailer.completedSequence(), tailer.completedRevision(), tailer.observedPublishedSequence(),
          tailer.subscriptionCount());
      return result;
    }).exceptionallyCompose(failure -> {
      Throwable cause = failure;
      while ((cause instanceof java.util.concurrent.CompletionException
          || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
        cause = cause.getCause();
      }
      if (tailer.isClosed()) pending.detachClosedEventTailer(tailer);
      metrics.recordEventTailerFailure(cause instanceof BackendFailure backend
          ? backend.category() : BackendFailure.Category.UNKNOWN);
      return CompletableFuture.failedFuture(failure);
    });
  }

  private CompletableFuture<EventHistoryWorker.CycleResult> advanceOneRevision(SharedEventTailer tailer,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget, boolean progressed) {
    return tailer.step(scope, budget).thenCompose(step -> switch (step) {
      case WAITING -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(progressed, false));
      case METADATA -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false));
      case PUBLISHED -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false));
      case OPENED, BUFFERED -> advanceOneRevision(tailer, scope, budget, true);
    });
  }

  private CompletableFuture<EventHistoryWorker.CycleResult> idle(RequestScope scope,
      SharedEventTailer tailer) {
    var cost = scope.storageCostSnapshot();
    metrics.recordEventTailerCycle(false, 0, 0, cost, tailer == null ? 0 : tailer.completedSequence(),
        tailer == null ? 0 : tailer.completedRevision(),
        tailer == null ? 0 : tailer.observedPublishedSequence(),
        tailer == null ? 0 : tailer.subscriptionCount());
    return CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(false, false));
  }

  /** The pin owns this child; closing it must not cancel the owner-lane cycle scope. */
  private static final class Child implements AutoCloseable {
    private final RequestScope scope;
    private final CompletableFuture<Void> drained;
    private final RequestScope.ScopedCleanup registration;

    private Child(RequestScope parent) {
      scope = parent.childWithTimeoutNanos(parent.remainingDeadlineNanos());
      drained = CompletableFuture.allOf(scope.cleanupCompletion().toCompletableFuture(),
          scope.physicalProducersCompletion().toCompletableFuture());
      registration = parent.registerScopedCleanup(scope, drained);
    }

    @Override public void close() throws Exception {
      registration.close();
      drained.join();
    }
  }
}
