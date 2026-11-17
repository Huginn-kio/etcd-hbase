package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Creates one table-group tailer and applies its read proposals on the owner lane. */
final class EventTailerCycle {
  private static final int MAX_REVISION_EVENTS = 4096;
  private static final long MAX_REVISION_BYTES = LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES;
  private static final int MAX_PROOF_PAGES = 4096;
  private static final long MAX_PROOF_BYTES = 512L << 20;

  private record TailerSeed(SelectedSequenceReader source, ReadView view, CommitState state) {}

  private final String tableGroup;
  private final SerialMutationQueue ownerLane;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final TableGroupStores stores;
  private final EventHistoryPinManager pins;
  private final BudgetManager budgets;
  private final ByteKey authorization;
  private final M3TableGroupRuntime.Pending pending;
  private final RequestMetrics metrics;

  EventTailerCycle(String tableGroup, SerialMutationQueue ownerLane,
      Supplier<Optional<CommitOwnerService.OwnerToken>> owner, TableGroupStores stores,
      EventHistoryPinManager pins, BudgetManager budgets, ByteKey authorization,
      M3TableGroupRuntime.Pending pending, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.ownerLane = Objects.requireNonNull(ownerLane, "ownerLane");
    this.owner = Objects.requireNonNull(owner, "owner");
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

  CompletableFuture<EventHistoryWorker.CycleResult> run(
      Optional<CommitOwnerService.OwnerToken> expectedOwner, RequestScope scope) {
    Objects.requireNonNull(expectedOwner, "expectedOwner");
    Objects.requireNonNull(scope, "scope");
    SharedEventTailer current = pending.eventTailer().orElse(null);
    if (current != null) return advance(current, expectedOwner, scope);
    return create(scope).thenCompose(seed -> {
      if (seed.isEmpty()) return idle(scope, null);
      return installOnOwnerLane(seed.orElseThrow(), expectedOwner, scope).thenCompose(installed ->
          installed.subscriptionCount() == 0
              ? idle(scope, installed)
              : CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false)));
    });
  }

  private CompletableFuture<Optional<TailerSeed>> create(RequestScope scope) {
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
    CompletableFuture<Optional<TailerSeed>> creation =
        stores.views().openAuxiliary(authorization, pinScope.scope).thenCompose(pinned -> {
      var reader = new EventIndexBackfillReader(stores.records(), stores.manifests());
      return reader.read(pinned, pinScope.scope, proof).thenCompose(checkpoint -> {
        ReadView view = pinned.view();
        pinned.close();
        if (checkpoint.isEmpty()) {
          return CompletableFuture.completedFuture(Optional.<TailerSeed>empty());
        }
        EventIndexBackfillCheckpoint completed = checkpoint.orElseThrow();
        return stores.control().read(scope).thenApply(snapshot -> {
          CommitState state = snapshot.orElseThrow(
              () -> new IllegalStateException("event control is missing")).state();
          if (!completed.complete() || completed.sequence() > view.finalizedSequence()
              || !completed.registryIdentity().equals(pins.registryIdentity())
              || !pins.namespace().equals(state.eventRegistryNamespace())) {
            throw new java.util.concurrent.CompletionException(
                new BackendFailure(BackendFailure.Category.UNAVAILABLE));
          }
          var source = new SelectedSequenceReader(pins, stores.control(), stores.decisions(),
              stores.manifestRepository(), new HBaseEventRevisionIndexRepository(stores.records()));
          source.observePublishedSequence(state.publishedSequence());
          return Optional.of(new TailerSeed(source, view, state));
        });
      }).whenComplete((ignored, failure) -> pinned.close());
    });
    return creation.whenComplete((ignored, failure) -> close(pinScope, failure));
  }

  private CompletableFuture<SharedEventTailer> installOnOwnerLane(TailerSeed seed,
      Optional<CommitOwnerService.OwnerToken> expectedOwner, RequestScope cycleScope) {
    var laneRequest = RequestScope.withoutDeadline();
    CompletableFuture<SharedEventTailer> installed;
    try {
      installed = ownerLane.submitBackground(laneRequest, laneScope -> {
        if (cycleScope.isCancelled() || cycleScope.isDeadlineExceeded()
            || !isCurrentOwner(expectedOwner) || !seedMatchesOwner(seed, expectedOwner)) {
          return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        BudgetManager.Account memory;
        try {
          memory = pending.eventTailerMemory(budgets, tableGroup);
        } catch (BackendFailure failure) {
          return CompletableFuture.failedFuture(failure);
        }
        // The checkpoint proves the captured legacy window is complete. Live delivery starts
        // after the finalized barrier that was captured with this same sequence/revision pair.
        var candidate = new SharedEventTailer(seed.source(), authorization, memory,
            seed.view().finalizedSequence(), seed.view().stableRevision(), 100_000,
            MAX_REVISION_EVENTS, MAX_REVISION_BYTES);
        try {
          return CompletableFuture.completedFuture(pending.installEventTailer(candidate));
        } catch (RuntimeException | Error failure) {
          candidate.close();
          throw failure;
        }
      });
    } catch (RuntimeException | Error failure) {
      laneRequest.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return installed.whenComplete((ignored, failure) -> laneRequest.close());
  }

  private static boolean seedMatchesOwner(TailerSeed seed,
      Optional<CommitOwnerService.OwnerToken> expectedOwner) {
    if (expectedOwner.isEmpty()) return true;
    var token = expectedOwner.orElseThrow();
    return seed.state().ownerEpoch() == token.epoch()
        && seed.state().ownerInstance().filter(token.instance()::equals).isPresent();
  }

  private boolean isCurrentOwner(Optional<CommitOwnerService.OwnerToken> expectedOwner) {
    return expectedOwner.equals(owner.get());
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
      Optional<CommitOwnerService.OwnerToken> expectedOwner, RequestScope scope) {
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
    return advanceOneRevision(tailer, expectedOwner, scope, budget, false).thenApply(result -> {
      boolean publishedRevision = tailer.completedRevision() > previousRevision;
      metrics.recordEventTailerCycle(result.progressed(),
          publishedRevision ? tailer.lastPublishedEventCount() : 0,
          publishedRevision ? tailer.lastPublishedEventBytes() : 0,
          scope.storageCostSnapshot().minus(before),
          tailer.completedSequence(), tailer.completedRevision(), tailer.observedPublishedSequence(),
          tailer.subscriptionCount());
      return result;
    }).exceptionallyCompose(failure -> {
      Throwable cause = CompletionFailures.unwrapCompletion(failure);
      if (tailer.isClosed()) pending.detachClosedEventTailer(tailer);
      metrics.recordEventTailerFailure(cause instanceof BackendFailure backend
          ? backend.category() : BackendFailure.Category.UNKNOWN);
      return CompletableFuture.failedFuture(failure);
    });
  }

  private CompletableFuture<EventHistoryWorker.CycleResult> advanceOneRevision(SharedEventTailer tailer,
      Optional<CommitOwnerService.OwnerToken> expectedOwner, RequestScope scope,
      ManifestCatalog.ProofReadBudget budget, boolean progressed) {
    return tailer.prepareStep(scope, budget)
        .thenCompose(proposal -> applyOnOwnerLane(tailer, proposal, expectedOwner, scope))
        .thenCompose(step -> switch (step) {
          case WAITING -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(progressed, false));
          case METADATA -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false));
          case PUBLISHED -> CompletableFuture.completedFuture(new EventHistoryWorker.CycleResult(true, false));
          case OPENED, BUFFERED -> advanceOneRevision(tailer, expectedOwner, scope, budget, true);
        });
  }

  private CompletableFuture<SharedEventTailer.Step> applyOnOwnerLane(
      SharedEventTailer tailer, SharedEventTailer.StepProposal proposal,
      Optional<CommitOwnerService.OwnerToken> expectedOwner, RequestScope cycleScope) {
    var laneRequest = RequestScope.withoutDeadline();
    CompletableFuture<SharedEventTailer.Step> applied;
    try {
      applied = ownerLane.submitBackground(laneRequest, laneScope -> {
        if (cycleScope.isCancelled() || cycleScope.isDeadlineExceeded() || !isCurrentOwner(expectedOwner)) {
          return tailer.discardStep(proposal).thenCompose(ignored ->
              CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
        }
        return tailer.applyStep(proposal);
      });
    } catch (RuntimeException | Error failure) {
      laneRequest.close();
      return tailer.discardStep(proposal).thenCompose(ignored -> CompletableFuture.failedFuture(failure));
    }
    return applied.whenComplete((ignored, failure) -> {
      laneRequest.close();
      if (failure != null) tailer.discardStep(proposal);
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

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  /** The pin owns this child; closing it waits for its actual backend producers. */
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
