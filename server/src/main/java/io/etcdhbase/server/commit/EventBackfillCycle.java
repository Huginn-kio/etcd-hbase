package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.EventBackfillWindow;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCapture;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillPlanner;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.EventIndexBackfillStep;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** One owner-lane backfill cycle, including protected reads and one durable M2 publication. */
final class EventBackfillCycle {
  private static final int MAX_PROOF_PAGES = 16_384;
  private static final long MAX_PROOF_BYTES = MAX_PROOF_PAGES * ManifestCatalog.maximumProofReadPageBytes();
  private static final int MAX_EVENTS_PER_SEQUENCE = 4096;
  // Proof callbacks can retain one page while starting the next control or manifest read.
  private static final BudgetManager.Reservation RESERVATION = LeaseBudgetProfile.EVENT_BACKFILL_MEMORY;
  private static final BudgetManager.ReadLimits READ_LIMITS =
      new BudgetManager.ReadLimits(MAX_PROOF_PAGES, MAX_PROOF_BYTES, MAX_PROOF_PAGES,
          MAX_PROOF_PAGES, MAX_PROOF_PAGES);

  private final String tableGroup;
  private final TableGroupStores stores;
  private final EventHistoryPinManager pins;
  private final InvocationService invocations;
  private final EventBackfillExecutor publisher;
  private final BudgetManager budgets;
  private final ByteKey authorization;
  private final Clock clock;

  EventBackfillCycle(String tableGroup, TableGroupStores stores, EventHistoryPinManager pins,
      InvocationService invocations, EventBackfillPreparation preparation, CommitPublisher publisher,
      BudgetManager budgets, ByteKey authorization, Clock clock) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.stores = Objects.requireNonNull(stores, "stores");
    this.pins = Objects.requireNonNull(pins, "pins");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.publisher = new EventBackfillExecutor(stores.control(), preparation, publisher, clock);
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    if (tableGroup.isBlank() || authorization.length() != 32 || pins.namespace().isEmpty()) {
      throw new IllegalArgumentException("invalid event backfill identity or selected registry");
    }
  }

  /** True means the entire captured legacy window is durably finalized. */
  CompletableFuture<Boolean> run(CommitOwnerService.OwnerToken owner, RequestScope scope) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    var result = new CompletableFuture<Boolean>();
    try {
      Thread.ofVirtual().name("event-backfill-cycle").start(() -> {
        try {
          result.complete(execute(owner, scope));
        } catch (Throwable failure) {
          result.completeExceptionally(failure);
          if (failure instanceof Error fatal) throw fatal;
        } finally {
          drained.complete(null);
        }
      });
    } catch (RuntimeException | Error failure) {
      drained.complete(null);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return result.copy();
  }

  private boolean execute(CommitOwnerService.OwnerToken owner, RequestScope parent) throws Exception {
    Optional<EventIndexBackfillCheckpoint> current = readCheckpoint(parent);
    try (Child windowScope = new Child(parent)) {
      var account = budgets.admitReserved(tableGroup, windowScope.scope, RESERVATION, READ_LIMITS);
      var budget = proof(windowScope.scope, account);
      CommitState state = requireOwner(owner, windowScope.scope);
      if (current.isPresent()) {
        var previous = current.orElseThrow();
        if (previous.sequence() > state.finalizedSequence()
            || !previous.registryIdentity().equals(pins.registryIdentity())
            || !pins.namespace().equals(state.eventRegistryNamespace())) {
          throw new IllegalStateException("backfill checkpoint is not finalized in the selected registry");
        }
        if (previous.complete()) return true;
      }
      EventIndexBackfillCheckpoint checkpoint;
      EventBackfillWindow window;
      if (current.isEmpty()) {
        checkpoint = new EventIndexBackfillPlanner(stores.control(), stores.decisions())
            .plan(owner, pins.namespace().orElseThrow(), pins.registryIdentity(), windowScope.scope, budget).join();
        window = new EventIndexBackfillCapture(pins, stores.control(), stores.decisions())
            .open(checkpoint, authorization, windowScope.scope, budget).join();
      } else {
        checkpoint = current.orElseThrow();
        window = EventBackfillWindow.open(pins, stores.control(), checkpoint, authorization,
            windowScope.scope, budget).join();
      }
      try (window) {
        EventIndexBackfillCheckpoint proposal = current.isEmpty() ? checkpoint
            : verifyNext(owner, state, checkpoint, window, windowScope.scope, budget);
        publish(owner, state, proposal, window, windowScope.scope, account, budget);
        return proposal.complete();
      } finally {
        window.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  private Optional<EventIndexBackfillCheckpoint> readCheckpoint(RequestScope parent) throws Exception {
    try (Child readScope = new Child(parent)) {
      var account = budgets.admitReserved(tableGroup, readScope.scope, RESERVATION, READ_LIMITS);
      var budget = proof(readScope.scope, account);
      var pinned = stores.views().openAuxiliary(authorization, readScope.scope).join();
      return new EventIndexBackfillReader(stores.records(), stores.manifests())
          .read(pinned, readScope.scope, budget).join();
    }
  }

  private EventIndexBackfillCheckpoint verifyNext(CommitOwnerService.OwnerToken owner, CommitState state,
      EventIndexBackfillCheckpoint checkpoint, EventBackfillWindow window, RequestScope parent,
      ManifestCatalog.ProofReadBudget windowBudget) throws Exception {
    try (Child stepScope = new Child(parent)) {
      var account = budgets.admitReserved(tableGroup, stepScope.scope, RESERVATION, READ_LIMITS);
      var budget = proof(stepScope.scope, account);
      var source = new SelectedSequenceReader(pins, stores.control(), stores.decisions(),
          stores.manifestRepository(), new HBaseEventRevisionIndexRepository(stores.records()));
      long nextSequence = Math.addExact(state.publishedSequence(), 1);
      try (var step = EventIndexBackfillStep.open(source, stores.control(), window, checkpoint,
          authorization, owner.epoch(), nextSequence, stepScope.scope, budget, windowBudget).join()) {
        for (int count = 0; count <= MAX_EVENTS_PER_SEQUENCE; count++) {
          var proposal = step.next(budget, windowBudget).join();
          if (proposal.isPresent()) return proposal.orElseThrow().checkpoint();
        }
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
    }
  }

  private void publish(CommitOwnerService.OwnerToken owner, CommitState state,
      EventIndexBackfillCheckpoint checkpoint, EventBackfillWindow window, RequestScope scope,
      BudgetManager.Account account, ManifestCatalog.ProofReadBudget budget) {
    var proposed = InvocationBinding.begin(InvocationId.create(), digest(checkpoint), authorization,
        TransactionId.create(), owner.epoch(), state.publishedSequence(), state.publishedRevision());
    var invocation = invocations.bindOrJoin(proposed, scope).join();
    var result = publisher.publish(invocation, owner, checkpoint, window, scope, account, budget).join();
    if (result.state() != CommitOutcomeResolver.State.SELECTED_FINALIZED) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private CommitState requireOwner(CommitOwnerService.OwnerToken owner, RequestScope scope) {
    CommitState state = stores.control().read(scope).join().orElseThrow(
        () -> new IllegalStateException("backfill control row is missing")).state();
    if (state.ownerEpoch() != owner.epoch() || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || state.ownerLeaseExpired(clock.millis()) || state.publication().isPresent()
        || state.publishedSequence() != state.finalizedSequence()) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return state;
  }

  private static ManifestCatalog.ProofReadBudget proof(RequestScope scope, BudgetManager.Account account) {
    return new ManifestCatalog.ProofReadBudget(scope, account, MAX_PROOF_PAGES, MAX_PROOF_BYTES);
  }

  private static ByteKey digest(EventIndexBackfillCheckpoint checkpoint) {
    try {
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(checkpoint.encode().bytes()));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  /** Parent cancellation stops the child; release waits for its physical producers. */
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
