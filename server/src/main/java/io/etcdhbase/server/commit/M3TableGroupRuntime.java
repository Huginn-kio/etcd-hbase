package io.etcdhbase.server.commit;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns the local KV runtime for one validated table group; the caller owns its HBase factory. */
public final class M3TableGroupRuntime implements AutoCloseable {
  private static final BudgetManager.Reservation REQUEST_MEMORY =
      new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 64L << 20);
  private static final BudgetManager.Reservation RECOVERY_MEMORY =
      new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 8L << 20, 48L << 20);
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m3-runtime-cleanup").start(task);

  private final Pending resources;
  private final KvService kvService;
  private final long clusterId;
  private final long memberId;
  private final RequestMetrics metrics;

  private M3TableGroupRuntime(Pending resources, KvService kvService, long clusterId, long memberId,
      RequestMetrics metrics) {
    this.resources = resources;
    this.kvService = kvService;
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.metrics = metrics;
  }

  /**
   * Opens an already initialized group and completes recovery before returning. The caller owns
   * the startup scope and factory and must not close the latter until startup and runtime physical
   * producers have drained, including when startup fails.
   */
  public static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup) throws Exception {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(activation, "activation");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(connections, "connections");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(budgets, "budgets");
    Objects.requireNonNull(scans, "scans");
    Objects.requireNonNull(startup, "startup");
    if (limits.maxConcurrentRequests() < 1 || limits.maxConcurrentRequests() > 100_000
        || limits.requestDeadlineMs() < 1 || limits.requestDeadlineMs() > 600_000
        || limits.maxScannersPerTable() < 1 || limits.maxScannersPerTable() > 1024
        || activation.maxEtcdRequestBytes() > limits.maxRpcBytes()
        || limits.maxRpcBytes() > limits.maxRequestBytes() || limits.maxQueuedRequests() != 0) {
      throw new IllegalArgumentException("M3 transport or queue limits are incompatible");
    }
    Pending pending = null;
    try {
      var stores = await(TableGroupStores.openConfigured(connections, identity, plan,
          activation.pinConfiguration(), startup), startup);
      pending = new Pending(startup);
      pending.initialize(plan, activation, limits, budgets);
      var invocations = new InvocationService(stores.invocations());
      var owners = new CommitOwnerService(stores.control());
      var recovery = new RecoveryManager(plan.system().profileFingerprint(), stores.control(), owners,
          stores.invocations(), stores.outcomes(), stores.finalizer());
      String instance = CommitOwnerService.newInstanceId();
      var ownerFuture = M3OwnerLease.acquire(recovery, owners, instance,
          Duration.ofMillis(activation.ownerLeaseMs()), Duration.ofMillis(activation.heartbeatMs()),
          Duration.ofMillis(2L * activation.heartbeatMs()), startup, pending.queue::close);
      pending.trackOwner(ownerFuture);
      var owner = await(ownerFuture, startup);
      ByteKey authorization = authorizationDigest(identity);
      var business = assembleBusiness(plan, activation, connections, budgets, scans, pending, stores, invocations, owner);
      var service = assembleService(plan, activation, identity, authorization, budgets, pending, stores,
          invocations, owner, business);
      startup.closeAsync(CLEANUP);
      await(CompletableFuture.allOf(startup.cleanupCompletion().toCompletableFuture(),
          startup.physicalProducersCompletion().toCompletableFuture()), startup);
      if (!owner.isReady()) throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      return new M3TableGroupRuntime(pending, service,
          stores.clusterId(), activation.memberIdBits(), business.metrics());
    } catch (Exception failure) {
      throw new StartupException(failure, abort(startup, pending, failure));
    } catch (Error fatal) {
      fatal.addSuppressed(new StartupCleanup(abort(startup, pending, fatal)));
      throw fatal;
    }
  }

  private record BusinessComponents(PutOwnerProcessor putProcessor, TxnOwnerProcessor txnProcessor,
      TxnReadExecutor txnReads, RequestMetrics metrics) {}

  private static BusinessComponents assembleBusiness(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      HBaseConnectionFactory connections, BudgetManager budgets, ScanScheduler scans, Pending pending,
      TableGroupStores stores, InvocationService invocations, M3OwnerLease owner) {
    var preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()),
        stores.results(), new ManifestPreparation(stores.manifestRepository(),
            new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations);
    var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
        new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results()),
        stores.invocations(), pending.handoff, stores.finalizer());
    var metrics = new RequestMetrics();
    var executor = new PutMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
        stores.control(), stores.views(), stores.manifests(), connections, plan.primary(), pending.worker,
        preparation, publisher, 5L << 20, 8L << 20, metrics);
    var putProcessor = new PutOwnerProcessor(plan.source().tableGroupId(), owner.token(), budgets,
        REQUEST_MEMORY, executor, stores.outcomes());
    var txnLimits = new TxnExecutionLimits(5L << 20, 8L << 20, 32L << 20, 4096, 4096);
    var txnPreparation = new TxnCommitPreparation(new ResultPreparation(stores.resultRepository()),
        stores.results(), new ManifestPreparation(stores.manifestRepository(),
            new ManifestPreparation.Limits(128, 4L << 20, 128, 8L << 20)), invocations, 8L << 20);
    var txnExecutor = new TxnMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
        stores.control(), stores.views(), stores.manifests(), stores.records(), stores.manifestRepository(),
        connections, plan.primary(), pending.worker, scans, txnPreparation, publisher, txnLimits, metrics);
    var txnProcessor = new TxnOwnerProcessor(plan.source().tableGroupId(), owner.token(), budgets,
        REQUEST_MEMORY, txnExecutor, stores.outcomes(), activation.maxEtcdRequestBytes());
    var txnReads = new TxnReadExecutor(plan.source().tableGroupId(), stores.views(), stores.manifests(),
        connections, plan.primary(), pending.worker, scans, txnLimits, metrics);
    return new BusinessComponents(putProcessor, txnProcessor, txnReads, metrics);
  }

  private static KvService assembleService(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      AuthorizedBackendIdentity identity, ByteKey authorization, BudgetManager budgets, Pending pending,
      TableGroupStores stores, InvocationService invocations, M3OwnerLease owner, BusinessComponents business) {
    var receiver = new MutationForwardReceiver(stores.control(), invocations,
        (peer, request, scope) -> peer.equals(owner.token().instance())
            ? CompletableFuture.completedFuture(new MutationForwardReceiver.VerifiedAuthorization(identity, authorization))
            : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)),
        (request, binding, principal, scope) -> MutationOperationEnvelope.isTransaction(request.operationBytes())
            ? business.txnProcessor().process(request, binding, principal, scope)
            : business.putProcessor().process(request, binding, principal, scope));
    var lane = new MutationOwnerLane(plan.source().tableGroupId(), pending.queue, receiver, business.metrics());
    var route = new OwnerForwarding.Route(owner.token().instance(), owner.token().epoch(),
        owner.token().instance(), owner.token().instance());
    // A direct same-process call has no network peer. Unknown or remote owners are rejected;
    // authenticated cross-process forwarding remains a separate deployment capability.
    var relay = new MutationRelay(stores.control(),
        (group, instanceId, epoch) -> group.equals(plan.source().tableGroupId())
            && instanceId.equals(owner.token().instance()) && epoch == owner.token().epoch()
                ? CompletableFuture.completedFuture(route)
                : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)),
        (selected, request, scope) -> selected.equals(route)
            ? lane.receive(owner.token().instance(), request, scope)
            : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)));
    var gateway = new PutGateway(plan.source().tableGroupId(), identity, authorization,
        stores.clusterId(), activation.memberIdBits(), activation.maxEtcdRequestBytes(),
        stores.control(), relay, stores.invocationResults(), budgets, REQUEST_MEMORY, pending.worker, business.metrics());
    var txnGateway = new TxnGateway(plan.source().tableGroupId(), identity, authorization,
        stores.clusterId(), activation.memberIdBits(), activation.maxEtcdRequestBytes(),
        stores.control(), relay, stores.invocationResults(), business.txnReads(), budgets, REQUEST_MEMORY, pending.worker, business.metrics());
    return new KvService(gateway, txnGateway);
  }

  public KvService kvService() { return kvService; }
  public long clusterId() { return clusterId; }
  public long memberId() { return memberId; }
  public RequestMetrics metrics() { return metrics; }
  public boolean isReady() { return resources.isReady(); }
  public CompletableFuture<Void> drained() { return resources.drained.copy(); }
  @Override public void close() { resources.close(); }

  private static CompletableFuture<Void> abort(RequestScope startup, Pending pending, Throwable failure) {
    try { startup.closeAsync(CLEANUP); }
    catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
    if (pending != null) {
      pending.resolveNoOwnerIfUnstarted();
      try { pending.close(); }
      catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
      return pending.drained;
    }
    return CompletableFuture.allOf(startup.cleanupCompletion().toCompletableFuture(),
        startup.physicalProducersCompletion().toCompletableFuture());
  }

  /** Startup failed; callers retain the factory until this completion has settled successfully. */
  public static final class StartupException extends Exception {
    private static final long serialVersionUID = 1L;
    private final CompletableFuture<Void> cleanup;

    private StartupException(Throwable cause, CompletableFuture<Void> cleanup) {
      super("M3 table-group startup failed", cause);
      this.cleanup = cleanup;
    }

    public CompletionStage<Void> cleanup() { return cleanup.minimalCompletionStage(); }
  }

  /** Carries the same ownership completion when a fatal startup error must be rethrown. */
  public static final class StartupCleanup extends Exception {
    private static final long serialVersionUID = 1L;
    private final CompletableFuture<Void> cleanup;

    private StartupCleanup(CompletableFuture<Void> cleanup) {
      super("M3 startup cleanup is still owned", null, false, false);
      this.cleanup = cleanup;
    }

    public CompletionStage<Void> cleanup() { return cleanup.minimalCompletionStage(); }
  }

  private static ByteKey authorizationDigest(AuthorizedBackendIdentity identity) {
    if (identity.allowedTableGroupIds().size() > 64) {
      throw new IllegalArgumentException("identity has too many authorized table groups");
    }
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(1);
        output.writeUTF(identity.providerId());
        output.writeUTF(identity.subject());
        output.writeUTF(identity.scope().tableGroupId());
        output.writeUTF(identity.scope().connectionProfileId());
        output.writeLong(identity.credentialGeneration());
        output.writeInt(identity.allowedTableGroupIds().size());
        for (String group : identity.allowedTableGroupIds().stream().sorted().toList()) output.writeUTF(group);
      }
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException failure) {
      throw new IllegalStateException("cannot derive the authorization scope digest", failure);
    }
  }

  private static <T> T await(CompletableFuture<T> future, RequestScope scope) throws Exception {
    long remaining = scope.remainingDeadlineNanos();
    if (remaining <= 0) throw new TimeoutException("M3 startup deadline expired");
    try {
      return future.get(remaining, TimeUnit.NANOSECONDS);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception exception) throw exception;
      if (cause instanceof Error fatal) throw fatal;
      throw new IllegalStateException("M3 startup failed", cause);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  /** Owns all lanes until their real producers, timers, workers, and reserved recovery capacity drain. */
  private static final class Pending {
    private final RequestScope startup;
    private final RequestScope recoveryScope = RequestScope.withoutDeadline();
    private final CompletableFuture<Void> ownerResolved = new CompletableFuture<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private volatile M3OwnerLease owner;
    private volatile Throwable shutdownFailure;
    private boolean ownerStarted;
    private SerialMutationQueue queue;
    private BoundedPublicationHandoff handoff;
    private ThreadPoolExecutor worker;

    private Pending(RequestScope startup) { this.startup = startup; }

    private void initialize(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
        RuntimeConfiguration.ResourceBudgets limits, BudgetManager budgets) throws BackendFailure {
      var account = budgets.admit(plan.source().tableGroupId(), recoveryScope, RECOVERY_MEMORY);
      var slot = account.tryAcquire(new BudgetManager.Work(0, 1, 0, 1, 0))
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      var released = new CompletableFuture<Void>();
      try {
        recoveryScope.registerScopedCleanup(() -> startup.physicalProducersCompletion().whenComplete((ignored, failure) -> {
          try { slot.close(); released.complete(null); }
          catch (RuntimeException | Error cleanup) { released.completeExceptionally(cleanup); }
        }), released);
      } catch (RuntimeException | Error failure) {
        try { slot.close(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
        throw failure;
      }
      Duration deadline = Duration.ofMillis(Math.min(limits.requestDeadlineMs(), 300_000));
      long queueBytes = Math.addExact(8192L, Math.multiplyExact(8L, activation.maxEtcdRequestBytes()));
      queue = new SerialMutationQueue(0, queueBytes, deadline);
      handoff = new BoundedPublicationHandoff(1, 1, deadline);
      int queuedWorkers = (int) Math.min(1024L, Math.max(8L, (long) limits.maxConcurrentRequests() * 2));
      worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queuedWorkers),
          Thread.ofPlatform().daemon().name("m3-backend-", 0).factory());
    }

    private void trackOwner(CompletableFuture<M3OwnerLease> future) {
      ownerStarted = true;
      future.whenComplete((value, failure) -> {
        owner = value;
        ownerResolved.complete(null);
      });
    }

    private void resolveNoOwnerIfUnstarted() {
      if (!ownerStarted) ownerResolved.complete(null);
    }

    private boolean isReady() {
      M3OwnerLease current = owner;
      return !closing.get() && current != null && current.isReady();
    }

    private void close() {
      if (!closing.compareAndSet(false, true)) return;
      try {
        if (queue != null) queue.close();
      } catch (RuntimeException | Error failure) {
        shutdownFailure = failure;
      }
      try {
        Thread.ofVirtual().name("m3-runtime-drain").start(this::drain);
      } catch (RuntimeException | Error failure) {
        shutdownFailure = merge(shutdownFailure, failure);
        drain();
        throw failure;
      }
    }

    private void drain() {
      Throwable failure = shutdownFailure;
      try {
        if (queue != null) queue.drained().join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (handoff != null) { handoff.close(); handoff.drained().join(); }
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        ownerResolved.join();
        if (owner != null) {
          owner.close();
          owner.drained().join();
        }
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (owner != null) while (!owner.awaitTermination(Duration.ofSeconds(30))) { /* wait for the owned timer */ }
        if (queue != null) while (!queue.awaitTermination(Duration.ofSeconds(30))) { /* wait for the owned lane */ }
        if (handoff != null) while (!handoff.awaitTermination(Duration.ofSeconds(30))) { /* wait for the publication lane */ }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        failure = merge(failure, interrupted);
      }
      if (worker != null) {
        worker.shutdown();
        try {
          while (!worker.awaitTermination(30, TimeUnit.SECONDS)) { /* wait for owned backend work */ }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          failure = merge(failure, interrupted);
        }
      }
      try {
        recoveryScope.closeAsync(CLEANUP);
        CompletableFuture.allOf(recoveryScope.cleanupCompletion().toCompletableFuture(),
            recoveryScope.physicalProducersCompletion().toCompletableFuture()).join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    }

    private static Throwable merge(Throwable first, Throwable next) {
      if (first == null) return next;
      if (first != next) first.addSuppressed(next);
      return first;
    }
  }
}
