package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Renews one Lease through a selected M2 auxiliary-only commit. */
public final class LeaseRenewalExecutor {
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);
  private static final int MAX_EXAMINED_LEASES = 1_024;
  private static final long MAX_METADATA_BYTES = 64L << 20;

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final long clusterId;
  private final long memberId;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final LeaseReadView leases;
  private final LeaseTimeSource time;
  private final Clock ownerClock;
  private final LeaseRenewalPreparation preparation;
  private final CommitPublisher publisher;
  private final RequestMetrics metrics;

  public LeaseRenewalExecutor(String tableGroup, ByteKey profileFingerprint,
      long clusterId, long memberId, CommitControlStore control, ReadViewService views,
      LeaseReadView leases, LeaseTimeSource time, Clock ownerClock,
      LeaseRenewalPreparation preparation, CommitPublisher publisher, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.leases = Objects.requireNonNull(leases, "leases");
    this.time = Objects.requireNonNull(time, "time");
    this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32
        || clusterId == 0 || memberId == 0) {
      throw new IllegalArgumentException("invalid KeepAlive execution configuration");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
  }

  /** Caller owns the serialized mutation lane and the durable invocation binding. */
  public CompletableFuture<InvocationId> executeNew(long leaseId, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid KeepAlive attempt"));
    }
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }

    var drained = new CompletableFuture<Void>();
    RequestScope.StorageCostSnapshot initialStorageCost = scope.storageCostSnapshot();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    CompletableFuture<InvocationId> operation;
    try {
      PutCommitPreparation.retainUntilDrain(scope, account.tryAcquire(FINALIZATION_CAPACITY)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      operation = currentBase(binding, owner, scope)
          .thenCompose(ignored -> {
            long started = System.nanoTime();
            return views.openAuxiliary(binding.authorizationScopeDigest(), scope)
                .thenCompose(pinned -> prepare(leaseId, binding, pinned, scope, account))
                .whenComplete((sealed, failure) -> metrics.recordLeaseRenewalStage(
                    RequestMetrics.LeaseRenewalStage.PREPARE,
                    Math.max(0, System.nanoTime() - started)));
          })
          .thenCompose(prepared -> {
            if (prepared instanceof LeaseRenewalPreparation.NoChanges noChanges) {
              return CompletableFuture.completedFuture(noChanges.invocation().invocationId());
            }
            LeaseRenewalPreparation.Sealed sealed =
                (LeaseRenewalPreparation.Sealed) prepared;
            long started = System.nanoTime();
            return currentBase(binding, owner, scope)
                .thenCompose(ignored -> publisher.publish(
                    sealed.invocation(), owner, sealed.manifest(), scope))
                .thenCompose(outcome -> {
                  if (outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED) {
                    metrics.recordLeaseRenewalSelected(
                        sealed.auxiliaryRecordCount(), sealed.auxiliaryPayloadBytes());
                    return CompletableFuture.completedFuture(binding.invocationId());
                  }
                  return CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.UNKNOWN));
                })
                .whenComplete((id, failure) -> metrics.recordLeaseRenewalStage(
                    RequestMetrics.LeaseRenewalStage.PUBLISH_AND_FINALIZE,
                    Math.max(0, System.nanoTime() - started)));
          });
    } catch (BackendFailure | RuntimeException failure) {
      metrics.recordLeaseRenewalStorageCost(
          scope.storageCostSnapshot().minus(initialStorageCost));
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      metrics.recordLeaseRenewalStorageCost(
          scope.storageCostSnapshot().minus(initialStorageCost));
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> {
      metrics.recordLeaseRenewalStorageCost(
          scope.storageCostSnapshot().minus(initialStorageCost));
      drained.complete(null);
    });
    return operation.copy();
  }

  private CompletableFuture<LeaseRenewalPreparation.Prepared> prepare(long leaseId,
      InvocationBinding binding, PinnedReadView pinned, RequestScope scope,
      BudgetManager.Account account) {
    if (pinned.view().barrierSequence() != binding.baseSequence()
        || pinned.view().targetRevision() != binding.baseRevision()
        || pinned.view().selectedDecision().isPresent()
        || binding.baseSequence() == Long.MAX_VALUE) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var readBudget = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_EXAMINED_LEASES, MAX_METADATA_BYTES);
    return leases.find(pinned, leaseId, scope, readBudget).thenCompose(found -> {
      var observed = time.observe();
      LeaseState renewed = null;
      long ttl = 0;
      if (found.isPresent()) {
        LeaseState current = found.orElseThrow();
        if (current.identity().leaseId() != leaseId) {
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.UNKNOWN));
        }
        if (current.status() == LeaseState.Status.ACTIVE) {
          if (LeaseTimeRules.isExpired(current, observed)) {
            // J43 will select the durable revoke. Until that decision is available, do not
            // return a TTL=0 success while the authoritative state still says ACTIVE.
            return CompletableFuture.failedFuture(
                new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
          }
          long nextSequence = Math.addExact(binding.baseSequence(), 1);
          long deadline = LeaseTimeRules.deadlineEpochMillis(observed, current.grantedTtlSeconds());
          renewed = current.renew(deadline, nextSequence);
          ttl = renewed.grantedTtlSeconds();
        }
      }
      var response = Rpc.LeaseKeepAliveResponse.newBuilder()
          .setHeader(ResponseHeaderFactory.create(clusterId, memberId, binding.baseRevision(), 0))
          .setID(leaseId)
          .setTTL(ttl)
          .build();
      return preparation.prepare(binding, response, renewed, scope, account,
          () -> leases.finish(pinned, scope));
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(
        CompletionFailures.unwrapCompletion(failure)));
  }

  private CompletableFuture<Void> currentBase(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenAccept(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      if (!state.profileFingerprint().equals(profileFingerprint)) {
        throw new IllegalStateException("KeepAlive control profile differs from its table group");
      }
      if (!state.supportsAuxiliaryRecords()
          || state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.ownerLeaseExpired(ownerClock.millis()) || state.publication().isPresent()
          || state.publishedSequence() != binding.baseSequence()
          || state.publishedRevision() != binding.baseRevision()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    });
  }
}
