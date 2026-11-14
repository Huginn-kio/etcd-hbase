package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseEnumerationReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.LeaseGrantResponseMapper;
import io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapper;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.hbase.codec.LeaseGrantResultMessages;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Adapts the implemented Lease unary methods to one table group's bounded runtime. */
public final class LeaseGateway {
  private static final int MAX_EXAMINED_LEASES = 100_000;
  private static final long MAX_METADATA_BYTES = 64L << 20;
  private static final long OUTPUT_OBJECT_ALLOWANCE = 256;
  private static final java.util.concurrent.Executor CLEANUP =
      task -> Thread.ofVirtual().name("m4-keepalive-cleanup").start(task);
  static final int BINDING_PAGE_ENTRIES = 128;
  private static final long BINDING_PAGE_OVERHEAD_BYTES = 64L << 10;

  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorizationDigest;
  private final long clusterId;
  private final long memberId;
  private final int maximumEtcdRequestBytes;
  private final CommitControlStore control;
  private final MutationRelay relay;
  private final InvocationResultResolver results;
  private final ReadViewService views;
  private final LeaseReadView leaseReads;
  private final LeaseEnumerationReader leaseEnumeration;
  private final LeaseBindingReader leaseBindings;
  private final LeaseTimeSource time;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final BudgetManager.Reservation bindingReadReservation;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final RequestMetrics metrics;

  public LeaseGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes, CommitControlStore control,
      MutationRelay relay, InvocationResultResolver results, ReadViewService views,
      LeaseReadView leaseReads, LeaseEnumerationReader leaseEnumeration,
      LeaseBindingReader leaseBindings, LeaseTimeSource time,
      BudgetManager budgets, BudgetManager.Reservation reservation, Executor worker,
      ScheduledExecutorService timers, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.control = Objects.requireNonNull(control, "control");
    this.relay = Objects.requireNonNull(relay, "relay");
    this.results = Objects.requireNonNull(results, "results");
    this.views = Objects.requireNonNull(views, "views");
    this.leaseReads = Objects.requireNonNull(leaseReads, "leaseReads");
    this.leaseEnumeration = Objects.requireNonNull(leaseEnumeration, "leaseEnumeration");
    this.leaseBindings = Objects.requireNonNull(leaseBindings, "leaseBindings");
    this.time = Objects.requireNonNull(time, "time");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    if (tableGroup.isBlank() || authorizationDigest.length() != 32 || clusterId == 0 || memberId == 0
        || maximumEtcdRequestBytes < 1
        || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      throw new IllegalArgumentException("invalid Lease gateway identity or request limit");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumEtcdRequestBytes = maximumEtcdRequestBytes;
    this.bindingReadReservation = bindingReadReservation(reservation, maximumEtcdRequestBytes);
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  static long bindingPageBytes(int maximumEtcdRequestBytes) {
    if (maximumEtcdRequestBytes < 1 || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid binding request limit");
    }
    return Math.addExact(maximumEtcdRequestBytes, BINDING_PAGE_OVERHEAD_BYTES);
  }

  private static BudgetManager.Reservation bindingReadReservation(
      BudgetManager.Reservation base, int maximumEtcdRequestBytes) {
    long pageBytes = LeaseBindingReader.requiredPageMemoryBytes(
        BINDING_PAGE_ENTRIES, bindingPageBytes(maximumEtcdRequestBytes));
    return new BudgetManager.Reservation(base.heads(), Math.max(base.page(), pageBytes),
        base.io(), base.output(), base.candidates());
  }

  public BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grantHandler() {
    return this::grant;
  }

  public BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>>
      timeToLiveHandler() {
    return this::timeToLive;
  }

  public BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> leasesHandler() {
    return this::leases;
  }

  public Function<StreamObserver<Rpc.LeaseKeepAliveResponse>,
      StreamObserver<Rpc.LeaseKeepAliveRequest>> keepAliveHandler() {
    return observer -> {
      RequestScope streamScope = RequestScopeInterceptor.currentScope();
      return new LeaseKeepAliveStream(observer, streamScope, worker, timers,
          request -> keepAlive(request, streamScope), metrics);
    };
  }

  private void grant(Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, reservation);
      retainInput(scope, account, request.getSerializedSize());
      if (request.getSerializedSize() > maximumEtcdRequestBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      worker.execute(() -> beginGrant(request, observer, scope, account, terminal, drained));
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<LeaseKeepAliveStream.Delivery> keepAlive(
      Rpc.LeaseKeepAliveRequest request, RequestScope streamScope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(streamScope, "streamScope");
    RequestScope operationScope = streamScope.childWithTimeoutNanos(
        MutationForwardRequest.MAX_TIMEOUT_NANOS);
    CompletableFuture<Void> deliveryComplete = new CompletableFuture<>();
    operationScope.trackPhysicalProducer(deliveryComplete);
    CompletableFuture<Void> operationDrained = CompletableFuture.allOf(
        operationScope.cleanupCompletion().toCompletableFuture(),
        operationScope.physicalProducersCompletion().toCompletableFuture());
    var parentRegistration = streamScope.registerScopedCleanup(
        () -> {
          if (streamScope.isCancelled()) operationScope.cancelAsync(CLEANUP);
          else operationScope.closeAsync(CLEANUP);
        }, operationDrained);
    var finished = new AtomicBoolean();
    Runnable finish = () -> {
      if (!finished.compareAndSet(false, true)) return;
      deliveryComplete.complete(null);
      operationScope.closeAsync(CLEANUP);
      try {
        parentRegistration.close();
      } catch (Exception failure) {
        // The child scope owns cleanup; the interceptor retains its drain registration.
      }
    };

    CompletableFuture<LeaseKeepAliveStream.Delivery> result = new CompletableFuture<>();
    BudgetManager.Account account = null;
    try {
      account = budgets.admit(tableGroup, operationScope, reservation);
      retainInput(operationScope, account, request.getSerializedSize());
      if (request.getSerializedSize() > maximumEtcdRequestBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      metrics.recordLeaseRenewalRequest(request.getSerializedSize());
    } catch (BackendFailure | RuntimeException failure) {
      if (account != null) account.close();
      finish.run();
      result.completeExceptionally(failure);
      return result;
    } catch (Error fatal) {
      if (account != null) account.close();
      finish.run();
      throw fatal;
    }

    BudgetManager.Account admitted = account;
    try {
      worker.execute(() -> beginKeepAlive(request, operationScope, admitted, result, finish));
    } catch (RuntimeException rejected) {
      admitted.close();
      finish.run();
      result.completeExceptionally(
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    } catch (Error fatal) {
      admitted.close();
      finish.run();
      throw fatal;
    }
    return result;
  }

  private void beginKeepAlive(Rpc.LeaseKeepAliveRequest request, RequestScope scope,
      BudgetManager.Account account, CompletableFuture<LeaseKeepAliveStream.Delivery> result,
      Runnable finish) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(request.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    try {
      control.read(scope).thenCompose(snapshot -> {
        var state = snapshot.orElseThrow(() ->
            new IllegalStateException("authoritative commit control state is missing")).state();
        long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
        if (state.ownerInstance().isEmpty() || remaining <= 0) {
          return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
        }
        var forwarded = new MutationForwardRequest(tableGroup, invocation, digest, authorizationDigest,
            ServiceAuthorizationContextReference.create(identity, authorizationDigest), state.ownerEpoch(),
            remaining, 0, operation);
        return relay.forward(forwarded, scope);
      }).thenCompose(reply -> resolveKeepAliveReply(reply, invocation, digest, scope, account))
          .whenComplete((resolved, failure) -> {
            if (!scope.startIfOpen(() -> {})) {
              if (resolved != null) resolved.close();
              account.close();
              finish.run();
              result.completeExceptionally(new BackendFailure(scope.isDeadlineExceeded()
                  ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
              return;
            }
            if (failure != null) {
              account.close();
              finish.run();
              result.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
              return;
            }
            projectKeepAliveResponse(request, resolved, account, result, finish);
          });
    } catch (RuntimeException | Error failure) {
      account.close();
      finish.run();
      result.completeExceptionally(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedInvocation> resolveKeepAliveReply(
      MutationRelay.Result reply, InvocationId invocation, ByteKey digest, RequestScope scope,
      BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        metrics.recordLeaseRenewalUnknownOutcome();
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveKeepAliveOriginal(invocation, digest, scope, account, false);
    }
    if (reply instanceof MutationRelay.LeaseRejection rejection) {
      return CompletableFuture.failedFuture(new LeaseFailure(rejection.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return resolveKeepAliveOriginal(invocation, digest, scope, account, true).exceptionallyCompose(failure ->
        CompletableFuture.failedFuture(new BackendFailure(unknown.category(),
            CompletionFailures.unwrapCompletion(failure))));
  }

  private CompletableFuture<InvocationResultResolver.ResolvedInvocation> resolveKeepAliveOriginal(
      InvocationId invocation, ByteKey digest, RequestScope scope,
      BudgetManager.Account account, boolean outcomeUnknown) {
    if (outcomeUnknown) metrics.recordLeaseRenewalUnknownOutcome();
    long started = System.nanoTime();
    return results.resolveInvocation(invocation, digest, authorizationDigest, scope, account)
        .whenComplete((resolved, failure) -> {
      metrics.recordLeaseRenewalStage(RequestMetrics.LeaseRenewalStage.RESULT_RESOLUTION,
          Math.max(0, System.nanoTime() - started));
      if (failure != null && !outcomeUnknown) metrics.recordLeaseRenewalUnknownOutcome();
    });
  }

  private void projectKeepAliveResponse(Rpc.LeaseKeepAliveRequest request,
      InvocationResultResolver.ResolvedInvocation resolved, BudgetManager.Account account,
      CompletableFuture<LeaseKeepAliveStream.Delivery> result, Runnable finish) {
    try (resolved) {
      var buffer = resolved.payload().buffer();
      var bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      Rpc.LeaseKeepAliveResponse response = Rpc.LeaseKeepAliveResponse.parseFrom(bytes);
      if (!response.hasHeader() || response.getID() != request.getID() || response.getTTL() < 0
          || response.getTTL() > LeaseTimeRules.MAX_TTL_SECONDS
          || response.getHeader().getClusterId() != clusterId
          || response.getHeader().getMemberId() == 0
          || response.getHeader().getRevision() != resolved.revision()
          || resolved.containsKeyChanges()) {
        throw new BackendFailure(BackendFailure.Category.UNKNOWN);
      }
      // etcd fills this header at the serving RPC endpoint before renewing through its lessor.
      // The durable owner result supplies the revision, while a forwarded response must identify
      // the member that accepted this public stream.
      response = response.toBuilder().setHeader(response.getHeader().toBuilder()
          .setMemberId(memberId)).build();
      int responseBytes = response.getSerializedSize();
      BudgetManager.Lease output = account.tryRetain(BudgetManager.Memory.OUTPUT, responseBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      var released = new AtomicBoolean();
      Runnable release = () -> {
        if (!released.compareAndSet(false, true)) return;
        output.close();
        account.close();
        finish.run();
      };
      if (!result.complete(new LeaseKeepAliveStream.Delivery(response, release))) release.run();
    } catch (BackendFailure | InvalidProtocolBufferException | RuntimeException failure) {
      account.close();
      finish.run();
      result.completeExceptionally(failure);
    } catch (Error fatal) {
      account.close();
      finish.run();
      throw fatal;
    }
  }

  private void beginGrant(Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseGrant(request.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    try {
      control.read(scope).thenCompose(snapshot -> {
        var state = snapshot.orElseThrow(() ->
            new IllegalStateException("authoritative commit control state is missing")).state();
        long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
        if (state.ownerInstance().isEmpty() || remaining <= 0) {
          return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
        }
        var forwarded = new MutationForwardRequest(tableGroup, invocation, digest, authorizationDigest,
            ServiceAuthorizationContextReference.create(identity, authorizationDigest), state.ownerEpoch(),
            remaining, 0, operation);
        return relay.forward(forwarded, scope);
      }).thenCompose(reply -> resolveGrantReply(reply, request, invocation, digest, scope, account))
          .whenComplete((resolved, failure) -> {
            if (!scope.startIfOpen(() -> {})) {
              if (resolved != null) resolved.close();
              gatewayDrained.complete(null);
              return;
            }
            dispatchGrantResponse(request, observer, scope, account, terminal, gatewayDrained,
                resolved, failure);
          });
    } catch (RuntimeException | Error failure) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveGrantReply(
      MutationRelay.Result reply, Rpc.LeaseGrantRequest request, InvocationId invocation,
      ByteKey digest, RequestScope scope, BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveOriginal(invocation, digest, scope, account);
    }
    if (reply instanceof MutationRelay.LeaseRejection rejection) {
      return CompletableFuture.failedFuture(new LeaseFailure(rejection.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return resolveOriginal(invocation, digest, scope, account).exceptionallyCompose(failure -> {
      Throwable cause = CompletionFailures.unwrapCompletion(failure);
      return CompletableFuture.failedFuture(new BackendFailure(unknown.category(), cause));
    });
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveOriginal(
      InvocationId invocation, ByteKey digest, RequestScope scope, BudgetManager.Account account) {
    return results.resolve(invocation, digest, authorizationDigest, scope, account);
  }

  private void dispatchGrantResponse(Rpc.LeaseGrantRequest request,
      StreamObserver<Rpc.LeaseGrantResponse> observer, RequestScope scope, BudgetManager.Account account,
      AtomicBoolean terminal, CompletableFuture<Void> gatewayDrained,
      InvocationResultResolver.ResolvedResult resolved, Throwable failure) {
    try {
      worker.execute(() -> {
        try {
          if (failure != null) {
            signalError(scope, observer, terminal, CompletionFailures.unwrapCompletion(failure));
            return;
          }
          if (resolved == null || scope.isClosed() || scope.isDeadlineExceeded()) {
            if (resolved != null) resolved.close();
            return;
          }
          var buffer = resolved.payload().buffer();
          var bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          var result = LeaseGrantResultMessages.decode(ByteKey.copyOf(bytes));
          var response = LeaseGrantResponseMapper.toWire(request, result, resolved, clusterId, memberId);
          send(response, response.getSerializedSize(), scope, account, observer, terminal);
        } catch (RuntimeException | Error projectionFailure) {
          signalError(scope, observer, terminal, projectionFailure);
          if (projectionFailure instanceof Error fatal) throw fatal;
        } finally {
          if (resolved != null) resolved.close();
          gatewayDrained.complete(null);
        }
      });
    } catch (RuntimeException rejected) {
      if (resolved != null) resolved.close();
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal,
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
  }

  private void timeToLive(Rpc.LeaseTimeToLiveRequest request,
      StreamObserver<Rpc.LeaseTimeToLiveResponse> observer) {
    Objects.requireNonNull(request, "request");
    BudgetManager.Reservation readReservation = request.getKeys() ? bindingReadReservation : reservation;
    startRead(observer, readReservation, (scope, account) -> readTimeToLive(request, scope, account),
        Rpc.LeaseTimeToLiveResponse::getSerializedSize);
  }

  private void leases(Rpc.LeaseLeasesRequest request,
      StreamObserver<Rpc.LeaseLeasesResponse> observer) {
    Objects.requireNonNull(request, "request");
    startRead(observer, reservation, this::readLeases, Rpc.LeaseLeasesResponse::getSerializedSize);
  }

  private <T> void startRead(StreamObserver<T> observer, BudgetManager.Reservation memory,
      BiFunction<RequestScope, BudgetManager.Account, CompletableFuture<T>> operation,
      java.util.function.ToIntFunction<T> responseSize) {
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, memory);
      worker.execute(() -> {
        CompletableFuture<T> result;
        try {
          result = Objects.requireNonNull(operation.apply(scope, account), "Lease read future");
        } catch (RuntimeException | Error failure) {
          drained.complete(null);
          signalError(scope, observer, terminal, failure);
          if (failure instanceof Error fatal) throw fatal;
          return;
        }
        result.whenComplete((response, failure) -> {
          try {
            worker.execute(() -> {
              try {
                if (failure != null) {
                  signalError(scope, observer, terminal, CompletionFailures.unwrapCompletion(failure));
                } else if (response != null) {
                  send(response, responseSize.applyAsInt(response), scope, account, observer, terminal);
                }
              } finally {
                drained.complete(null);
              }
            });
          } catch (RuntimeException rejected) {
            drained.complete(null);
            signalError(scope, observer, terminal,
                new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
          }
        });
      });
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<Rpc.LeaseTimeToLiveResponse> readTimeToLive(
      Rpc.LeaseTimeToLiveRequest request, RequestScope scope, BudgetManager.Account account) {
    return readTimeToLive(request, scope, account, true);
  }

  private CompletableFuture<Rpc.LeaseTimeToLiveResponse> readTimeToLive(
      Rpc.LeaseTimeToLiveRequest request, RequestScope scope, BudgetManager.Account account,
      boolean resolveExpiredEmpty) {
    if (request.getID() == 0) {
      return views.openAuxiliary(authorizationDigest, scope).thenCompose(pin ->
          leaseReads.finish(pin, scope).thenApply(ignored ->
              projectTimeToLive(request, Optional.empty(), pin, time.observe(), List.of())));
    }
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, 64, MAX_METADATA_BYTES);
    return views.openAuxiliary(authorizationDigest, scope).thenCompose(pin ->
        leaseReads.find(pin, request.getID(), scope, proof)
            .thenCompose(found -> {
              var clock = time.observe();
              if (found.isPresent() && found.orElseThrow().status() == LeaseState.Status.ACTIVE
                  && LeaseTimeRules.isExpired(found.orElseThrow(), clock)) {
                LeaseState expired = found.orElseThrow();
                if (expired.boundKeyCount() != 0 || expired.boundKeyBytes() != 0
                    || expired.revokeCostBytes() != 0) {
                  return CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.UNAVAILABLE));
                }
                if (resolveExpiredEmpty) {
                  return expireEmptyLease(expired, scope, account)
                      .thenCompose(ignored -> readTimeToLive(request, scope, account, false));
                }
              }
              if (!request.getKeys() || found.isEmpty()
                  || found.orElseThrow().status() != LeaseState.Status.ACTIVE) {
                return CompletableFuture.completedFuture(
                    projectTimeToLive(request, found, pin, clock, List.of()));
              }
              return leaseBindings.read(pin, found.orElseThrow(), scope, account, proof)
                  .thenApply(keys -> projectTimeToLive(request, found, pin, clock, keys));
            })
            .thenCompose(response -> leaseReads.finish(pin, scope).thenApply(ignored -> response)));
  }

  private CompletableFuture<Void> expireEmptyLease(LeaseState state, RequestScope scope,
      BudgetManager.Account account) {
    var candidate = new ExpiryCandidateIdentity(state.deadlineEpochMillis(),
        state.identity().leaseId(), state.identity().incarnation(), state.renewalGeneration());
    ByteKey operation = MutationOperationEnvelope.leaseExpireEmpty(candidate.encode().bytes());
    InvocationId invocation = InvocationId.create();
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    return control.read(scope).thenCompose(snapshot -> {
      var controlState = snapshot.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
      if (controlState.ownerInstance().isEmpty() || remaining <= 0) {
        return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
      }
      var forwarded = new MutationForwardRequest(tableGroup, invocation, digest, authorizationDigest,
          ServiceAuthorizationContextReference.create(identity, authorizationDigest),
          controlState.ownerEpoch(), remaining, 0, operation);
      return relay.forward(forwarded, scope).thenCompose(reply ->
          resolveExpiryReply(reply, invocation, digest, scope, account));
    });
  }

  private CompletableFuture<Void> resolveExpiryReply(MutationRelay.Result reply,
      InvocationId invocation, ByteKey digest, RequestScope scope, BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return closeResolved(results.resolve(invocation, digest, authorizationDigest, scope, account));
    }
    if (reply instanceof MutationRelay.LeaseRejection) {
      return CompletableFuture.completedFuture(null);
    }
    if (reply instanceof MutationRelay.KnownRejection rejected) {
      return CompletableFuture.failedFuture(new BackendFailure(rejected.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return closeResolved(results.resolve(invocation, digest, authorizationDigest, scope, account))
        .exceptionallyCompose(failure -> CompletableFuture.failedFuture(new BackendFailure(
            unknown.category(), CompletionFailures.unwrapCompletion(failure))));
  }

  private static CompletableFuture<Void> closeResolved(
      CompletableFuture<InvocationResultResolver.ResolvedResult> future) {
    return future.thenAccept(InvocationResultResolver.ResolvedResult::close);
  }

  private Rpc.LeaseTimeToLiveResponse projectTimeToLive(Rpc.LeaseTimeToLiveRequest request,
      Optional<LeaseState> found, PinnedReadView pin,
      io.etcdhbase.core.lease.LeaseTimeObservation clock, List<ByteKey> keys) {
    return LeaseTimeToLiveResponseMapper.toWire(request, found, pin.view().targetRevision(),
        clock, keys, clusterId, memberId);
  }

  private CompletableFuture<Rpc.LeaseLeasesResponse> readLeases(
      RequestScope scope, BudgetManager.Account account) {
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, MAX_EXAMINED_LEASES, MAX_METADATA_BYTES);
    return views.openAuxiliary(authorizationDigest, scope).thenCompose(pin ->
        leaseEnumeration.read(pin, scope, account, proof).thenCompose(states -> {
          var clock = time.observe();
          List<Rpc.LeaseStatus> leases = states.stream().filter(state -> state.status() == LeaseState.Status.ACTIVE)
              .filter(state -> !LeaseTimeRules.isExpired(state, clock))
              .map(state -> Rpc.LeaseStatus.newBuilder()
                  .setID(state.identity().leaseId())
                  .build())
              .sorted((left, right) -> Long.compareUnsigned(left.getID(), right.getID())).toList();
          var response = Rpc.LeaseLeasesResponse.newBuilder()
              .setHeader(ResponseHeaderFactory.create(clusterId, memberId, pin.view().targetRevision(), 0))
              .addAllLeases(leases).build();
          return leaseReads.finish(pin, scope).thenApply(ignored -> response);
        }));
  }

  private <T> void send(T response, int serializedBytes, RequestScope scope,
      BudgetManager.Account account, StreamObserver<T> observer, AtomicBoolean terminal) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) return;
    BudgetManager.Lease output = null;
    try {
      long estimated = Math.addExact(Math.multiplyExact(2L, serializedBytes), OUTPUT_OBJECT_ALLOWANCE);
      output = account.tryRetain(BudgetManager.Memory.OUTPUT, estimated)
          .orElseThrow(LeaseGateway::resourceLimit);
      if (serializedBytes > maximumEtcdRequestBytes) {
        throw resourceLimit();
      }
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      observer.onNext(response);
      if (!scope.isClosed() && !scope.isDeadlineExceeded() && terminal.compareAndSet(false, true)) {
        observer.onCompleted();
      }
    } catch (BackendFailure | RuntimeException failure) {
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    } finally {
      if (output != null) output.close();
    }
  }

  private static void retainInput(RequestScope scope, BudgetManager.Account account, int serializedBytes)
      throws BackendFailure {
    long bytes = Math.addExact(1024L, Math.multiplyExact(4L, serializedBytes));
    PutCommitPreparation.retainUntilDrain(scope, account.tryRetain(BudgetManager.Memory.CANDIDATE, bytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
  }

  private static CompletionException resourceLimit() {
    return new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
  }

  private static void signalError(RequestScope scope, StreamObserver<?> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (!terminal.compareAndSet(false, true) || scope.isClosed()) return;
    try {
      observer.onError(ErrorProjection.project(failure));
    } catch (RuntimeException | Error terminationFailure) {
      if (failure != terminationFailure) failure.addSuppressed(terminationFailure);
      if (terminationFailure instanceof Error fatal) throw fatal;
    }
  }
}
