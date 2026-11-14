package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.internal.v1.ForwardMutationRequest;
import io.etcdhbase.grpc.internal.v1.ForwardMutationResponse;
import io.etcdhbase.grpc.internal.v1.InternalOwnerServiceGrpc;
import io.etcdhbase.server.config.ListenerAddress;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** One bounded mTLS listener and one no-retry channel per statically trusted owner peer. */
public final class OwnerForwardingRuntime implements OwnerForwarding.Directory,
    OwnerForwarding.AuthenticatedChannel, AutoCloseable {
  private static final int MAX_CONCURRENT_INTERNAL_REQUESTS = 1_024;
  private static final int MESSAGE_ALLOWANCE_BYTES = 16 * 1024;
  private static final int MEMORY_BUDGET_DENOMINATOR = 8;
  private static final int REQUEST_MEMORY_MULTIPLIER = 4;

  /** Per-process owner transport limits derived from the shared memory and request budgets. */
  public record RequestLimits(int maxMessageBytes, int maxConcurrentRequests, long reservedMemoryBytes) {
    public RequestLimits {
      if (maxMessageBytes < 1 || maxConcurrentRequests < 1 || reservedMemoryBytes < 1) {
        throw new IllegalArgumentException("invalid owner request limits");
      }
    }
  }

  public static RequestLimits requestLimits(long memoryBytes, int maxEtcdRequestBytes,
      int configuredConcurrentRequests) {
    if (memoryBytes < 1 || maxEtcdRequestBytes < 1
        || maxEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES
        || configuredConcurrentRequests < 1) {
      throw new IllegalArgumentException("invalid owner request budget inputs");
    }
    int maxMessageBytes = Math.addExact(maxEtcdRequestBytes, MESSAGE_ALLOWANCE_BYTES);
    long perRequestMemory = Math.multiplyExact((long) maxMessageBytes, REQUEST_MEMORY_MULTIPLIER);
    long transportMemoryLimit = memoryBytes / MEMORY_BUDGET_DENOMINATOR;
    long memoryRequestLimit = transportMemoryLimit / perRequestMemory;
    if (memoryRequestLimit < 1) {
      throw new IllegalArgumentException("M3 memory budget cannot admit one bounded owner request");
    }
    int concurrency = (int) Math.min(Math.min(configuredConcurrentRequests,
        MAX_CONCURRENT_INTERNAL_REQUESTS), memoryRequestLimit);
    long reservedMemoryBytes = Math.multiplyExact(perRequestMemory, concurrency);
    return new RequestLimits(maxMessageBytes, concurrency, reservedMemoryBytes);
  }

  private final Object lifecycle = new Object();
  private final String localInstanceId;
  private final M3RuntimeConfiguration.OwnerForwardingSettings settings;
  private final Map<String, M3RuntimeConfiguration.OwnerPeer> peersByInstance;
  private final Map<String, String> identitiesByLowercase;
  private final Map<String, InternalOwnerGrpcService.Handler> handlers = new ConcurrentHashMap<>();
  private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
  private final SslContext clientTls;
  private final int maxMessageBytes;
  private final int maxConcurrentRequests;
  private final int requestDeadlineMs;
  private final int shutdownTimeoutMs;
  private Server server;
  private ScheduledThreadPoolExecutor deadlines;
  private RequestScopeInterceptor requests;
  private boolean started;
  private boolean closing;

  public OwnerForwardingRuntime(String localInstanceId,
      M3RuntimeConfiguration.OwnerForwardingSettings settings, RequestLimits requestLimits,
      int requestDeadlineMs, int shutdownTimeoutMs) throws IOException {
    this.localInstanceId = Objects.requireNonNull(localInstanceId, "localInstanceId");
    this.settings = Objects.requireNonNull(settings, "settings");
    Objects.requireNonNull(requestLimits, "requestLimits");
    if (localInstanceId.isBlank()
        || requestDeadlineMs < 1 || shutdownTimeoutMs < 1) {
      throw new IllegalArgumentException("invalid owner forwarding runtime limits");
    }
    this.maxConcurrentRequests = requestLimits.maxConcurrentRequests();
    this.requestDeadlineMs = requestDeadlineMs;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
    maxMessageBytes = requestLimits.maxMessageBytes();
    var peers = new java.util.HashMap<String, M3RuntimeConfiguration.OwnerPeer>();
    var identities = new java.util.HashMap<String, String>();
    for (var peer : settings.peers()) {
      if (peers.putIfAbsent(peer.instanceId(), peer) != null
          || identities.putIfAbsent(peer.peerIdentity().toLowerCase(java.util.Locale.ROOT),
              peer.peerIdentity()) != null) {
        throw new IllegalArgumentException("owner peers must have unique instance and certificate identities");
      }
    }
    if (!peers.containsKey(localInstanceId)) {
      throw new IllegalArgumentException("local owner instance is absent from its trusted peer directory");
    }
    peersByInstance = Map.copyOf(peers);
    identitiesByLowercase = Map.copyOf(identities);
    clientTls = buildClientTls(settings);
  }

  void register(String tableGroupId, MutationOwnerLane lane) {
    Objects.requireNonNull(lane, "lane");
    register(tableGroupId, lane::receive);
  }

  void register(String tableGroupId, InternalOwnerGrpcService.Handler handler) {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(handler, "handler");
    synchronized (lifecycle) {
      if (started || closing) throw new IllegalStateException("owner forwarding registration is closed");
      if (tableGroupId.isBlank() || handlers.putIfAbsent(tableGroupId, handler) != null) {
        throw new IllegalArgumentException("owner forwarding table group is blank or already registered");
      }
    }
  }

  public void start() throws IOException {
    synchronized (lifecycle) {
      if (started || closing) throw new IllegalStateException("owner forwarding runtime cannot start");
      if (handlers.isEmpty()) throw new IllegalStateException("owner forwarding has no table-group receivers");
      deadlines = new ScheduledThreadPoolExecutor(1,
          Thread.ofPlatform().daemon().name("owner-forward-deadline-", 0).factory());
      deadlines.setRemoveOnCancelPolicy(true);
      deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      requests = new RequestScopeInterceptor(deadlines, maxConcurrentRequests,
          Duration.ofMillis(Math.min(requestDeadlineMs, 600_000)), maxMessageBytes, new RequestMetrics());
      try {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(
                ListenerAddress.parse(settings.bindHost()).connectionName(), settings.port()))
            .sslContext(buildServerTls(settings))
            .maxInboundMessageSize(maxMessageBytes)
            .maxConcurrentCallsPerConnection(maxConcurrentRequests)
            .intercept(new OwnerPeerIdentityInterceptor(
                settings.peers().stream().map(M3RuntimeConfiguration.OwnerPeer::peerIdentity).toList()))
            .intercept(requests)
            .addService(new InternalOwnerGrpcService(handlers))
            .build();
        server.start();
        started = true;
      } catch (IOException | RuntimeException | Error failure) {
        closeAfterFailedStart(failure);
        throw failure;
      }
    }
  }

  boolean isTrustedPeerIdentity(String identity) {
    return identity != null
        && identitiesByLowercase.containsKey(identity.toLowerCase(java.util.Locale.ROOT));
  }

  @Override
  public CompletableFuture<OwnerForwarding.Route> resolve(
      String tableGroupId, String ownerInstance, long ownerEpoch) {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(ownerInstance, "ownerInstance");
    synchronized (lifecycle) {
      if (!started || closing || !handlers.containsKey(tableGroupId)) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    }
    M3RuntimeConfiguration.OwnerPeer peer = peersByInstance.get(ownerInstance);
    if (peer == null || ownerEpoch <= 0) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return CompletableFuture.completedFuture(new OwnerForwarding.Route(
        ownerInstance, ownerEpoch, ownerInstance, peer.peerIdentity()));
  }

  @Override
  public CompletableFuture<OwnerForwarding.Reply> sendOnce(
      OwnerForwarding.Route route, MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(route, "route");
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    M3RuntimeConfiguration.OwnerPeer peer = peersByInstance.get(route.ownerInstance());
    if (peer == null || !route.endpointId().equals(peer.instanceId())
        || !route.peerIdentity().equals(peer.peerIdentity())
        || request.expectedOwnerEpoch() != route.ownerEpoch()
        || request.hopCount() != MutationForwardRequest.MAX_HOPS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    ManagedChannel channel;
    synchronized (lifecycle) {
      if (!started || closing) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      try {
        channel = channels.computeIfAbsent(peer.instanceId(), ignored -> buildChannel(peer));
      } catch (RuntimeException failure) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
      }
    }
    long remaining = Math.min(request.remainingTimeoutNanos(), scope.remainingDeadlineNanos());
    if (remaining <= 0) {
      return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(
          scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    return callOnce(channel, request, scope, remaining);
  }

  private CompletableFuture<OwnerForwarding.Reply> callOnce(
      ManagedChannel channel, MutationForwardRequest request, RequestScope scope, long remainingNanos) {
    var result = new CompletableFuture<OwnerForwarding.Reply>();
    var response = new AtomicReference<ForwardMutationResponse>();
    var clientCall = new AtomicReference<ClientCallStreamObserver<ForwardMutationRequest>>();
    var terminal = new AtomicBoolean();
    RequestScope.ScopedCleanup registration;
    try {
      registration = scope.registerScopedCleanup(() -> {
        ClientCallStreamObserver<ForwardMutationRequest> call = clientCall.get();
        if (call != null && !result.isDone()) call.cancel("owner forwarding scope ended", null);
      }, result.handle((reply, failure) -> (Void) null));
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    ClientResponseObserver<ForwardMutationRequest, ForwardMutationResponse> observer =
        new ClientResponseObserver<>() {
          @Override
          public void beforeStart(ClientCallStreamObserver<ForwardMutationRequest> call) {
            clientCall.set(call);
            if (scope.isClosed() || scope.isDeadlineExceeded()) {
              call.cancel("owner forwarding scope is no longer active", null);
            }
          }

          @Override
          public void onNext(ForwardMutationResponse value) {
            if (!response.compareAndSet(null, value)) {
              ClientCallStreamObserver<ForwardMutationRequest> call = clientCall.get();
              if (call != null) call.cancel("owner returned multiple replies", null);
              finish(registration, result, terminal, null,
                  new IllegalStateException("owner returned multiple replies"));
            }
          }

          @Override
          public void onError(Throwable failure) {
            finish(registration, result, terminal, null, failure);
          }

          @Override
          public void onCompleted() {
            ForwardMutationResponse value = response.get();
            if (value == null) {
              finish(registration, result, terminal, null,
                  new IllegalStateException("owner completed without a reply"));
            } else {
              finish(registration, result, terminal,
                  OwnerForwardingProtocolMapper.fromWire(value), null);
            }
          }
        };
    try {
      InternalOwnerServiceGrpc.newStub(channel).withDeadlineAfter(remainingNanos, TimeUnit.NANOSECONDS)
          .forwardMutation(OwnerForwardingProtocolMapper.toWire(request), observer);
    } catch (RuntimeException failure) {
      finish(registration, result, terminal, null, failure);
    }
    return result;
  }

  private static void finish(RequestScope.ScopedCleanup registration,
      CompletableFuture<OwnerForwarding.Reply> result, AtomicBoolean terminal,
      OwnerForwarding.Reply reply, Throwable failure) {
    if (!terminal.compareAndSet(false, true)) return;
    try {
      registration.close();
    } catch (Exception cleanupFailure) {
      if (failure == null) failure = cleanupFailure;
      else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
    }
    if (failure != null) result.completeExceptionally(failure);
    else result.complete(Objects.requireNonNull(reply, "reply"));
  }

  private ManagedChannel buildChannel(M3RuntimeConfiguration.OwnerPeer peer) {
    return NettyChannelBuilder.forAddress(
            ListenerAddress.parse(peer.host()).connectionName(), peer.port())
        .sslContext(clientTls)
        .overrideAuthority(peer.peerIdentity())
        .maxInboundMessageSize(maxMessageBytes)
        .disableRetry()
        .build();
  }

  private static SslContext buildClientTls(M3RuntimeConfiguration.OwnerForwardingSettings settings)
      throws IOException {
    try {
      return GrpcSslContexts.configure(SslContextBuilder.forClient()
          .keyManager(new File(settings.certificateFile()), new File(settings.privateKeyFile()))
          .trustManager(new File(settings.trustedCaFile()))).build();
    } catch (javax.net.ssl.SSLException failure) {
      throw new IOException("owner forwarding client TLS configuration is invalid", failure);
    }
  }

  private static SslContext buildServerTls(M3RuntimeConfiguration.OwnerForwardingSettings settings)
      throws IOException {
    try {
      return GrpcSslContexts.configure(SslContextBuilder.forServer(
              new File(settings.certificateFile()), new File(settings.privateKeyFile()))
          .trustManager(new File(settings.trustedCaFile()))
          .clientAuth(ClientAuth.REQUIRE)).build();
    } catch (javax.net.ssl.SSLException failure) {
      throw new IOException("owner forwarding server TLS configuration is invalid", failure);
    }
  }

  private void closeAfterFailedStart(Throwable original) {
    if (server != null) {
      try {
        server.shutdownNow();
      } catch (RuntimeException | Error cleanup) {
        if (cleanup != original) original.addSuppressed(cleanup);
      }
      server = null;
    }
    if (requests != null) {
      try {
        requests.close();
      } catch (RuntimeException | Error cleanup) {
        if (cleanup != original) original.addSuppressed(cleanup);
      }
      requests = null;
    }
    if (deadlines != null) {
      deadlines.shutdownNow();
      deadlines = null;
    }
  }

  @Override
  public void close() {
    List<Throwable> failures = new ArrayList<>();
    Server activeServer;
    RequestScopeInterceptor activeRequests;
    ScheduledThreadPoolExecutor activeDeadlines;
    synchronized (lifecycle) {
      if (closing) return;
      closing = true;
      activeServer = server;
      activeRequests = requests;
      activeDeadlines = deadlines;
    }
    if (activeServer != null) {
      activeServer.shutdown();
      try {
        if (!activeServer.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
          activeServer.shutdownNow();
          if (!activeServer.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
            failures.add(new IllegalStateException("owner forwarding server did not terminate"));
          }
        }
      } catch (InterruptedException interrupted) {
        activeServer.shutdownNow();
        Thread.currentThread().interrupt();
        failures.add(interrupted);
      }
    }
    if (activeRequests != null) {
      try {
        activeRequests.close();
      } catch (RuntimeException | Error failure) {
        failures.add(failure);
      }
    }
    if (activeDeadlines != null) activeDeadlines.shutdownNow();
    for (ManagedChannel channel : channels.values()) {
      channel.shutdown();
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMs);
    for (ManagedChannel channel : channels.values()) {
      long remaining = Math.max(0, deadline - System.nanoTime());
      try {
        if (!channel.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
          channel.shutdownNow();
          remaining = Math.max(0, deadline - System.nanoTime());
          if (!channel.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
            failures.add(new IllegalStateException("owner forwarding channel did not terminate"));
          }
        }
      } catch (InterruptedException interrupted) {
        channel.shutdownNow();
        Thread.currentThread().interrupt();
        failures.add(interrupted);
        break;
      }
    }
    channels.clear();
    handlers.clear();
    if (!failures.isEmpty()) {
      IllegalStateException failure = new IllegalStateException("owner forwarding runtime cleanup failed");
      failures.forEach(failure::addSuppressed);
      throw failure;
    }
  }
}
