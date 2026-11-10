package io.etcdhbase.server;

import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/** A disposable real gRPC server process for the local owner-restart history. */
public final class M3HistoryServerMain {
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);

  private M3HistoryServerMain() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 7 || !Set.of("before-dispatch", "candidate-preparation", "result-preparation",
        "sealed-before-publish", "cas-ack-lost", "partial-materialization", "old-owner-resume", "cancel-after-cas", "after-commit")
        .contains(arguments[6])) {
      throw new IllegalArgumentException("invalid history server fixture arguments");
    }
    var configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    try (var connection = ConnectionFactory.createAsyncConnection(configuration).get(20, TimeUnit.SECONDS)) {
      connection.getAdmin().listNamespaces().get(20, TimeUnit.SECONDS);
    }
    var plan = M3TableGroupRuntimeIT.plan(arguments[1], Integer.parseInt(arguments[2]), arguments[3]);
    var auth = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), auth,
            configuration, HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-history-child", new char[0])) {
      var identity = provider.authenticate(credentials, auth).toCompletableFuture().get(30, TimeUnit.SECONDS);
      var limits = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
      var budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
      Path ready = Path.of(arguments[5]);
      var armed = new AtomicBoolean();
      HBaseConnectionFactory connections = arguments[6].equals("cas-ack-lost")
          || arguments[6].equals("partial-materialization") || arguments[6].equals("old-owner-resume")
          || arguments[6].equals("cancel-after-cas")
          || arguments[6].equals("candidate-preparation") || arguments[6].equals("result-preparation")
          || arguments[6].equals("sealed-before-publish")
          ? new M3HistoryFaultFactory(provider, plan, armed, ready.resolveSibling("rpc-held"), arguments[6]) : provider;
      try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
        M3TableGroupRuntime runtime;
        try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
          runtime = M3TableGroupRuntime.open(plan, M3TableGroupRuntimeIT.activation(arguments[4]),
              RESOURCES, connections, identity, budgets, scans, startup);
        }
        try {
          var deadlines = new ScheduledThreadPoolExecutor(1);
          try (var requests = new RequestScopeInterceptor(deadlines, RESOURCES.maxConcurrentRequests(),
              Duration.ofMillis(RESOURCES.requestDeadlineMs()), RESOURCES.maxRequestBytes(), new RequestMetrics())) {
            var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .maxInboundMessageSize(RESOURCES.maxRpcBytes()).intercept(requests)
                .intercept(new HoldRpc(ready.resolveSibling("rpc-held"), arguments[6], armed))
                .addService(runtime.kvService()).build().start();
            try {
              Path pending = ready.resolveSibling(ready.getFileName() + ".tmp");
              Files.writeString(pending, "127.0.0.1:" + server.getPort());
              Files.move(pending, ready, StandardCopyOption.ATOMIC_MOVE);
              if ((arguments[6].equals("old-owner-resume") || arguments[6].equals("cancel-after-cas"))
                  && arguments[4].equals("1")) arrangeDrain(runtime, budgets, requests, ready);
              if (arguments[6].equals("cancel-after-cas") && arguments[4].equals("1")) {
                arrangeBudgetProbe(budgets, requests, ready);
              }
              new CountDownLatch(1).await();
            } finally {
              server.shutdownNow();
              server.awaitTermination(10, TimeUnit.SECONDS);
            }
          } finally {
            deadlines.shutdownNow();
          }
        } finally {
          runtime.close();
          runtime.drained().get(30, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static void arrangeDrain(M3TableGroupRuntime runtime, BudgetManager budgets,
      RequestScopeInterceptor requests, Path ready) {
    Thread.ofVirtual().name("m3-old-owner-drain").start(() -> {
      long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      try {
        while (!Files.exists(ready.resolveSibling("shutdown-old"))) {
          if (System.nanoTime() >= deadline) throw new IllegalStateException("old owner drain timed out");
          Thread.sleep(25);
        }
        runtime.close();
        runtime.drained().get(30, TimeUnit.SECONDS);
        while (requests.activeRequests() != 0 || budgets.snapshot().allocatedBytes() != 0
            || budgets.snapshot().reservedBytes() != 0) {
          if (System.nanoTime() >= deadline) throw new IllegalStateException("old owner resources did not drain");
          Thread.sleep(25);
        }
        Files.writeString(ready.resolveSibling("old-drained"), "drained");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } catch (Exception failure) {
        failure.printStackTrace(System.err);
      }
    });
  }

  private static void arrangeBudgetProbe(BudgetManager budgets, RequestScopeInterceptor requests, Path ready) {
    Thread.ofVirtual().name("m3-cancellation-budget").start(() -> {
      long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      try {
        awaitMarker(ready.resolveSibling("measure-baseline"), deadline);
        while (requests.activeRequests() != 0) {
          if (System.nanoTime() >= deadline) throw new IllegalStateException("baseline requests did not drain");
          Thread.sleep(25);
        }
        writeBudget(ready.resolveSibling("baseline-budget"), budgets, requests);
        awaitMarker(ready.resolveSibling("measure-held"), deadline);
        writeBudget(ready.resolveSibling("held-budget"), budgets, requests);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } catch (Exception failure) {
        failure.printStackTrace(System.err);
      }
    });
  }

  private static void awaitMarker(Path marker, long deadline) throws Exception {
    while (!Files.exists(marker)) {
      if (System.nanoTime() >= deadline) throw new IllegalStateException("fixture marker timed out");
      Thread.sleep(25);
    }
  }

  private static void writeBudget(Path path, BudgetManager budgets, RequestScopeInterceptor requests) throws Exception {
    var snapshot = budgets.snapshot();
    Path pending = path.resolveSibling(path.getFileName() + ".tmp");
    Files.writeString(pending, snapshot.allocatedBytes() + "," + snapshot.reservedBytes() + "," + requests.activeRequests());
    Files.move(pending, path, StandardCopyOption.ATOMIC_MOVE);
  }

  /** Test-only barriers at dispatch or after a committed Txn result, before it is sent. */
  private record HoldRpc(Path marker, String mode, AtomicBoolean armed) implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
        Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      var target = new AtomicBoolean();
      var guarded = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
        @Override public void sendMessage(RespT message) {
          if (target.get() && mode.equals("after-commit") && message instanceof Rpc.TxnResponse && !hold()) return;
          super.sendMessage(message);
        }
      };
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(guarded, headers)) {
        @Override public void onMessage(ReqT message) {
          if (message instanceof Rpc.TxnRequest txn && txn.getSuccessList().stream()
              .anyMatch(op -> op.hasRequestPut() && op.getRequestPut().getKey().toStringUtf8().endsWith("/lost-response"))) {
            target.set(true);
            armed.set(true);
            if (mode.equals("before-dispatch") && !hold()) return;
          }
          super.onMessage(message);
        }
      };
    }

    private boolean hold() {
      try {
        Files.writeString(marker, mode);
        new CountDownLatch(1).await();
        return true;
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return false;
      } catch (java.io.IOException failure) {
        throw new IllegalStateException("could not create RPC barrier", failure);
      }
    }
  }
}
