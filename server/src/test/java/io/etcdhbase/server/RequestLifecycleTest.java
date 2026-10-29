package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RequestLifecycleTest {
  @Test
  void streamingRequestByteBudgetRejectsTheSecondMessageBeforeTheHandlerSeesIt() throws Exception {
    try (var fixture = new Fixture(Duration.ofSeconds(5), RequestCompletionRecorder.NOOP)) {
      var call =
          fixture.channel.newCall(etcdserverpb.WatchGrpc.getWatchMethod(), CallOptions.DEFAULT);
      var terminal = new CountDownLatch(1);
      var status = new AtomicReference<Status>();
      call.start(
          new ClientCall.Listener<>() {
            @Override
            public void onClose(Status result, Metadata trailers) {
              status.set(result);
              terminal.countDown();
            }
          },
          new Metadata());
      call.request(1);
      var message =
          Rpc.WatchRequest.newBuilder()
              .setCreateRequest(
                  Rpc.WatchCreateRequest.newBuilder()
                      .setKey(com.google.protobuf.ByteString.copyFrom(new byte[600])))
              .build();
      call.sendMessage(message);
      assertTrue(fixture.firstMessage.await(2, TimeUnit.SECONDS));
      call.sendMessage(message);
      assertTrue(terminal.await(3, TimeUnit.SECONDS));
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, status.get().getCode());
      assertTrue(fixture.cleaned.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
      assertEquals(1, fixture.receivedMessages.get());
      assertEquals(0, fixture.interceptor.activeRequests());
    }
  }

  @Test
  void clientCancellationClosesResourcesAndReleasesAdmission() throws Exception {
    try (var fixture = new Fixture(Duration.ofSeconds(5), RequestCompletionRecorder.NOOP)) {
      var call = fixture.call();
      assertTrue(fixture.started.await(2, TimeUnit.SECONDS));
      assertEquals(1, fixture.interceptor.activeRequests());
      var failure =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  KVGrpc.newBlockingStub(fixture.channel)
                      .withDeadlineAfter(2, TimeUnit.SECONDS)
                      .range(Rpc.RangeRequest.getDefaultInstance()));
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, failure.getStatus().getCode());
      call.cancel("private-client-reason", null);
      assertTrue(fixture.cleaned.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.scope.get().isCancelled());
      assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
      assertEquals(0, fixture.interceptor.activeRequests());
      assertFalse(fixture.shared.isCancelled());
    }
  }

  @Test
  void serverDeadlineTerminatesAClientWithoutItsOwnDeadline() throws Exception {
    try (var fixture = new Fixture(Duration.ofSeconds(1), RequestCompletionRecorder.NOOP)) {
      fixture.call();
      assertTrue(fixture.started.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.terminal.await(3, TimeUnit.SECONDS));
      assertEquals(Status.Code.DEADLINE_EXCEEDED, fixture.status.get().getCode());
      assertTrue(fixture.cleaned.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
      assertEquals(0, fixture.interceptor.activeRequests());
      assertTrue(fixture.scope.get().isCancelled());
    }
  }

  @Test
  void observerFailureCannotMaskHandlerFailureOrLeakAdmission() throws Exception {
    try (var fixture =
        new Fixture(
            Duration.ofSeconds(5),
            (method, outcome, trace, scope, elapsed) -> {
              throw new IllegalStateException("private-observation-value");
            })) {
      fixture.throwInHandler = true;
      fixture.call();
      assertTrue(fixture.terminal.await(3, TimeUnit.SECONDS));
      assertTrue(fixture.cleaned.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
      assertEquals(0, fixture.interceptor.activeRequests());
      assertEquals(1, fixture.interceptor.observationFailureCount());
      assertEquals(Status.Code.INTERNAL, fixture.status.get().getCode());
    }
  }

  private static final class Fixture implements AutoCloseable {
    final CountDownLatch firstMessage = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicInteger receivedMessages =
        new java.util.concurrent.atomic.AtomicInteger();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch cleaned = new CountDownLatch(1);
    final CountDownLatch completed = new CountDownLatch(1);
    final CountDownLatch terminal = new CountDownLatch(1);
    final AtomicReference<RequestScope> scope = new AtomicReference<>();
    final AtomicReference<Status> status = new AtomicReference<>();
    final java.util.concurrent.CompletableFuture<String> shared =
        new java.util.concurrent.CompletableFuture<>();
    final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    final RequestScopeInterceptor interceptor;
    final Server server;
    final ManagedChannel channel;
    volatile boolean throwInHandler;

    Fixture(Duration timeout, RequestCompletionRecorder recorder) throws Exception {
      scheduler.setRemoveOnCancelPolicy(true);
      interceptor =
          new RequestScopeInterceptor(
              scheduler,
              1,
              timeout,
              1024,
              (method, outcome, trace, request, elapsed) -> {
                try {
                  recorder.recordCompletion(method, outcome, trace, request, elapsed);
                } finally {
                  if (request.isClosed()) completed.countDown();
                }
              });
      server =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
              .intercept(interceptor)
              .addService(
                  new KVGrpc.KVImplBase() {
                    @Override
                    public void range(
                        Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> response) {
                      var current = RequestScopeInterceptor.currentScope();
                      scope.set(current);
                      current.registerCleanup(cleaned::countDown);
                      current.track(shared.copy());
                      started.countDown();
                      if (throwInHandler) throw new IllegalStateException("private-handler-value");
                    }
                  })
              .addService(
                  new etcdserverpb.WatchGrpc.WatchImplBase() {
                    @Override
                    public StreamObserver<Rpc.WatchRequest> watch(
                        StreamObserver<Rpc.WatchResponse> responses) {
                      RequestScopeInterceptor.currentScope().registerCleanup(cleaned::countDown);
                      return new StreamObserver<>() {
                        @Override
                        public void onNext(Rpc.WatchRequest request) {
                          receivedMessages.incrementAndGet();
                          firstMessage.countDown();
                        }

                        @Override
                        public void onError(Throwable failure) {}

                        @Override
                        public void onCompleted() {}
                      };
                    }
                  })
              .build()
              .start();
      channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
    }

    ClientCall<Rpc.RangeRequest, Rpc.RangeResponse> call() {
      var call = channel.newCall(KVGrpc.getRangeMethod(), CallOptions.DEFAULT);
      call.start(
          new ClientCall.Listener<>() {
            @Override
            public void onClose(Status result, Metadata trailers) {
              status.set(result);
              terminal.countDown();
            }
          },
          new Metadata());
      call.request(1);
      call.sendMessage(Rpc.RangeRequest.getDefaultInstance());
      call.halfClose();
      return call;
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
      scheduler.shutdownNow();
      assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS));
      assertTrue(server.awaitTermination(3, TimeUnit.SECONDS));
      assertTrue(scheduler.awaitTermination(3, TimeUnit.SECONDS));
    }
  }
}
