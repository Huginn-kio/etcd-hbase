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
  void holdsAdmissionUntilConcurrentCleanupReallyCompletesAndRecordsItsFailure() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var fixture = new Fixture(Duration.ofSeconds(5), RequestCompletionRecorder.NOOP)) {
      fixture.extraCleanup =
          () -> {
            entered.countDown();
            release.await();
            throw new java.io.IOException("private-cleanup-value");
          };
      var call = fixture.call();
      assertTrue(fixture.started.await(2, TimeUnit.SECONDS));
      try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
        try {
          var cancellation = executor.submit(() -> fixture.scope.get().cancel());
          assertTrue(entered.await(2, TimeUnit.SECONDS));
          call.cancel("cancelled", null);
          assertTrue(fixture.terminal.await(2, TimeUnit.SECONDS));
          assertEquals(1, fixture.interceptor.activeRequests());
          assertEquals(1, fixture.completed.getCount());
          release.countDown();
          assertTrue(cancellation.get(2, TimeUnit.SECONDS));
          assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
          assertEquals(0, fixture.interceptor.activeRequests());
          assertEquals(1, fixture.recordedCleanupFailures.get());
        } finally {
          release.countDown();
        }
      }
    } finally {
      release.countDown();
    }
  }

  @Test
  void blockedFutureCallbackCannotDelayAnotherCallsDeadline() throws Exception {
    try (var fixture = new Fixture(Duration.ofMillis(300), RequestCompletionRecorder.NOOP, 2)) {
      fixture.blockTrackedCallback = true;
      try {
        fixture.call();
        assertTrue(fixture.callbackEntered.await(2, TimeUnit.SECONDS));
        var secondDone = new CountDownLatch(1);
        var secondStatus = new AtomicReference<Status>();
        fixture.call(secondDone, secondStatus);
        assertTrue(secondDone.await(2, TimeUnit.SECONDS));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, secondStatus.get().getCode());
        assertEquals(1, fixture.releaseCallback.getCount());
      } finally {
        fixture.releaseCallback.countDown();
      }
    }
  }

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
      assertEquals(
          1,
          fixture.completed.getCount(),
          "a rejected request is not this admitted call's cleanup completion");
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
    var fixture =
        new Fixture(
            Duration.ofSeconds(5),
            (method, outcome, trace, scope, elapsed) -> {
              throw new IllegalStateException("private-observation-value");
            });
    try (fixture) {
      fixture.throwInHandler = true;
      fixture.call();
      assertTrue(fixture.terminal.await(3, TimeUnit.SECONDS));
      assertTrue(fixture.cleaned.await(2, TimeUnit.SECONDS));
      assertTrue(fixture.completed.await(2, TimeUnit.SECONDS));
      assertEquals(0, fixture.interceptor.activeRequests());
      assertEquals(Status.Code.INTERNAL, fixture.status.get().getCode());
    }
    assertEquals(1, fixture.interceptor.observationFailureCount());
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
    final java.util.Set<RequestScope> admittedScopes =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    final AtomicReference<Status> status = new AtomicReference<>();
    final java.util.concurrent.CompletableFuture<String> shared =
        new java.util.concurrent.CompletableFuture<>();
    final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    final RequestScopeInterceptor interceptor;
    final Server server;
    final ManagedChannel channel;
    volatile boolean throwInHandler;
    volatile AutoCloseable extraCleanup;
    volatile boolean blockTrackedCallback;
    final CountDownLatch callbackEntered = new CountDownLatch(1);
    final CountDownLatch releaseCallback = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicInteger handlers =
        new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.atomic.AtomicInteger recordedCleanupFailures =
        new java.util.concurrent.atomic.AtomicInteger();

    Fixture(Duration timeout, RequestCompletionRecorder recorder) throws Exception {
      this(timeout, recorder, 1);
    }

    Fixture(Duration timeout, RequestCompletionRecorder recorder, int capacity) throws Exception {
      scheduler.setRemoveOnCancelPolicy(true);
      interceptor =
          new RequestScopeInterceptor(
              scheduler,
              capacity,
              timeout,
              1024,
              (method, outcome, trace, request, elapsed) -> {
                try {
                  if (admittedScopes.contains(request))
                    recordedCleanupFailures.set(request.cleanupFailureCount());
                  recorder.recordCompletion(method, outcome, trace, request, elapsed);
                } finally {
                  if (admittedScopes.contains(request) && request.isClosed()) completed.countDown();
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
                      admittedScopes.add(current);
                      current.registerCleanup(cleaned::countDown);
                      var tracked = current.track(shared.copy());
                      if (blockTrackedCallback && handlers.incrementAndGet() == 1) {
                        tracked.whenComplete(
                            (value, failure) -> {
                              callbackEntered.countDown();
                              try {
                                if (!releaseCallback.await(5, TimeUnit.SECONDS))
                                  throw new AssertionError("callback release timed out");
                              } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(interrupted);
                              }
                            });
                      }
                      if (extraCleanup != null) current.registerCleanup(extraCleanup);
                      started.countDown();
                      if (throwInHandler) throw new IllegalStateException("private-handler-value");
                    }
                  })
              .addService(
                  new etcdserverpb.WatchGrpc.WatchImplBase() {
                    @Override
                    public StreamObserver<Rpc.WatchRequest> watch(
                        StreamObserver<Rpc.WatchResponse> responses) {
                      var current = RequestScopeInterceptor.currentScope();
                      admittedScopes.add(current);
                      current.registerCleanup(cleaned::countDown);
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
      return call(terminal, status);
    }

    ClientCall<Rpc.RangeRequest, Rpc.RangeResponse> call(
        CountDownLatch done, AtomicReference<Status> observed) {
      var call = channel.newCall(KVGrpc.getRangeMethod(), CallOptions.DEFAULT);
      call.start(
          new ClientCall.Listener<>() {
            @Override
            public void onClose(Status result, Metadata trailers) {
              observed.set(result);
              done.countDown();
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
      interceptor.close();
      assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS));
      assertTrue(server.awaitTermination(3, TimeUnit.SECONDS));
      assertTrue(scheduler.awaitTermination(3, TimeUnit.SECONDS));
    }
  }
}
