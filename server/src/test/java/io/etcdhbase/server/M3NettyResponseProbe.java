package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Exercises real Netty serialization; the response is from an already committed fixture call. */
final class M3NettyResponseProbe {
  @FunctionalInterface
  interface Responder {
    void send(RequestScope scope, StreamObserver<Rpc.PutResponse> response) throws Exception;
  }

  private M3NettyResponseProbe() {}

  static void verifyCancellationDuringSerialization(Responder responder, LongSupplier allocatedBytes)
      throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var cancelled = new CountDownLatch(1);
    var completed = new CountDownLatch(1);
    var requestScope = new AtomicReference<RequestScope>();
    var failure = new AtomicReference<Throwable>();
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var workers = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1), Thread.ofPlatform().daemon().name("m3-netty-probe-", 0).factory());
    var interceptor = new RequestScopeInterceptor(scheduler, 1, Duration.ofSeconds(30), 1024,
        (method, outcome, trace, scope, elapsed) -> completed.countDown());
    MethodDescriptor<Rpc.PutRequest, Rpc.PutResponse> original = KVGrpc.getPutMethod();
    var descriptor = original.toBuilder(original.getRequestMarshaller(),
        slowMarshaller(original.getResponseMarshaller(), entered, release)).build();
    var service = ServerServiceDefinition.builder(KVGrpc.SERVICE_NAME)
        .addMethod(descriptor, ServerCalls.asyncUnaryCall((request, response) -> {
          RequestScope scope = RequestScopeInterceptor.currentScope();
          requestScope.set(scope);
          scope.registerCleanup(cancelled::countDown);
          workers.execute(() -> {
            try {
              responder.send(scope, response);
            } catch (Throwable problem) {
              failure.set(problem);
              if (!scope.isClosed()) response.onError(problem);
            }
          });
        })).build();
    Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
        .intercept(interceptor).addService(service).build().start();
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
        .usePlaintext().build();
    try {
      ClientCall<Rpc.PutRequest, Rpc.PutResponse> call = channel.newCall(descriptor, CallOptions.DEFAULT);
      call.start(new ClientCall.Listener<>() {
        @Override public void onClose(Status status, Metadata trailers) {}
      }, new Metadata());
      call.request(1);
      call.sendMessage(Rpc.PutRequest.newBuilder().setKey(com.google.protobuf.ByteString.copyFromUtf8("probe")).build());
      call.halfClose();
      assertTrue(entered.await(10, TimeUnit.SECONDS), "Netty did not begin response serialization");
      call.cancel("synthetic cancellation", null);
      assertTrue(cancelled.await(10, TimeUnit.SECONDS), "server did not observe cancellation");
      assertTrue(allocatedBytes.getAsLong() > 0, "response bytes must remain accounted");
      assertFalse(requestScope.get().cleanupCompletion().toCompletableFuture().isDone());
      assertFalse(completed.await(100, TimeUnit.MILLISECONDS), "request permit was released early");
      release.countDown();
      requestScope.get().cleanupCompletion().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertTrue(completed.await(10, TimeUnit.SECONDS));
      assertNull(failure.get());
    } finally {
      release.countDown();
      channel.shutdownNow();
      channel.awaitTermination(10, TimeUnit.SECONDS);
      server.shutdownNow();
      server.awaitTermination(10, TimeUnit.SECONDS);
      workers.shutdownNow();
      workers.awaitTermination(10, TimeUnit.SECONDS);
      interceptor.close();
      scheduler.shutdownNow();
      scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private static MethodDescriptor.Marshaller<Rpc.PutResponse> slowMarshaller(
      MethodDescriptor.Marshaller<Rpc.PutResponse> delegate, CountDownLatch entered,
      CountDownLatch release) {
    return new MethodDescriptor.Marshaller<>() {
      @Override public InputStream stream(Rpc.PutResponse value) {
        return new FilterInputStream(delegate.stream(value)) {
          private final AtomicBoolean firstRead = new AtomicBoolean(true);

          @Override public int read() throws IOException {
            waitOnce();
            return in.read();
          }

          @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            waitOnce();
            return in.read(bytes, offset, length);
          }

          private void waitOnce() throws IOException {
            if (!firstRead.compareAndSet(true, false)) return;
            entered.countDown();
            try {
              if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("serialization stalled");
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new IOException("serialization interrupted", interrupted);
            }
          }
        };
      }

      @Override public Rpc.PutResponse parse(InputStream input) { return delegate.parse(input); }
    };
  }
}
