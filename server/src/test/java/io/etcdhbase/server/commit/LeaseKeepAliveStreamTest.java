package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LeaseKeepAliveStreamTest {
  @Test
  void serializesResponsesAndRequestsOneInboundMessageAtATime() {
    var observer = new RecordingObserver();
    var writes = new AtomicInteger();
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> {
        writes.incrementAndGet();
        return CompletableFuture.completedFuture(delivery(request.getID(), releases));
      }, metrics);

      assertEquals(1, observer.requests);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(11).build());

      assertEquals(1, writes.get());
      assertEquals(1, observer.responses.size());
      assertEquals(11, observer.responses.getFirst().getID());
      assertEquals(1, releases.get());
      assertEquals(2, observer.requests,
          "the next request is admitted only after the previous response leaves the stream");
      assertEquals(1, metrics.leaseRenewals().responses());
      assertEquals(observer.responses.getFirst().getSerializedSize(),
          metrics.leaseRenewals().responseBytes());
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());
      stream.onCompleted();
      assertTrue(observer.completed);
    }
  }

  @Test
  void doesNotSendUntilReadyAndReleasesPendingDeliveryOnCancellation() {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(delivery(request.getID(), releases)), metrics);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(21).build());

      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      observer.ready = true;
      observer.readyHandler.run();
      assertEquals(1, observer.responses.size());
      assertEquals(1, releases.get());
      assertEquals(2, observer.requests);
      assertEquals(1, metrics.leaseRenewals().responses());
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());

      observer.ready = false;
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(22).build());
      assertEquals(1, observer.responses.size());
      assertEquals(1, releases.get());
      observer.cancel();

      assertEquals(2, releases.get(), "cancellation releases a response waiting for transport readiness");
      assertEquals(1, observer.responses.size());
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void halfCloseWaitsForCurrentOperationBeforeCompleting() {
    var observer = new RecordingObserver();
    CompletableFuture<LeaseKeepAliveStream.Delivery> operation = new CompletableFuture<>();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> operation,
          new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(31).build());
      stream.onCompleted();

      assertFalse(observer.completed, "the response for an accepted request must precede half-close");
      operation.complete(delivery(31, releases));

      assertEquals(1, observer.responses.size());
      assertEquals(31, observer.responses.getFirst().getID());
      assertTrue(observer.completed);
      assertEquals(1, releases.get());
    }
  }

  @Test
  void observerFailureReleasesDeliveryAndStopsFurtherInput() {
    var observer = new RecordingObserver();
    observer.failOnNext = true;
    var handled = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> {
        handled.incrementAndGet();
        return CompletableFuture.completedFuture(delivery(request.getID(), releases));
      }, new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(41).build());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(42).build());

      assertEquals(1, handled.get());
      assertEquals(1, releases.get());
      assertEquals(1, observer.requests);
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void timesOutAStalledOutboundResponseAndReleasesItsDelivery() throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(1).build(),
              releases::incrementAndGet)), metrics);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(51).build());

      assertTrue(observer.failed.await(2, TimeUnit.SECONDS), "stalled response was not terminated");
      assertTrue(observer.failure instanceof io.grpc.StatusRuntimeException);
      assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertTrue(observer.responses.isEmpty(), "an expired positive TTL response was sent");
      assertEquals(1, releases.get(), "the timed-out response budget must be released once");
      assertTrue(scope.isCancelled(), "the failed stream must stop accepting new work");
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());
    }
  }

  private static ScheduledThreadPoolExecutor timers() {
    var timers = new ScheduledThreadPoolExecutor(1);
    timers.setRemoveOnCancelPolicy(true);
    timers.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return timers;
  }

  private static LeaseKeepAliveStream.Delivery delivery(long id, AtomicInteger releases) {
    return new LeaseKeepAliveStream.Delivery(
        Rpc.LeaseKeepAliveResponse.newBuilder().setID(id).setTTL(30).build(), releases::incrementAndGet);
  }

  private static final class RecordingObserver
      extends ServerCallStreamObserver<Rpc.LeaseKeepAliveResponse> {
    private final List<Rpc.LeaseKeepAliveResponse> responses = new ArrayList<>();
    private Runnable readyHandler;
    private Runnable cancelHandler;
    private boolean ready = true;
    private boolean cancelled;
    private boolean completed;
    private boolean failOnNext;
    private int requests;
    private final CountDownLatch failed = new CountDownLatch(1);
    private Throwable failure;

    @Override public boolean isCancelled() { return cancelled; }
    @Override public void setOnCancelHandler(Runnable handler) { cancelHandler = handler; }
    @Override public void setCompression(String compression) {}
    @Override public void setOnReadyHandler(Runnable handler) { readyHandler = handler; }
    @Override public boolean isReady() { return ready; }
    @Override public void request(int count) { requests += count; }
    @Override public void disableAutoInboundFlowControl() {}
    @Override public void setMessageCompression(boolean enabled) {}

    @Override public void onNext(Rpc.LeaseKeepAliveResponse response) {
      if (failOnNext) throw new IllegalStateException("simulated observer failure");
      responses.add(response);
    }

    @Override public void onError(Throwable cause) { failure = cause; failed.countDown(); }
    @Override public void onCompleted() { completed = true; }

    private void cancel() {
      cancelled = true;
      cancelHandler.run();
    }
  }
}
