package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces bounded admission and propagates client/server deadlines into request-owned resources.
 */
public final class RequestScopeInterceptor implements ServerInterceptor {
  static final Context.Key<RequestScope> SCOPE = Context.key("etcd-hbase-request-scope");
  static final Context.Key<String> TRACE = Context.key("etcd-hbase-trace-id");
  private final ScheduledExecutorService scheduler;
  private final Semaphore admission;
  private final int maxConcurrentRequests;
  private final Duration timeout;
  private final int maxRequestBytes;
  private final RequestCompletionRecorder recorder;
  private final AtomicLong observationFailures = new AtomicLong();

  public RequestScopeInterceptor(
      ScheduledExecutorService scheduler,
      int maxConcurrentRequests,
      Duration timeout,
      int maxRequestBytes,
      RequestCompletionRecorder recorder) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.recorder = Objects.requireNonNull(recorder, "recorder");
    if (maxConcurrentRequests < 1
        || maxRequestBytes < 1
        || timeout.isNegative()
        || timeout.isZero()
        || timeout.compareTo(Duration.ofMinutes(10)) > 0) {
      throw new IllegalArgumentException("invalid request budget");
    }
    this.admission = new Semaphore(maxConcurrentRequests);
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.maxRequestBytes = maxRequestBytes;
  }

  public static RequestScope currentScope() {
    return Objects.requireNonNull(SCOPE.get(), "request scope is available only inside an RPC");
  }

  public static String currentTraceId() {
    return Objects.requireNonNull(TRACE.get(), "trace ID is available only inside an RPC");
  }

  public int activeRequests() {
    return maxConcurrentRequests - admission.availablePermits();
  }

  public long observationFailureCount() {
    return observationFailures.get();
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
      ServerCall<RequestT, ResponseT> call,
      Metadata headers,
      ServerCallHandler<RequestT, ResponseT> next) {
    var methodClass =
        RequestLifecycle.classifyMethod(call.getMethodDescriptor().getFullMethodName());
    if (!admission.tryAcquire()) {
      try (var scope = RequestScope.withTimeout(timeout)) {
        scope.close();
        record(
            methodClass,
            RequestCompletionRecorder.Outcome.RESOURCE_EXHAUSTED,
            UUID.randomUUID().toString(),
            scope,
            0);
      }
      call.close(
          Status.RESOURCE_EXHAUSTED.withDescription("request capacity exhausted"), new Metadata());
      return new ServerCall.Listener<>() {};
    }
    RequestLifecycle<RequestT, ResponseT> lifecycle = null;
    Context.CancellableContext context = null;
    try {
      context =
          Context.current().withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS, scheduler);
      lifecycle = new RequestLifecycle<>(call, context, methodClass, maxRequestBytes, this);
      return lifecycle.start(headers, next);
    } catch (RuntimeException failure) {
      if (lifecycle != null) lifecycle.fail(failure);
      else {
        if (context != null) context.close();
        release();
        call.close(
            Status.INTERNAL.withDescription("request initialization failed"), new Metadata());
      }
      return new ServerCall.Listener<>() {};
    } catch (Error failure) {
      if (lifecycle == null) {
        if (context != null) context.close();
        release();
      } else lifecycle.finish(Status.INTERNAL);
      throw failure;
    }
  }

  void release() {
    admission.release();
  }

  void record(
      RequestCompletionRecorder.MethodClass methodClass,
      RequestCompletionRecorder.Outcome outcome,
      String traceId,
      RequestScope scope,
      long elapsedNanos) {
    try {
      recorder.recordCompletion(methodClass, outcome, traceId, scope, elapsedNanos);
    } catch (RuntimeException failure) {
      observationFailures.incrementAndGet();
    }
  }
}
