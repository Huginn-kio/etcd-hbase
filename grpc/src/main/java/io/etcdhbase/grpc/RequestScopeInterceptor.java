package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Maps gRPC cancellation and deadlines to an internal request scope. */
public final class RequestScopeInterceptor implements ServerInterceptor {
  private static final Context.Key<RequestScope> REQUEST_SCOPE =
      Context.key("etcd-hbase-request-scope");

  public static RequestScope currentScope() {
    RequestScope scope = REQUEST_SCOPE.get();
    if (scope == null) {
      throw new IllegalStateException("request scope is available only inside an intercepted RPC");
    }
    return scope;
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
      ServerCall<RequestT, ResponseT> call,
      Metadata headers,
      ServerCallHandler<RequestT, ResponseT> next) {
    Context parent = Context.current();
    RequestScope scope = createScope(parent.getDeadline());
    Context requestContext = parent.withValue(REQUEST_SCOPE, scope);
    Context.CancellationListener cancellationListener = ignored -> scope.cancel();
    requestContext.addListener(cancellationListener, Runnable::run);

    AtomicReference<ServerCall.Listener<RequestT>> delegate = new AtomicReference<>();
    try {
      requestContext.run(() -> delegate.set(next.startCall(call, headers)));
      Objects.requireNonNull(delegate.get(), "server call handler returned no listener");
    } catch (RuntimeException | Error failure) {
      requestContext.removeListener(cancellationListener);
      scope.close();
      throw failure;
    }

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate.get()) {
      @Override
      public void onMessage(RequestT message) {
        runScoped(() -> super.onMessage(message));
      }

      @Override
      public void onHalfClose() {
        runScoped(super::onHalfClose);
      }

      @Override
      public void onReady() {
        runScoped(super::onReady);
      }

      @Override
      public void onCancel() {
        scope.cancel();
        try {
          requestContext.run(super::onCancel);
        } finally {
          requestContext.removeListener(cancellationListener);
        }
      }

      @Override
      public void onComplete() {
        try {
          requestContext.run(super::onComplete);
        } finally {
          scope.close();
          requestContext.removeListener(cancellationListener);
        }
      }

      private void runScoped(Runnable callback) {
        try {
          requestContext.run(callback);
        } catch (RuntimeException | Error failure) {
          scope.cancel();
          requestContext.removeListener(cancellationListener);
          throw failure;
        }
      }
    };
  }

  private static RequestScope createScope(Deadline deadline) {
    if (deadline == null) {
      return RequestScope.withoutDeadline();
    }
    long remainingNanos = Math.max(0, deadline.timeRemaining(TimeUnit.NANOSECONDS));
    return RequestScope.withTimeout(Duration.ofNanos(remainingNanos));
  }
}
