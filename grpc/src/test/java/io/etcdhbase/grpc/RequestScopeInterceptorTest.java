package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class RequestScopeInterceptorTest {
  private final RequestScopeInterceptor interceptor = new RequestScopeInterceptor();

  @Test
  void installsScopeForTheHandlerAndClosesItOnCompletion() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    AtomicInteger cleanupCount = new AtomicInteger();
    ServerCall.Listener<String> listener =
        intercept(
            (call, headers) -> {
              captured.set(RequestScopeInterceptor.currentScope());
              return new ServerCall.Listener<>() {};
            });
    captured.get().registerCleanup(cleanupCount::incrementAndGet);

    listener.onComplete();

    assertTrue(captured.get().isClosed());
    assertFalse(captured.get().isCancelled());
    assertEquals(1, cleanupCount.get());
  }

  @Test
  void parentContextCancellationClosesScopeAndRunsCleanupOnce() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    AtomicInteger cleanupCount = new AtomicInteger();
    Context.CancellableContext cancellable = Context.current().withCancellation();
    Context previous = cancellable.attach();
    try {
      ServerCall.Listener<String> listener =
          intercept(
              (call, headers) -> {
                captured.set(RequestScopeInterceptor.currentScope());
                return new ServerCall.Listener<>() {};
              });
      captured.get().registerCleanup(cleanupCount::incrementAndGet);

      cancellable.cancel(new CancellationException("test cancellation"));
      listener.onCancel();

      assertTrue(captured.get().isCancelled());
      assertTrue(captured.get().isClosed());
      assertEquals(1, cleanupCount.get());
    } finally {
      cancellable.detach(previous);
      cancellable.close();
    }
  }

  @Test
  void closesScopeWhenStartingTheHandlerFails() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    IllegalStateException expected = new IllegalStateException("handler failed");
    ServerCallHandler<String, String> handler =
        (call, headers) -> {
          captured.set(RequestScopeInterceptor.currentScope());
          throw expected;
        };

    IllegalStateException observed =
        assertThrows(IllegalStateException.class, () -> intercept(handler));

    assertTrue(captured.get().isClosed());
    assertTrue(observed == expected);
  }

  @Test
  void currentScopeIsUnavailableOutsideAnInterceptedCall() {
    assertThrows(IllegalStateException.class, RequestScopeInterceptor::currentScope);
  }

  private ServerCall.Listener<String> intercept(ServerCallHandler<String, String> handler) {
    @SuppressWarnings("unchecked")
    ServerCall<String, String> call = mock(ServerCall.class);
    ServerCall.Listener<String> listener = interceptor.interceptCall(call, new Metadata(), handler);
    assertNotNull(listener);
    return listener;
  }
}
