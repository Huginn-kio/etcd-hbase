package io.etcdhbase.hbase.connection;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ResultScanner;

/** Binds only request-owned work and scanners; the J04 factory retains shared connections. */
public final class HBaseRequestContext {
  private final RequestScope scope;

  public HBaseRequestContext(RequestScope scope) {
    this.scope = Objects.requireNonNull(scope, "scope");
  }

  public CompletableFuture<AsyncConnection> borrowConnection(
      HBaseConnectionFactory factory, AuthorizedBackendIdentity identity) {
    // Cancelling this request must never cancel the factory's shared connection future.
    return execute(() -> factory.connectionFor(identity).toCompletableFuture().copy());
  }

  /** Cancellation is best effort; it cannot roll back an operation accepted by HBase. */
  public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> operation) {
    if (scope.isDeadlineExceeded()) {
      scope.cancel();
      return CompletableFuture.failedFuture(new TimeoutException());
    }
    if (scope.isClosed()) {
      return CompletableFuture.failedFuture(new CancellationException());
    }
    try {
      return scope.track(operation.get());
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  /** Transfers scanner ownership, including a scanner arriving after cancellation. */
  public ResultScanner ownScanner(ResultScanner scanner) {
    scope.registerCleanup(scanner);
    return scanner;
  }
}
