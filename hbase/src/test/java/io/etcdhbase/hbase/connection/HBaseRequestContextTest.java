package io.etcdhbase.hbase.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.jupiter.api.Test;

class HBaseRequestContextTest {
  @Test
  void cancellingBorrowDoesNotCloseOrCancelTheSharedConnection() {
    var scope = RequestScope.withoutDeadline();
    var context = new HBaseRequestContext(scope);
    var identity = mock(AuthorizedBackendIdentity.class);
    var factory = mock(HBaseConnectionFactory.class);
    var source = new CompletableFuture<AsyncConnection>();
    when(factory.connectionFor(identity)).thenReturn(source);
    var borrowed = context.borrowConnection(factory, identity);
    scope.cancel();
    assertTrue(borrowed.isCancelled());
    assertFalse(source.isCancelled());
    var connection = mock(AsyncConnection.class);
    source.complete(connection);
    verifyNoInteractions(connection);
  }

  @Test
  void cancelsOperationsClosesLateScannersAndPreservesAsyncFailure() {
    var scope = RequestScope.withoutDeadline();
    var context = new HBaseRequestContext(scope);
    var failed = new IllegalStateException("private-value");
    var operation = context.execute(() -> CompletableFuture.failedFuture(failed));
    assertSame(failed, assertThrows(CompletionException.class, operation::join).getCause());
    var pending = context.execute(CompletableFuture::new);
    scope.cancel();
    assertTrue(pending.isCancelled());
    var scanner = mock(ResultScanner.class);
    context.ownScanner(scanner);
    context.ownScanner(scanner);
    verify(scanner, times(1)).close();
  }

  @Test
  void expiredDeadlineNeverSubmitsBackendWork() {
    var context = new HBaseRequestContext(RequestScope.withTimeout(Duration.ZERO));
    var work = new java.util.concurrent.atomic.AtomicBoolean();
    var result =
        context.execute(
            () -> {
              work.set(true);
              return CompletableFuture.completedFuture(1);
            });
    assertFalse(work.get());
    assertTrue(
        assertThrows(CompletionException.class, result::join).getCause()
            instanceof java.util.concurrent.TimeoutException);
  }
}
