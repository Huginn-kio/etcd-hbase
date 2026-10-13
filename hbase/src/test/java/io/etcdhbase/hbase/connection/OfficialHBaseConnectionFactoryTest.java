package io.etcdhbase.hbase.connection;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OfficialHBaseConnectionFactoryTest {
  @Test
  void shutdownWaitsForAnAcceptedConnectionToBePublished() throws Exception {
    CompletableFuture<AsyncConnection> creation = new CompletableFuture<>();
    BlockingConnectionFuture publication = new BlockingConnectionFuture();
    AsyncConnection connection = mock(AsyncConnection.class);
    OfficialHBaseConnectionFactory factory =
        new OfficialHBaseConnectionFactory(
            1, ignored -> creation, () -> publication);
    var result = factory.connectionFor(identity());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch shutdownStarted = new CountDownLatch(1);

    try {
      Future<Boolean> creationCompletion = executor.submit(() -> creation.complete(connection));
      assertTrue(publication.completionStarted.await(5, TimeUnit.SECONDS));

      Future<?> shutdown =
          executor.submit(
              () -> {
                shutdownStarted.countDown();
                factory.close();
              });
      assertTrue(shutdownStarted.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> shutdown.get(100, TimeUnit.MILLISECONDS));

      publication.allowCompletion.countDown();
      assertTrue(creationCompletion.get(5, TimeUnit.SECONDS));
      shutdown.get(5, TimeUnit.SECONDS);

      assertTrue(result.toCompletableFuture().isDone());
      assertSame(connection, result.toCompletableFuture().join());
      verify(connection).close();
    } finally {
      publication.allowCompletion.countDown();
      factory.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static AuthorizedBackendIdentity identity() {
    return new AuthorizedBackendIdentity(
        BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID,
        "alice",
        "ctx-a",
        new AuthenticationScope("local", "hbase"),
        Set.of("local"),
        1);
  }

  private static final class BlockingConnectionFuture extends CompletableFuture<AsyncConnection> {
    private final CountDownLatch completionStarted = new CountDownLatch(1);
    private final CountDownLatch allowCompletion = new CountDownLatch(1);

    @Override
    public boolean complete(AsyncConnection connection) {
      completionStarted.countDown();
      try {
        if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("connection publication was not released");
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("connection publication was interrupted", interrupted);
      }
      return super.complete(connection);
    }
  }
}
