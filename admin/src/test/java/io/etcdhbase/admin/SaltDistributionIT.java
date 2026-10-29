package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;
import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/** Exercises real server routing; it does not measure production capacity or latency. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class SaltDistributionIT {
  private TemporaryHBaseClusterProcess cluster;
  private HBaseBackendIdentityProvider provider;
  private AsyncConnection connection;
  private List<ServerName> servers;

  @BeforeAll
  void startThreeServersAndAuthenticateAnExplicitDevelopmentIdentity() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start(3);
    var scope = new AuthenticationScope("local", "local-hbase");
    provider =
        new HBaseBackendIdentityProvider(
            DEVELOPMENT_NO_AUTH_PROVIDER_ID,
            scope,
            cluster.clientConfiguration(),
            HBaseAuthenticators.simpleDevelopment(),
            (backend, ignored) -> backend.getAdmin().listNamespaces().thenApply(names -> null),
            1,
            1,
            OPERATION_TIMEOUT);
    try (var credentials = new BackendCredentials("j05-salt-operator", new char[0])) {
      var identity =
          await(provider.authenticate(credentials, scope).toCompletableFuture(), OPERATION_TIMEOUT);
      connection = await(provider.connectionFor(identity).toCompletableFuture(), OPERATION_TIMEOUT);
    }
    servers =
        await(connection.getAdmin().getRegionServers(), OPERATION_TIMEOUT).stream()
            .sorted()
            .toList();
  }

  @AfterAll
  void closeTheOwnedIdentityBeforeStoppingItsDisposableServers() throws Exception {
    try {
      if (provider != null) {
        provider.close();
      }
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  @Test
  void observesThreeIndependentRegionServers() {
    assertEquals(3, servers.size());
    assertEquals(3, servers.stream().map(ServerName::getPort).distinct().count());
  }
}
