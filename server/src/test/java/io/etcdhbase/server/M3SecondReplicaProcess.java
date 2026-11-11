package io.etcdhbase.server;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.server.commit.M3CacheRuntimeTestAccess;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseConfiguration;

/** Attempts a second complete runtime while another process owns the table group. */
public final class M3SecondReplicaProcess {
  private M3SecondReplicaProcess() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 4) throw new IllegalArgumentException("invalid replica fixture arguments");
    var plan = M3TableGroupRuntimeIT.plan(arguments[1], Integer.parseInt(arguments[2]), arguments[3]);
    var configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    var authentication = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), authentication,
             configuration, HBaseAuthenticators.simpleDevelopment(),
             (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
             1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-second-replica", new char[0])) {
      var identity = provider.authenticate(credentials, authentication).toCompletableFuture().get(30, TimeUnit.SECONDS);
      var limit = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
      var budgets = new BudgetManager(limit, Map.of(plan.source().tableGroupId(), limit), limit);
      var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);
      try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30));
           var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        expectOwnerRejection(() -> M3CacheRuntimeTestAccess.open(plan, M3TableGroupRuntimeIT.activation("2"),
            resources, provider, identity, budgets, scans, startup));
      }
      if (budgets.snapshot().reservedBytes() != 0 || budgets.snapshot().allocatedBytes() != 0) {
        throw new IllegalStateException("second runtime did not release its resources");
      }
      System.out.println("m3-second-replica: active owner rejected; resources drained");
    }
  }

  @FunctionalInterface
  private interface RuntimeOpen {
    M3TableGroupRuntime open() throws Exception;
  }

  private static void expectOwnerRejection(RuntimeOpen opener) throws Exception {
    try {
      var unexpected = opener.open();
      try { throw new IllegalStateException("second runtime unexpectedly acquired an active owner"); }
      finally { unexpected.close(); unexpected.drained().get(30, TimeUnit.SECONDS); }
    } catch (M3TableGroupRuntime.StartupException failure) {
      verifyOwnerRejection(failure);
    }
  }

  private static void verifyOwnerRejection(M3TableGroupRuntime.StartupException failure) throws Exception {
    failure.cleanup().toCompletableFuture().get(30, TimeUnit.SECONDS);
    if (!(failure.getCause() instanceof BackendFailure backend)
        || backend.category() != BackendFailure.Category.UNAVAILABLE) throw failure;
  }
}
