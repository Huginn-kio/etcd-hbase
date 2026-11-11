package io.etcdhbase.server.commit;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Duration;
import java.util.Optional;

/** Exercises the internal assembly without exposing a production activation switch. */
public final class M3CacheRuntimeTestAccess {
  private M3CacheRuntimeTestAccess() {}

  public static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup) throws Exception {
    var cache = new M3TableGroupRuntime.CacheLimits(1L << 20, 8, 8, 8, 8L << 20,
        new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192),
        new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000),
        8, 512L << 20, 8, Duration.ofSeconds(30).toNanos());
    return M3TableGroupRuntime.open(plan, activation, limits, connections, identity,
        budgets, scans, startup, Optional.of(cache));
  }

  public static long hits(M3TableGroupRuntime runtime) { return runtime.cacheHitCount(); }
}
