package io.etcdhbase.server;

import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Builds one ledger shared by every active table group and listener in this process. */
final class M3RuntimeBudgets {
  private M3RuntimeBudgets() {}

  static BudgetManager create(RuntimeConfiguration configuration, M3RuntimeConfiguration activation) {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(activation, "activation");
    var resources = configuration.resources();
    var groups = configuration.listeners().stream().map(RuntimeConfiguration.Listener::tableGroupId)
        .distinct().toList();
    if (groups.isEmpty() || groups.size() > 64 || resources.maxConcurrentRequests() < 1
        || resources.maxConcurrentRequests() > 100_000 || resources.maxScannersPerTable() < 1
        || resources.maxScannersPerTable() > 1024 || resources.maxQueuedRequests() != 0) {
      throw new IllegalArgumentException("invalid M3 group or admission limits");
    }
    int requests = Math.addExact(Math.multiplyExact(resources.maxConcurrentRequests(), 2), groups.size());
    int operations = Math.addExact(Math.multiplyExact(resources.maxConcurrentRequests(), 4),
        Math.multiplyExact(groups.size(), 2));
    int scanners = Math.multiplyExact(resources.maxScannersPerTable(), groups.size());
    int cleanup = Math.addExact(operations, scanners);
    long memory = activation.memoryBytes();
    var global = new BudgetManager.Limits(requests, 0, operations, scanners, cleanup, 0, memory);
    var perGroup = new BudgetManager.Limits(requests, 0, operations,
        resources.maxScannersPerTable(), cleanup, 0, memory);
    Map<String, BudgetManager.Limits> groupLimits = new LinkedHashMap<>();
    for (String group : groups) groupLimits.put(group, perGroup);
    return new BudgetManager(global, groupLimits, perGroup);
  }
}
