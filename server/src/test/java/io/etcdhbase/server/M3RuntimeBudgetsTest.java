package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;

class M3RuntimeBudgetsTest {
  @Test
  void tableGroupsShareTheSameGlobalMemoryLimit() throws Exception {
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a"),
        new RuntimeConfiguration.Listener("second", "127.0.0.1", 0, "b")),
        List.of(), List.of(), List.of(), resources);
    var activation = new M3RuntimeConfiguration("1", 6_000, 1_000, 30_000, 30_000,
        256L << 20, 1_572_864, 2, 16, 60_000);
    var budgets = M3RuntimeBudgets.create(configuration, activation);
    var reservation = new BudgetManager.Reservation(0, 1, 1, 0, 160L << 20);
    try (var first = RequestScope.withoutDeadline(); var second = RequestScope.withoutDeadline();
         var admitted = budgets.admit("a", first, reservation)) {
      var failure = assertThrows(BackendFailure.class, () -> budgets.admit("b", second, reservation));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
    try (var scope = RequestScope.withoutDeadline();
         var account = budgets.admit("a", scope, new BudgetManager.Reservation(0, 1, 1, 0, 0));
         var queued = account.tryAcquire(BudgetManager.Work.queued(256)).orElseThrow()) {
      assertTrue(budgets.snapshot().queued() > 0, "internal scan scheduling needs a bounded queue");
    }
    assertEquals(0, budgets.snapshot().queued());
  }
}
