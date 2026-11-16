package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.commit.LeaseBudgetProfile;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3MemoryReservations;
import io.etcdhbase.server.config.RuntimeConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;

class M3RuntimeBudgetsTest {
  @Test
  void m4MinimumAccountsForEveryGroupAndConcurrentLeaseOwner() throws Exception {
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a"),
        new RuntimeConfiguration.Listener("second", "127.0.0.1", 0, "b")),
        List.of(), List.of(), List.of(), resources);
    long regularMinimum = M3MemoryReservations.minimumLeaseMemory(2, 1_572_864);
    long minimum = Math.addExact(regularMinimum,
        LeaseBudgetProfile.capacityForGroups(2, 1_572_864).memoryBytes());
    var tooSmall = leaseActivation(minimum - 1);
    assertThrows(IllegalArgumentException.class, () -> M3RuntimeBudgets.create(configuration, tooSmall));
    var budgets = M3RuntimeBudgets.create(configuration, leaseActivation(minimum));
    try (var recoveryA = RequestScope.withoutDeadline(); var recoveryB = RequestScope.withoutDeadline();
         var gateway = RequestScope.withoutDeadline(); var owner = RequestScope.withoutDeadline();
         var a = budgets.admit("a", recoveryA, M3MemoryReservations.RECOVERY);
         var b = budgets.admit("b", recoveryB, M3MemoryReservations.RECOVERY);
         var read = budgets.admit("a", gateway,
             M3MemoryReservations.leaseBindingRead(M3MemoryReservations.LEASE, 1_572_864));
         var mutation = budgets.admit("a", owner, M3MemoryReservations.LEASE)) {
      assertEquals(regularMinimum, budgets.snapshot().reservedBytes());
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
  }

  private static M3RuntimeConfiguration leaseActivation(long memory) {
    return new M3RuntimeConfiguration("1", 6_000, 1_000, 30_000, 30_000,
        memory, 1_572_864, 2, 16, 60_000, false, true, 5, 1_000,
        1_000, 1024, 4L << 20, 32L << 20);
  }

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
