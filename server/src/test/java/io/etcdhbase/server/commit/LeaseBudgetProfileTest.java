package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LeaseBudgetProfileTest {
  @Test
  void leaseRequestsCanReserveTheLargestSystemRecordProofPage() {
    assertTrue(LeaseBudgetProfile.LEASE_REQUEST_MEMORY.io()
        >= ManifestCatalog.maximumProofReadPageBytes());
  }

  @Test
  void ownerMutationReservationCoversConcurrentExpiryAndBindingPages() throws Exception {
    int maximumEtcdRequestBytes = 1_572_864;
    long bindingPageBytes = LeaseGateway.bindingPageBytes(maximumEtcdRequestBytes);
    long requiredBindingPageMemory = LeaseBindingReader.requiredPageMemoryBytes(
        LeaseGateway.BINDING_PAGE_ENTRIES, bindingPageBytes);

    var reservation = LeaseBudgetProfile.ownerMutationReservation(maximumEtcdRequestBytes);
    var capacity = LeaseBudgetProfile.perGroupCapacity(maximumEtcdRequestBytes);
    long combinedPageMemory = Math.addExact(requiredBindingPageMemory,
        LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES);

    assertTrue(reservation.page() >= combinedPageMemory);
    assertTrue(capacity.memoryBytes() >= reservation.total()
        + LeaseBudgetProfile.EXPIRY_WORKER_MEMORY.total());

    var limits = new BudgetManager.Limits(1, 0, 1, 1, 1, 0, reservation.total());
    var budgets = new BudgetManager(limits, Map.of("alpha", limits), limits);
    try (var scope = RequestScope.withoutDeadline();
         var account = budgets.admit("alpha", scope, reservation);
         var expiryPage = account.tryRetain(BudgetManager.Memory.PAGE,
             LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES).orElseThrow();
         var bindingPage = account.tryRetain(BudgetManager.Memory.PAGE,
             requiredBindingPageMemory).orElseThrow()) {
      assertTrue(budgets.snapshot().allocatedBytes() >= combinedPageMemory);
    }
  }

  @Test
  void reservedCapacitySupportsCheckpointScanAndHistoryRevokeAtTheSameTime() throws Exception {
    int maximumEtcdRequestBytes = 1_572_864;
    var capacity = LeaseBudgetProfile.perGroupCapacity(maximumEtcdRequestBytes);
    var limits = new BudgetManager.Limits(capacity.requests(), capacity.queued(),
        capacity.operations(), capacity.scanners(), capacity.cleanup(), capacity.queuedBytes(),
        capacity.memoryBytes());
    var budgets = new BudgetManager(limits, Map.of("alpha", limits), limits,
        capacity, Map.of("alpha", capacity));

    try (var scope = RequestScope.withoutDeadline();
         var worker = budgets.admitReserved("alpha", scope,
             LeaseBudgetProfile.EXPIRY_WORKER_MEMORY,
             LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS);
         var revoke = budgets.admitReserved("alpha", scope,
             LeaseBudgetProfile.ownerMutationReservation(maximumEtcdRequestBytes));
         var revokeFinalization = revoke.tryAcquire(new BudgetManager.Work(0, 1, 0, 1, 0))
             .orElseThrow();
         var bindingScan = revoke.tryAcquire(BudgetManager.Work.scan()).orElseThrow();
         var historyPointRead = revoke.tryAcquire(new BudgetManager.Work(0, 1, 1, 1, 0))
             .orElseThrow();
         var workerControlRead = worker.tryAcquire(LeaseBudgetProfile.EXPIRY_CONTROL_READ_WORK)
             .orElseThrow()) {
      assertEquals(4, budgets.snapshot().operations());
      assertEquals(2, budgets.snapshot().scanners());
      assertEquals(3, budgets.snapshot().cleanup());
      workerControlRead.close();
      try (var workerCheckpointScan = worker.tryAcquire(
          LeaseBudgetProfile.EXPIRY_CHECKPOINT_SCAN_WORK).orElseThrow()) {
        assertEquals(4, budgets.snapshot().operations());
        assertEquals(3, budgets.snapshot().scanners());
        assertEquals(4, budgets.snapshot().cleanup());
      }
    }
  }
}
