package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared, bounded resource profile for Lease control and expiry work. */
public final class LeaseBudgetProfile {
  public static final long RESERVED_OWNER_QUEUE_BYTES = 64L << 10;
  public static final BudgetManager.Reservation LEASE_REQUEST_MEMORY =
      new BudgetManager.Reservation(64L << 10, 64L << 10,
          ManifestCatalog.maximumProofReadPageBytes(), 16L << 20, 64L << 20);
  public static final BudgetManager.Reservation EXPIRY_WORKER_MEMORY =
      new BudgetManager.Reservation(4L << 10, 64L << 10, 1L << 20, 0, 0);
  public static final BudgetManager.ReadLimits EXPIRY_WORKER_READ_LIMITS =
      new BudgetManager.ReadLimits(16, 1L << 20, 4, 4, 16);

  private LeaseBudgetProfile() {}

  /** Covers the binding page plus the expiry candidate page retained during automatic revocation. */
  public static BudgetManager.Reservation ownerMutationReservation(int maximumEtcdRequestBytes) {
    BudgetManager.Reservation bindingRead =
        LeaseGateway.bindingReadReservation(LEASE_REQUEST_MEMORY, maximumEtcdRequestBytes);
    return new BudgetManager.Reservation(bindingRead.heads(),
        Math.addExact(bindingRead.page(), LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES),
        bindingRead.io(), bindingRead.output(), bindingRead.candidates());
  }

  public static BudgetManager.ReservedCapacity perGroupCapacity(int maximumEtcdRequestBytes) {
    BudgetManager.Reservation leaseRequest = ownerMutationReservation(maximumEtcdRequestBytes);
    return new BudgetManager.ReservedCapacity(2, 0, 2, 1, 2, 0,
        Math.addExact(leaseRequest.total(), EXPIRY_WORKER_MEMORY.total()));
  }

  public static BudgetManager.ReservedCapacity capacityForGroups(
      int groupCount, int maximumEtcdRequestBytes) {
    if (groupCount < 1 || groupCount > 64) throw new IllegalArgumentException("invalid group count");
    BudgetManager.ReservedCapacity perGroup = perGroupCapacity(maximumEtcdRequestBytes);
    return new BudgetManager.ReservedCapacity(
        Math.multiplyExact(perGroup.requests(), groupCount),
        Math.multiplyExact(perGroup.queued(), groupCount),
        Math.multiplyExact(perGroup.operations(), groupCount),
        Math.multiplyExact(perGroup.scanners(), groupCount),
        Math.multiplyExact(perGroup.cleanup(), groupCount),
        Math.multiplyExact(perGroup.queuedBytes(), groupCount),
        Math.multiplyExact(perGroup.memoryBytes(), groupCount));
  }

  public static Map<String, BudgetManager.ReservedCapacity> capacitiesFor(
      List<String> groupIds, int maximumEtcdRequestBytes) {
    if (groupIds.isEmpty() || groupIds.size() > 64) throw new IllegalArgumentException("invalid group count");
    Map<String, BudgetManager.ReservedCapacity> capacities = new LinkedHashMap<>();
    for (String groupId : groupIds) {
      capacities.put(groupId, perGroupCapacity(maximumEtcdRequestBytes));
    }
    return Map.copyOf(capacities);
  }
}
