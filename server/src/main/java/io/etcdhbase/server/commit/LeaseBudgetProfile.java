package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
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
  public static final BudgetManager.Reservation RECOVERY_MEMORY =
      new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 8L << 20, 48L << 20);
  // The protected window and the nested legacy reader each own a separate scope and account.
  static final BudgetManager.Reservation EVENT_BACKFILL_MEMORY = new BudgetManager.Reservation(
      8192, 8192, 2 * ManifestCatalog.maximumProofReadPageBytes(), 8192, 2L << 20);
  public static final BudgetManager.ReadLimits EXPIRY_WORKER_READ_LIMITS =
      new BudgetManager.ReadLimits(16, 1L << 20, 4, 4, 16);
  static final BudgetManager.Work EXPIRY_CONTROL_READ_WORK =
      new BudgetManager.Work(0, 1, 0, 0, 0);
  static final BudgetManager.Work EXPIRY_CHECKPOINT_SCAN_WORK = BudgetManager.Work.scan();
  static final long MAX_EXPIRY_CONTROL_READ_BYTES = CommitControlMessages.MAX_CONTROL_BYTES;
  static final long MAX_EXPIRY_CHECKPOINT_READ_BYTES =
      HBaseSystemRecordStore.MAX_EXPIRY_CHECKPOINT_SCAN_BYTES;
  static final long MAX_EXPIRY_CHECKPOINT_RECORD_BYTES =
      LeaseExpiryCheckpoint.IDENTITY_BYTES + LeaseExpiryCheckpoint.MAX_ENCODED_BYTES;

  private LeaseBudgetProfile() {}

  /** Reserved memory for the nested window and step accounts of one backfill cycle. */
  public static long eventBackfillMinimumMemory() {
    return Math.multiplyExact(2, EVENT_BACKFILL_MEMORY.total());
  }

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
    // HISTORY Revoke peaks at 3 operations / 2 scanners / 3 cleanup leases. A checkpoint scan
    // can overlap it and adds 1 / 1 / 1; the preceding control Get is sequential.
    long leaseMemory = Math.addExact(leaseRequest.total(), EXPIRY_WORKER_MEMORY.total());
    return new BudgetManager.ReservedCapacity(4, 0, 4, 3, 4, 0,
        Math.addExact(leaseMemory, eventBackfillMinimumMemory()));
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
