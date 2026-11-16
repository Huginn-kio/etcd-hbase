package io.etcdhbase.server;

import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.commit.LeaseBudgetProfile;
import io.etcdhbase.server.commit.OwnerForwardingRuntime;
import io.etcdhbase.server.config.M3MemoryReservations;
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
    long ownerTransportMemory = activation.ownerForwarding() == null ? 0
        : OwnerForwardingRuntime.requestLimits(activation.memoryBytes(), activation.maxEtcdRequestBytes(),
            configuration.resources().maxConcurrentRequests()).reservedMemoryBytes();
    return create(configuration, activation, ownerTransportMemory);
  }

  static BudgetManager create(RuntimeConfiguration configuration, M3RuntimeConfiguration activation,
      long ownerTransportMemoryBytes) {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(activation, "activation");
    if (ownerTransportMemoryBytes < 0 || ownerTransportMemoryBytes >= activation.memoryBytes()) {
      throw new IllegalArgumentException("invalid owner transport memory reservation");
    }
    var resources = configuration.resources();
    var groups = configuration.listeners().stream().map(RuntimeConfiguration.Listener::tableGroupId)
        .distinct().toList();
    if (groups.isEmpty() || groups.size() > 64 || resources.maxConcurrentRequests() < 1
        || resources.maxConcurrentRequests() > 100_000 || resources.maxScannersPerTable() < 1
        || resources.maxScannersPerTable() > 1024 || resources.maxQueuedRequests() != 0) {
      throw new IllegalArgumentException("invalid M3 group or admission limits");
    }
    BudgetManager.ReservedCapacity perGroupReserve = activation.m4LeaseApiEnabled()
        ? LeaseBudgetProfile.perGroupCapacity(activation.maxEtcdRequestBytes())
        : BudgetManager.ReservedCapacity.none();
    BudgetManager.ReservedCapacity globalReserve = activation.m4LeaseApiEnabled()
        ? LeaseBudgetProfile.capacityForGroups(groups.size(), activation.maxEtcdRequestBytes())
        : BudgetManager.ReservedCapacity.none();
    Map<String, BudgetManager.ReservedCapacity> groupReserve = activation.m4LeaseApiEnabled()
        ? LeaseBudgetProfile.capacitiesFor(groups, activation.maxEtcdRequestBytes()) : Map.of();
    int requests = Math.addExact(
        Math.addExact(Math.multiplyExact(resources.maxConcurrentRequests(), 2), groups.size()),
        globalReserve.requests());
    int operations = Math.addExact(
        Math.addExact(Math.multiplyExact(resources.maxConcurrentRequests(), 4),
            Math.multiplyExact(groups.size(), 2)),
        globalReserve.operations());
    int scanners = Math.multiplyExact(
        Math.addExact(resources.maxScannersPerTable(), perGroupReserve.scanners()), groups.size());
    long memory = activation.memoryBytes() - ownerTransportMemoryBytes;
    if (activation.m4LeaseApiEnabled()) {
      long minimumLeaseMemory = Math.addExact(
          M3MemoryReservations.minimumLeaseMemory(groups.size(), activation.maxEtcdRequestBytes()),
          globalReserve.memoryBytes());
      if (memory < minimumLeaseMemory) {
        throw new IllegalArgumentException("M4 memory_bytes must leave at least " + minimumLeaseMemory
            + " after owner transport reservation for recovery accounts, Lease request/owner pairs, "
            + "and reserved expiry capacity");
      }
    }
    int cleanup = Math.addExact(operations, scanners);
    // This queue belongs to finite-page scans within already admitted requests. It does not
    // enable an external mutation request queue (which remains disabled for this runtime).
    long scanQueueBytes = Math.min(memory, Math.multiplyExact((long) requests,
        Math.addExact(256L, Math.multiplyExact(3L, resources.maxRequestBytes()))));
    var global = new BudgetManager.Limits(requests, requests, operations, scanners, cleanup, scanQueueBytes, memory);
    var perGroup = new BudgetManager.Limits(requests, requests, operations,
        Math.addExact(resources.maxScannersPerTable(), perGroupReserve.scanners()),
        cleanup, scanQueueBytes, memory);
    Map<String, BudgetManager.Limits> groupLimits = new LinkedHashMap<>();
    for (String group : groups) groupLimits.put(group, perGroup);
    return new BudgetManager(global, groupLimits, perGroup, globalReserve, groupReserve);
  }
}
