package io.etcdhbase.server.config;

import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.scan.BudgetManager;

/** Memory reservations shared by admission and activation validation. */
public final class M3MemoryReservations {
  public static final int LEASE_BINDING_PAGE_ENTRIES = 128;
  public static final BudgetManager.Reservation REQUEST =
      new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 64L << 20);
  public static final BudgetManager.Reservation LEASE = new BudgetManager.Reservation(
      REQUEST.heads(), REQUEST.page(), Math.max(REQUEST.io(), ManifestCatalog.maximumProofReadPageBytes()),
      REQUEST.output(), REQUEST.candidates());
  public static final BudgetManager.Reservation RECOVERY =
      new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 8L << 20, 48L << 20);

  private M3MemoryReservations() {}

  public static long leaseBindingPageBytes(int maxRequestBytes) {
    return Math.addExact(maxRequestBytes, 64L << 10);
  }

  public static BudgetManager.Reservation leaseBindingRead(
      BudgetManager.Reservation base, int maxRequestBytes) {
    long page = LeaseBindingReader.requiredPageMemoryBytes(
        LEASE_BINDING_PAGE_ENTRIES, leaseBindingPageBytes(maxRequestBytes));
    return new BudgetManager.Reservation(base.heads(), Math.max(base.page(), page),
        base.io(), base.output(), base.candidates());
  }

  /** One idle recovery account per group and one Lease gateway/owner pair. */
  public static long minimumLeaseMemory(int groups, int maxRequestBytes) {
    if (groups < 1 || maxRequestBytes < 1) throw new IllegalArgumentException("invalid memory inputs");
    long gateway = leaseBindingRead(LEASE, maxRequestBytes).total();
    return Math.addExact(Math.multiplyExact(groups, RECOVERY.total()), Math.addExact(gateway, LEASE.total()));
  }
}
