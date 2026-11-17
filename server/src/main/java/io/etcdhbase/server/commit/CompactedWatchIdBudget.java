package io.etcdhbase.server.commit;

/** Process-wide bound for compacted Watch IDs retained until clients cancel them. */
final class CompactedWatchIdBudget {
  static final int MAX_PROCESS_IDS = 65_536;
  private static final CompactedWatchIdBudget PROCESS_WIDE =
      new CompactedWatchIdBudget(MAX_PROCESS_IDS);

  private final int maximumIds;
  private int retainedIds;

  CompactedWatchIdBudget(int maximumIds) {
    if (maximumIds < 0) throw new IllegalArgumentException("negative compacted Watch ID limit");
    this.maximumIds = maximumIds;
  }

  static CompactedWatchIdBudget processWide() { return PROCESS_WIDE; }

  synchronized boolean tryAcquire() {
    if (retainedIds >= maximumIds) return false;
    retainedIds++;
    return true;
  }

  synchronized void release(int count) {
    if (count < 0 || count > retainedIds) {
      throw new IllegalStateException("compacted Watch ID budget release is out of range");
    }
    retainedIds -= count;
  }

  synchronized int retainedIds() { return retainedIds; }
}
