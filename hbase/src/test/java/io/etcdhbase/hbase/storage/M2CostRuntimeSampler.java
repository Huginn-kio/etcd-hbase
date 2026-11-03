package io.etcdhbase.hbase.storage;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Sparse runtime gauges sampled at workload boundaries, not continuous high-water telemetry. */
final class M2CostRuntimeSampler {
  private final AtomicLong peakHeapBytes = new AtomicLong();
  private final AtomicLong peakDirectBytes = new AtomicLong();
  private final AtomicLong peakThreadCount = new AtomicLong();

  void sample() {
    long heapBytes = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    long directBytes = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
        .filter(pool -> pool.getName().equals("direct"))
        .mapToLong(pool -> Math.max(0, pool.getMemoryUsed())).max().orElse(0);
    int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
    peakHeapBytes.accumulateAndGet(Math.max(0, heapBytes), Math::max);
    peakDirectBytes.accumulateAndGet(directBytes, Math::max);
    peakThreadCount.accumulateAndGet(threadCount, Math::max);
  }

  long peakHeapBytes() { return peakHeapBytes.get(); }
  long peakDirectBytes() { return peakDirectBytes.get(); }
  long peakThreadCount() { return peakThreadCount.get(); }
}
