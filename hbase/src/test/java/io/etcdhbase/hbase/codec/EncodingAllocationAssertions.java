package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

/** Checks rejection cost on the project's pinned Temurin runtime without exhausting the test JVM. */
final class EncodingAllocationAssertions {
  private EncodingAllocationAssertions() {}

  static void rejectsWithoutLargeCopy(Runnable encoding) {
    var allocation = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assertTrue(allocation.isThreadAllocatedMemorySupported());
    if (!allocation.isThreadAllocatedMemoryEnabled()) allocation.setThreadAllocatedMemoryEnabled(true);
    long threadId = Thread.currentThread().threadId();
    long before = allocation.getThreadAllocatedBytes(threadId);
    assertThrows(IllegalArgumentException.class, encoding::run);
    long after = allocation.getThreadAllocatedBytes(threadId);
    assertTrue(before >= 0 && after >= before, "thread allocation measurement must remain available");
    long allocated = after - before;
    // Exception/assertion overhead is allowed, but copying the 20 MiB fixture must fail this bound.
    assertTrue(allocated < 1024 * 1024, () -> "oversized input rejection allocated " + allocated + " bytes");
  }
}
