package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class BoundedSystemLeaseTimeSourceTest {
  private static final long UNCERTAINTY_MILLIS = 1_000;

  @Test
  void returnsConservativeBoundsWhileWallAndMonotonicClocksAdvanceTogether() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);

    LeaseTimeObservation initial = source.observe();
    assertTrue(initial.healthy());
    assertEquals(9_000, initial.nowLowerEpochMillis());
    assertEquals(11_000, initial.nowUpperEpochMillis());

    clocks.advance(5_000);

    LeaseTimeObservation advanced = source.observe();
    assertTrue(advanced.healthy());
    assertEquals(14_000, advanced.nowLowerEpochMillis());
    assertEquals(16_000, advanced.nowUpperEpochMillis());
  }

  @Test
  void failsClosedAndStaysUnhealthyAfterForwardWallClockJump() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);
    assertTrue(source.observe().healthy());

    clocks.jumpWallClock(60_000);

    assertFalse(source.observe().healthy());
    clocks.advance(5_000);
    assertFalse(source.observe().healthy());
  }

  @Test
  void failsClosedAfterBackwardWallClockJump() {
    var clocks = new MutableClocks(60_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);
    assertTrue(source.observe().healthy());

    clocks.jumpWallClock(-10_000);

    assertFalse(source.observe().healthy());
  }

  @Test
  void detectsCumulativeDriftAcrossManySmallWallClockChanges() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);

    for (int i = 0; i < 10; i++) {
      clocks.advanceMonotonic(100);
      clocks.advanceWall(150);
      assertTrue(source.observe().healthy());
    }
    assertTrue(source.observe().healthy());

    for (int i = 0; i < 11; i++) {
      clocks.advanceMonotonic(100);
      clocks.advanceWall(150);
    }

    assertFalse(source.observe().healthy());
  }

  @Test
  void failsClosedWhenClockReadThrowsAndDoesNotRecoverWithoutRevalidation() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);
    assertTrue(source.observe().healthy());

    clocks.wall().setFailing(true);
    assertFalse(source.observe().healthy());

    clocks.wall().setFailing(false);
    clocks.advance(5_000);
    assertFalse(source.observe().healthy());
  }

  @Test
  void failsClosedWhenUpperBoundOverflows() {
    var clocks = new MutableClocks(Long.MAX_VALUE - 500);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);

    assertFalse(source.observe().healthy());
  }

  @Test
  void includesClockReadWindowInUpperBoundAtConfiguredLimit() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1,
        monotonicWithReadWindow(1_000_000));

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(10_002, observation.nowUpperEpochMillis());
  }

  @Test
  void allowsOnlyDriftExplainedByMonotonicSamplingWindows() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1,
        monotonicWithReadWindow(1_000_000));
    clocks.advanceWall(4);

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(10_003, observation.nowLowerEpochMillis());
  }

  @Test
  void rejectsClockReadWindowBeyondConfiguredLimit() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1,
        monotonicWithReadWindow(1_000_001));

    assertFalse(source.observe().healthy());
  }

  @Test
  void runtimeReadinessBecomesFalseAfterLeaseClockDiscontinuity() {
    var unavailableClocks = new MutableClocks(10_000);
    unavailableClocks.wall().setFailing(true);
    var unavailableSource = new BoundedSystemLeaseTimeSource(unavailableClocks.wall(),
        UNCERTAINTY_MILLIS, unavailableClocks.monotonicNanos()::get);
    assertFalse(M3TableGroupRuntime.runtimeReady(true, Optional.of(unavailableSource)));

    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        clocks.monotonicNanos()::get);
    Optional<LeaseTimeSource> leaseTime = Optional.of(source);

    assertTrue(M3TableGroupRuntime.runtimeReady(true, leaseTime));
    clocks.jumpWallClock(60_000);
    assertFalse(M3TableGroupRuntime.runtimeReady(true, leaseTime));
    assertFalse(M3TableGroupRuntime.runtimeReady(false, Optional.empty()));
  }

  private static LongSupplier monotonicWithReadWindow(long readWindowNanos) {
    var monotonicNanos = new AtomicLong();
    return () -> monotonicNanos.getAndAdd(readWindowNanos);
  }

  private static final class MutableClocks {
    private final AtomicLong wallMillis;
    private final AtomicLong monotonicNanos = new AtomicLong();
    private final MutableClock wall;

    private MutableClocks(long wallMillis) {
      this.wallMillis = new AtomicLong(wallMillis);
      this.wall = new MutableClock(this.wallMillis);
    }

    private MutableClock wall() {
      return wall;
    }

    private AtomicLong monotonicNanos() {
      return monotonicNanos;
    }

    private void advance(long millis) {
      wallMillis.addAndGet(millis);
      advanceMonotonic(millis);
    }

    private void advanceMonotonic(long millis) {
      monotonicNanos.addAndGet(Math.multiplyExact(millis, 1_000_000L));
    }

    private void advanceWall(long millis) {
      wallMillis.addAndGet(millis);
    }

    private void jumpWallClock(long millis) {
      wallMillis.addAndGet(millis);
    }
  }

  private static final class MutableClock extends Clock {
    private final AtomicLong millis;
    private volatile boolean failing;

    private MutableClock(AtomicLong millis) {
      this.millis = millis;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis());
    }

    @Override
    public long millis() {
      if (failing) throw new IllegalStateException("clock unavailable");
      return millis.get();
    }

    private void setFailing(boolean failing) {
      this.failing = failing;
    }
  }
}
