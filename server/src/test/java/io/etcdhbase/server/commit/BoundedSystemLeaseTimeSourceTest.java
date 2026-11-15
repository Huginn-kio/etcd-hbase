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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class BoundedSystemLeaseTimeSourceTest {
  private static final long UNCERTAINTY_MILLIS = 1_000;
  private static final int MAXIMUM_DRIFT_PPM = 100_000;

  @Test
  void returnsConservativeBoundsWhileWallAndMonotonicClocksAdvanceTogether() {
    var clocks = new MutableClocks(10_000);
    var source = source(clocks);

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
  void wallClockStepPausesTrustedTimeAndReportsOnceUntilTheClockReturns() {
    var clocks = new MutableClocks(10_000);
    var reasons = new ArrayList<BoundedSystemLeaseTimeSource.UnhealthyReason>();
    var source = source(clocks, reasons::add);
    LeaseTimeObservation initial = source.observe();
    assertTrue(initial.healthy());

    clocks.jumpWallClock(60_000);

    assertFalse(source.observe().healthy());
    clocks.advance(5_000);
    assertFalse(source.observe().healthy());
    assertEquals(List.of(
        BoundedSystemLeaseTimeSource.UnhealthyReason.WALL_MONOTONIC_DISCONTINUITY), reasons);

    clocks.jumpWallClock(-60_000);
    LeaseTimeObservation recovered = source.observe();
    assertTrue(recovered.healthy(), recovered.toString());
    assertEquals(initial.nowLowerEpochMillis() + 5_000, recovered.nowLowerEpochMillis());
    assertEquals(initial.nowUpperEpochMillis() + 5_000, recovered.nowUpperEpochMillis());

    clocks.jumpWallClock(60_000);
    assertFalse(source.observe().healthy());
    assertEquals(2, reasons.size());
    assertEquals(BoundedSystemLeaseTimeSource.UnhealthyReason.WALL_MONOTONIC_DISCONTINUITY,
        reasons.get(1));
  }

  @Test
  void backwardWallClockStepAlsoPausesUntilTheClockCatchesUp() {
    var clocks = new MutableClocks(60_000);
    var source = source(clocks);
    assertTrue(source.observe().healthy());

    clocks.jumpWallClock(-10_000);

    assertFalse(source.observe().healthy());

    clocks.advanceMonotonic(10_000);
    clocks.advanceWall(20_000);
    LeaseTimeObservation recovered = source.observe();
    assertTrue(recovered.healthy(), recovered.toString());
    assertEquals(69_000, recovered.nowLowerEpochMillis());
    assertEquals(71_000, recovered.nowUpperEpochMillis());
  }

  @Test
  void detectsCumulativeDriftAcrossManySmallWallClockChanges() {
    var clocks = new MutableClocks(10_000);
    var source = source(clocks);
    assertTrue(source.observe().healthy());

    for (int i = 0; i < 10; i++) {
      clocks.advanceMonotonic(100);
      clocks.advanceWall(150);
      assertTrue(source.observe().healthy());
    }
    assertTrue(source.observe().healthy());

    for (int i = 0; i < 41; i++) {
      clocks.advanceMonotonic(100);
      clocks.advanceWall(150);
    }

    LeaseTimeObservation finalObservation = source.observe();
    assertFalse(finalObservation.healthy(), finalObservation.toString());
  }

  @Test
  void clockReadFailureRecoversOnlyAfterASuccessfulTimeObservation() {
    var clocks = new MutableClocks(10_000);
    var source = source(clocks);
    assertTrue(source.observe().healthy());

    clocks.wall().setFailing(true);
    assertFalse(source.observe().healthy());

    clocks.wall().setFailing(false);
    clocks.advance(5_000);
    LeaseTimeObservation recovered = source.observe();
    assertTrue(recovered.healthy(), recovered.toString());
  }

  @Test
  void reportsInitializationClockFailureOnceAndRecoversFromAValidObservation() {
    var clocks = new MutableClocks(10_000);
    clocks.wall().setFailing(true);
    var reasons = new ArrayList<BoundedSystemLeaseTimeSource.UnhealthyReason>();
    var source = source(clocks, reasons::add);

    assertFalse(source.observe().healthy());
    assertFalse(source.observe().healthy());
    assertEquals(List.of(BoundedSystemLeaseTimeSource.UnhealthyReason.WALL_CLOCK_READ_FAILED),
        reasons);

    clocks.wall().setFailing(false);
    clocks.advance(5_000);
    assertTrue(source.observe().healthy());
    assertEquals(1, reasons.size());
  }

  @Test
  void rejectsMonotonicRegressionAndWallTimeOverflow() {
    var regressedClocks = new MutableClocks(10_000);
    var regressedSource = source(regressedClocks);
    assertTrue(regressedSource.observe().healthy());
    regressedClocks.monotonicNanos().set(-1);
    assertFalse(regressedSource.observe().healthy());
    regressedClocks.monotonicNanos().set(0);
    assertTrue(regressedSource.observe().healthy());

    var overflowClocks = new MutableClocks(Long.MAX_VALUE - 500);
    var overflowSource = source(overflowClocks);
    assertFalse(overflowSource.observe().healthy());
  }

  @Test
  void startsWithTheConfiguredWallClockUncertaintyBounds() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        clocks.monotonicNanos()::get);

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(9_999, observation.nowLowerEpochMillis());
    assertEquals(10_001, observation.nowUpperEpochMillis());
  }

  @Test
  void allowsWallMovementWithinTheConfiguredRateAndUncertaintyBounds() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        clocks.monotonicNanos()::get);
    assertTrue(source.observe().healthy());

    clocks.advance(1);
    clocks.advanceWall(1);

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(10_001, observation.nowLowerEpochMillis());
    assertEquals(10_003, observation.nowUpperEpochMillis());
  }

  @Test
  void failsClosedWhenWallClockMovementExceedsConfiguredBounds() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        clocks.monotonicNanos()::get);
    assertTrue(source.observe().healthy());

    clocks.advance(1);
    clocks.advanceWall(5);

    assertFalse(source.observe().healthy());
  }

  @Test
  void includesClockReadWindowInUpperBoundAtConfiguredLimit() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        monotonicWithReadWindow(1_000_000));

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(10_002, observation.nowUpperEpochMillis());
  }

  @Test
  void allowsOnlyDriftExplainedByMonotonicSamplingWindows() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        monotonicWithReadWindow(1_000_000));
    clocks.advanceWall(4);

    LeaseTimeObservation observation = source.observe();

    assertTrue(observation.healthy());
    assertEquals(10_003, observation.nowLowerEpochMillis());
  }

  @Test
  void rejectsClockReadWindowBeyondConfiguredLimit() {
    var clocks = new MutableClocks(10_000);
    var source = new BoundedSystemLeaseTimeSource(clocks.wall(), 1, MAXIMUM_DRIFT_PPM,
        monotonicWithReadWindow(1_000_001));

    assertFalse(source.observe().healthy());
  }

  @Test
  void runtimeReadinessBecomesFalseOnDiscontinuityAndTrueAfterRecovery() {
    var unavailableClocks = new MutableClocks(10_000);
    unavailableClocks.wall().setFailing(true);
    var unavailableSource = source(unavailableClocks);
    assertFalse(M3TableGroupRuntime.runtimeReady(true, Optional.of(unavailableSource)));

    unavailableClocks.wall().setFailing(false);
    assertTrue(M3TableGroupRuntime.runtimeReady(true, Optional.of(unavailableSource)));

    var clocks = new MutableClocks(10_000);
    var source = source(clocks);
    Optional<LeaseTimeSource> leaseTime = Optional.of(source);

    assertTrue(M3TableGroupRuntime.runtimeReady(true, leaseTime));
    clocks.jumpWallClock(60_000);
    assertFalse(M3TableGroupRuntime.runtimeReady(true, leaseTime));
    clocks.jumpWallClock(-60_000);
    assertTrue(M3TableGroupRuntime.runtimeReady(true, leaseTime));
    assertFalse(M3TableGroupRuntime.runtimeReady(false, Optional.empty()));
  }

  private static BoundedSystemLeaseTimeSource source(MutableClocks clocks) {
    return new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        MAXIMUM_DRIFT_PPM, clocks.monotonicNanos()::get);
  }

  private static BoundedSystemLeaseTimeSource source(MutableClocks clocks,
      java.util.function.Consumer<BoundedSystemLeaseTimeSource.UnhealthyReason> reporter) {
    return new BoundedSystemLeaseTimeSource(clocks.wall(), UNCERTAINTY_MILLIS,
        MAXIMUM_DRIFT_PPM, clocks.monotonicNanos()::get, reporter);
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
