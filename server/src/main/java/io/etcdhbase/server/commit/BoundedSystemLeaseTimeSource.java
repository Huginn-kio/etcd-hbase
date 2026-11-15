package io.etcdhbase.server.commit;

import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import java.time.Clock;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Applies an operator-supplied wall-clock uncertainty bound and fails closed after a detected
 * discontinuity between the wall clock and a monotonic clock.
 *
 * <p>The configured bound is an operator assertion about the host clock. The monotonic comparison
 * detects jumps observed while this source is running; a deployment still has to validate clock
 * synchronization before starting or restarting a Lease-enabled runtime.
 */
final class BoundedSystemLeaseTimeSource implements LeaseTimeSource {
  private static final long NANOS_PER_MILLI = 1_000_000L;

  private final Clock clock;
  private final LongSupplier monotonicNanos;
  private final long uncertaintyMillis;
  private final Sample baselineSample;
  private Sample lastSample;
  private boolean healthy = true;

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis) {
    this(clock, uncertaintyMillis, System::nanoTime);
  }

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, LongSupplier monotonicNanos) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
    if (uncertaintyMillis <= 0 || uncertaintyMillis > 60_000) {
      throw new IllegalArgumentException("lease clock uncertainty must be between 1 and 60000 ms");
    }
    this.uncertaintyMillis = uncertaintyMillis;
    Sample initialSample = null;
    try {
      initialSample = sample();
    } catch (RuntimeException unhealthyClock) {
      this.healthy = false;
    }
    this.baselineSample = initialSample;
    this.lastSample = initialSample;
  }

  @Override
  public synchronized LeaseTimeObservation observe() {
    if (!healthy || lastSample == null) return unhealthy();
    try {
      Sample current = sample();
      if (!monotonicProgressed(lastSample, current)
          || !withinDriftBound(baselineSample, current)) {
        return markUnhealthy();
      }

      long lower = Math.max(0, Math.subtractExact(current.wallMillis(), uncertaintyMillis));
      long upper = Math.addExact(
          current.wallMillis(), Math.addExact(uncertaintyMillis, ceilMillis(current.readWindowNanos())));
      lastSample = current;
      return new LeaseTimeObservation(lower, upper, true);
    } catch (RuntimeException unhealthyClock) {
      return markUnhealthy();
    }
  }

  private Sample sample() {
    long startNanos = monotonicNanos.getAsLong();
    long wallMillis = clock.millis();
    long endNanos = monotonicNanos.getAsLong();
    long readWindowNanos = endNanos - startNanos;
    if (wallMillis < 0 || readWindowNanos < 0
        || ceilMillis(readWindowNanos) > uncertaintyMillis) {
      throw new IllegalStateException("lease clock sample is outside its configured bound");
    }
    long midpointNanos = startNanos + readWindowNanos / 2;
    return new Sample(wallMillis, midpointNanos, readWindowNanos);
  }

  private boolean monotonicProgressed(Sample previous, Sample current) {
    long elapsedNanos = current.monotonicMidpointNanos() - previous.monotonicMidpointNanos();
    return elapsedNanos >= 0;
  }

  private boolean withinDriftBound(Sample baseline, Sample current) {
    long elapsedNanos = current.monotonicMidpointNanos() - baseline.monotonicMidpointNanos();
    if (elapsedNanos < 0) return false;
    long elapsedMillis = elapsedNanos / NANOS_PER_MILLI;
    long wallElapsedMillis = Math.subtractExact(current.wallMillis(), baseline.wallMillis());
    long differenceMillis = Math.subtractExact(wallElapsedMillis, elapsedMillis);
    if (differenceMillis == Long.MIN_VALUE) return false;

    long samplingAllowance = Math.addExact(
        ceilHalfMillis(baseline.readWindowNanos()), ceilHalfMillis(current.readWindowNanos()));
    long maximumDifference = Math.addExact(uncertaintyMillis, samplingAllowance);
    return Math.abs(differenceMillis) <= maximumDifference;
  }

  private LeaseTimeObservation markUnhealthy() {
    healthy = false;
    lastSample = null;
    return unhealthy();
  }

  private static long ceilMillis(long nanos) {
    long millis = nanos / NANOS_PER_MILLI;
    return nanos % NANOS_PER_MILLI == 0 ? millis : Math.addExact(millis, 1);
  }

  private static long ceilHalfMillis(long nanos) {
    long halfNanos = nanos / 2 + nanos % 2;
    return ceilMillis(halfNanos);
  }

  private static LeaseTimeObservation unhealthy() {
    return new LeaseTimeObservation(0, 0, false);
  }

  private record Sample(long wallMillis, long monotonicMidpointNanos, long readWindowNanos) {}
}
