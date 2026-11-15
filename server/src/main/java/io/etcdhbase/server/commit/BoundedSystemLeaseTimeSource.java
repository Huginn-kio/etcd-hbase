package io.etcdhbase.server.commit;

import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import java.time.Clock;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedSystemLeaseTimeSource.class);

  private final Clock clock;
  private final LongSupplier monotonicNanos;
  private final long uncertaintyMillis;
  private final Consumer<UnhealthyReason> unhealthyReporter;
  private final Sample baselineSample;
  private Sample lastSample;
  private boolean healthy = true;

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis) {
    this(clock, uncertaintyMillis, System::nanoTime);
  }

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, LongSupplier monotonicNanos) {
    this(clock, uncertaintyMillis, monotonicNanos,
        BoundedSystemLeaseTimeSource::logUnhealthyTransition);
  }

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, LongSupplier monotonicNanos,
      Consumer<UnhealthyReason> unhealthyReporter) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
    this.unhealthyReporter = Objects.requireNonNull(unhealthyReporter, "unhealthyReporter");
    if (uncertaintyMillis <= 0 || uncertaintyMillis > 60_000) {
      throw new IllegalArgumentException("lease clock uncertainty must be between 1 and 60000 ms");
    }
    this.uncertaintyMillis = uncertaintyMillis;
    Sample initialSample = null;
    try {
      initialSample = sample();
    } catch (ClockSampleFailure failure) {
      markUnhealthy(failure.reason());
    } catch (ArithmeticException overflow) {
      markUnhealthy(UnhealthyReason.TIME_ARITHMETIC_OVERFLOW);
    } catch (RuntimeException unexpectedFailure) {
      markUnhealthy(UnhealthyReason.INITIALIZATION_FAILED);
    }
    this.baselineSample = initialSample;
    this.lastSample = initialSample;
  }

  @Override
  public synchronized LeaseTimeObservation observe() {
    if (!healthy || lastSample == null) return unhealthy();
    try {
      Sample current = sample();
      if (!monotonicProgressed(lastSample, current)) {
        return markUnhealthy(UnhealthyReason.MONOTONIC_CLOCK_REGRESSED);
      }
      if (!withinDriftBound(baselineSample, current)) {
        return markUnhealthy(UnhealthyReason.WALL_MONOTONIC_DISCONTINUITY);
      }

      long lower = Math.max(0, Math.subtractExact(current.wallMillis(), uncertaintyMillis));
      long upper = Math.addExact(
          current.wallMillis(), Math.addExact(uncertaintyMillis, ceilMillis(current.readWindowNanos())));
      lastSample = current;
      return new LeaseTimeObservation(lower, upper, true);
    } catch (ClockSampleFailure failure) {
      return markUnhealthy(failure.reason());
    } catch (ArithmeticException overflow) {
      return markUnhealthy(UnhealthyReason.TIME_ARITHMETIC_OVERFLOW);
    } catch (RuntimeException unexpectedFailure) {
      return markUnhealthy(UnhealthyReason.SAMPLE_FAILED);
    }
  }

  private Sample sample() {
    long startNanos = readMonotonicNanos();
    long wallMillis = readWallClockMillis();
    long endNanos = readMonotonicNanos();
    long readWindowNanos = endNanos - startNanos;
    if (wallMillis < 0 || readWindowNanos < 0
        || ceilMillis(readWindowNanos) > uncertaintyMillis) {
      throw new ClockSampleFailure(UnhealthyReason.SAMPLE_OUT_OF_BOUNDS);
    }
    long midpointNanos = startNanos + readWindowNanos / 2;
    return new Sample(wallMillis, midpointNanos, readWindowNanos);
  }

  private long readMonotonicNanos() {
    try {
      return monotonicNanos.getAsLong();
    } catch (RuntimeException unavailable) {
      throw new ClockSampleFailure(UnhealthyReason.MONOTONIC_CLOCK_READ_FAILED);
    }
  }

  private long readWallClockMillis() {
    try {
      return clock.millis();
    } catch (RuntimeException unavailable) {
      throw new ClockSampleFailure(UnhealthyReason.WALL_CLOCK_READ_FAILED);
    }
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

  private LeaseTimeObservation markUnhealthy(UnhealthyReason reason) {
    if (healthy) {
      healthy = false;
      lastSample = null;
      unhealthyReporter.accept(reason);
    }
    return unhealthy();
  }

  private static void logUnhealthyTransition(UnhealthyReason reason) {
    LOGGER.warn("Lease time source became unhealthy; reason={}", reason.code());
  }

  enum UnhealthyReason {
    INITIALIZATION_FAILED("initialization_failed"),
    WALL_CLOCK_READ_FAILED("wall_clock_read_failed"),
    MONOTONIC_CLOCK_READ_FAILED("monotonic_clock_read_failed"),
    SAMPLE_OUT_OF_BOUNDS("sample_out_of_bounds"),
    MONOTONIC_CLOCK_REGRESSED("monotonic_clock_regressed"),
    WALL_MONOTONIC_DISCONTINUITY("wall_monotonic_discontinuity"),
    TIME_ARITHMETIC_OVERFLOW("time_arithmetic_overflow"),
    SAMPLE_FAILED("sample_failed");

    private final String code;

    UnhealthyReason(String code) {
      this.code = code;
    }

    String code() {
      return code;
    }
  }

  private static final class ClockSampleFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final UnhealthyReason reason;

    private ClockSampleFailure(UnhealthyReason reason) {
      super(reason.code());
      this.reason = reason;
    }

    private UnhealthyReason reason() {
      return reason;
    }
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
