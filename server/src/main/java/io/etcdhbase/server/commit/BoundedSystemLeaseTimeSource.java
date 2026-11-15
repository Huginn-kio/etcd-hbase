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
 * Bounds wall time by configured absolute uncertainty and elapsed monotonic time.
 *
 * <p>The configured absolute and rate bounds are deployment assumptions. This local source can
 * detect inconsistent clock movement after its first observation, but cannot prove that the
 * initial wall-clock reading is within the configured absolute uncertainty.
 */
final class BoundedSystemLeaseTimeSource implements LeaseTimeSource {
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final long PARTS_PER_MILLION = 1_000_000L;
  private static final int MAX_CLOCK_DRIFT_PPM = 100_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(BoundedSystemLeaseTimeSource.class);

  private final Clock clock;
  private final long uncertaintyMillis;
  private final int maximumDriftPpm;
  private final LongSupplier monotonicNanos;
  private final Consumer<UnhealthyReason> unhealthyReporter;

  private boolean initialized;
  private long lastTrustedMonotonicNanos;
  private EpochBound trustedLowerBound;
  private EpochBound trustedUpperBound;
  private UnhealthyReason reportedUnhealthyReason;

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, int maximumDriftPpm) {
    this(clock, uncertaintyMillis, maximumDriftPpm, System::nanoTime);
  }

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, int maximumDriftPpm,
      LongSupplier monotonicNanos) {
    this(clock, uncertaintyMillis, maximumDriftPpm, monotonicNanos,
        BoundedSystemLeaseTimeSource::logUnhealthyTransition);
  }

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis, int maximumDriftPpm,
      LongSupplier monotonicNanos, Consumer<UnhealthyReason> unhealthyReporter) {
    this.clock = Objects.requireNonNull(clock, "clock");
    this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
    this.unhealthyReporter = Objects.requireNonNull(unhealthyReporter, "unhealthyReporter");
    if (uncertaintyMillis <= 0 || uncertaintyMillis > 60_000
        || maximumDriftPpm < 1 || maximumDriftPpm > MAX_CLOCK_DRIFT_PPM) {
      throw new IllegalArgumentException("invalid Lease clock uncertainty or rate-drift bound");
    }
    this.uncertaintyMillis = uncertaintyMillis;
    this.maximumDriftPpm = maximumDriftPpm;
  }

  @Override
  public synchronized LeaseTimeObservation observe() {
    try {
      Sample current = sample();
      EpochBound wallLower = wallLower(current);
      EpochBound wallUpper = wallUpper(current);
      if (!initialized) {
        trustedLowerBound = wallLower;
        trustedUpperBound = wallUpper;
        lastTrustedMonotonicNanos = current.monotonicEndNanos();
        initialized = true;
        return markHealthy(wallLower, wallUpper);
      }

      long elapsedNanos = current.monotonicEndNanos() - lastTrustedMonotonicNanos;
      if (elapsedNanos < 0) {
        return markUnhealthy(UnhealthyReason.MONOTONIC_CLOCK_REGRESSED);
      }
      long driftAllowanceNanos = driftAllowanceNanos(elapsedNanos);
      EpochBound projectedLower = trustedLowerBound.plusNanos(
          Math.subtractExact(elapsedNanos, driftAllowanceNanos));
      EpochBound projectedUpper = trustedUpperBound.plusNanos(
          Math.addExact(elapsedNanos, driftAllowanceNanos));
      EpochBound lower = EpochBound.max(trustedLowerBound,
          EpochBound.max(projectedLower, wallLower));
      EpochBound upper = EpochBound.min(projectedUpper, wallUpper);
      if (lower.compareTo(upper) > 0) {
        return markUnhealthy(UnhealthyReason.WALL_MONOTONIC_DISCONTINUITY);
      }

      trustedLowerBound = lower;
      trustedUpperBound = upper;
      lastTrustedMonotonicNanos = current.monotonicEndNanos();
      return markHealthy(lower, upper);
    } catch (ClockSampleFailure failure) {
      return markUnhealthy(failure.reason());
    } catch (ArithmeticException overflow) {
      return markUnhealthy(UnhealthyReason.TIME_ARITHMETIC_OVERFLOW);
    } catch (RuntimeException unexpectedFailure) {
      return markUnhealthy(UnhealthyReason.SAMPLE_FAILED);
    }
  }

  private Sample sample() {
    long monotonicStart = readMonotonicNanos();
    long wallMillis = readWallClockMillis();
    long monotonicEnd = readMonotonicNanos();
    long readWindowNanos = monotonicEnd - monotonicStart;
    if (wallMillis < 0 || readWindowNanos < 0
        || ceilMillis(readWindowNanos) > uncertaintyMillis) {
      throw new ClockSampleFailure(UnhealthyReason.SAMPLE_OUT_OF_BOUNDS);
    }
    return new Sample(wallMillis, monotonicEnd, readWindowNanos);
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

  private EpochBound wallLower(Sample sample) {
    long lowerMillis = Math.max(0, Math.subtractExact(sample.wallMillis(), uncertaintyMillis));
    return EpochBound.atMillis(lowerMillis);
  }

  private EpochBound wallUpper(Sample sample) {
    long upperMillis = Math.addExact(sample.wallMillis(), uncertaintyMillis);
    return EpochBound.atMillis(upperMillis).plusNanos(sample.readWindowNanos());
  }

  private long driftAllowanceNanos(long elapsedNanos) {
    long wholeUnits = Math.multiplyExact(elapsedNanos / PARTS_PER_MILLION, maximumDriftPpm);
    long fractionalProduct = Math.multiplyExact(
        elapsedNanos % PARTS_PER_MILLION, (long) maximumDriftPpm);
    long fractionalUnits = fractionalProduct == 0 ? 0
        : Math.floorDiv(Math.addExact(fractionalProduct, PARTS_PER_MILLION - 1),
            PARTS_PER_MILLION);
    long calculatedAllowance = Math.addExact(wholeUnits, fractionalUnits);
    long configuredUncertaintyNanos = Math.multiplyExact(uncertaintyMillis, NANOS_PER_MILLI);
    return Math.min(calculatedAllowance, configuredUncertaintyNanos);
  }

  private LeaseTimeObservation markHealthy(EpochBound lower, EpochBound upper) {
    reportedUnhealthyReason = null;
    return new LeaseTimeObservation(lower.floorEpochMillis(), upper.ceilEpochMillis(), true);
  }

  private LeaseTimeObservation markUnhealthy(UnhealthyReason reason) {
    if (reportedUnhealthyReason == null) {
      try {
        unhealthyReporter.accept(reason);
      } catch (RuntimeException reportingFailure) {
        LOGGER.warn("Lease time source became unhealthy; reason={}, reporterFailure={}",
            reason.code(), reportingFailure.getClass().getSimpleName());
      }
    }
    reportedUnhealthyReason = reason;
    return unhealthy();
  }

  private static void logUnhealthyTransition(UnhealthyReason reason) {
    LOGGER.warn("Lease time source became unhealthy; reason={}", reason.code());
  }

  enum UnhealthyReason {
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

  private static LeaseTimeObservation unhealthy() {
    return new LeaseTimeObservation(0, 0, false);
  }

  /** Millisecond epoch plus a normalized sub-millisecond offset; avoids epoch-nanos overflow. */
  private record EpochBound(long epochMillis, int nanosWithinMillis)
      implements Comparable<EpochBound> {
    private EpochBound {
      if (nanosWithinMillis < 0 || nanosWithinMillis >= NANOS_PER_MILLI) {
        throw new IllegalArgumentException("sub-millisecond offset is not normalized");
      }
    }

    private static EpochBound atMillis(long epochMillis) {
      return new EpochBound(epochMillis, 0);
    }

    private EpochBound plusNanos(long deltaNanos) {
      long millisDelta = Math.floorDiv(deltaNanos, NANOS_PER_MILLI);
      int nanosDelta = (int) Math.floorMod(deltaNanos, NANOS_PER_MILLI);
      long nextMillis = Math.addExact(epochMillis, millisDelta);
      int nextNanos = nanosWithinMillis + nanosDelta;
      if (nextNanos >= NANOS_PER_MILLI) {
        nextMillis = Math.addExact(nextMillis, 1);
        nextNanos -= (int) NANOS_PER_MILLI;
      }
      return new EpochBound(nextMillis, nextNanos);
    }

    private long floorEpochMillis() {
      return epochMillis;
    }

    private long ceilEpochMillis() {
      return nanosWithinMillis == 0 ? epochMillis : Math.addExact(epochMillis, 1);
    }

    private static EpochBound max(EpochBound left, EpochBound right) {
      return left.compareTo(right) >= 0 ? left : right;
    }

    private static EpochBound min(EpochBound left, EpochBound right) {
      return left.compareTo(right) <= 0 ? left : right;
    }

    @Override
    public int compareTo(EpochBound other) {
      int millisecondOrder = Long.compare(epochMillis, other.epochMillis);
      return millisecondOrder != 0
          ? millisecondOrder : Integer.compare(nanosWithinMillis, other.nanosWithinMillis);
    }
  }

  private record Sample(long wallMillis, long monotonicEndNanos, long readWindowNanos) {}
}
