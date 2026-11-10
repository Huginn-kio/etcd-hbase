package io.etcdhbase.server.observability;

import static io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Full bounded-dimension counters, with rate-limited summaries for unexpected failures only. */
public final class RequestMetrics implements RequestCompletionRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestMetrics.class);
  private static final Pattern TRACE_ID =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  static final long WARNING_INTERVAL_NANOS = Duration.ofSeconds(30).toNanos();
  private final Map<Series, Counter> counters;
  private final Map<PutStage, StageCounter> putStages;
  private final LongAdder putOutputBytes = new LongAdder();
  private final LongSupplier nanoTime;
  private final Consumer<FailureSummary> warnings;
  private final AtomicLong lastWarningNanos;

  public RequestMetrics() {
    this(System::nanoTime, RequestMetrics::logSummary);
  }

  RequestMetrics(LongSupplier nanoTime, Consumer<FailureSummary> warnings) {
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.warnings = Objects.requireNonNull(warnings, "warnings");
    lastWarningNanos = new AtomicLong(nanoTime.getAsLong() - WARNING_INTERVAL_NANOS);
    Map<Series, Counter> initialized = new HashMap<>();
    for (MethodClass methodClass : MethodClass.values()) {
      for (Outcome outcome : Outcome.values())
        initialized.put(new Series(methodClass, outcome), new Counter());
    }
    counters = Map.copyOf(initialized);
    Map<PutStage, StageCounter> stages = new EnumMap<>(PutStage.class);
    for (PutStage stage : PutStage.values()) stages.put(stage, new StageCounter());
    putStages = Map.copyOf(stages);
  }

  /** Bounded stages that J30 actually observes; publication includes M2 finalization. */
  public enum PutStage { QUEUE, PREPARE, PUBLISH_AND_FINALIZE, RESULT_RESOLUTION, RESPONSE }

  public void recordPutStage(PutStage stage, long elapsedNanos) {
    Objects.requireNonNull(stage, "stage");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    var counter = putStages.get(stage);
    counter.count.increment();
    counter.totalNanos.add(elapsedNanos);
  }

  /** Counts protobuf response bytes accepted by onNext, excluding transport framing. */
  public void recordPutOutputBytes(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("output bytes must not be negative");
    putOutputBytes.add(bytes);
  }

  public long putOutputBytes() { return putOutputBytes.sum(); }

  public Map<PutStage, StageSnapshot> putStages() {
    Map<PutStage, StageSnapshot> snapshot = new EnumMap<>(PutStage.class);
    putStages.forEach((stage, counter) -> snapshot.put(stage,
        new StageSnapshot(counter.count.sum(), counter.totalNanos.sum())));
    return Map.copyOf(snapshot);
  }

  @Override
  public void recordCompletion(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      RequestScope scope,
      long elapsedNanos) {
    Objects.requireNonNull(methodClass, "methodClass");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(scope, "scope");
    if (traceId == null || !TRACE_ID.matcher(traceId).matches())
      throw new IllegalArgumentException("traceId must be a generated UUID");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    Counter counter = counters.get(new Series(methodClass, outcome));
    counter.completed.increment();
    counter.totalElapsedNanos.add(elapsedNanos);
    counter.cleanupFailures.add(scope.cleanupFailureCount());
    // Routine completions, including rejection/UNIMPLEMENTED, are represented by metrics.
    if (outcome != Outcome.BACKEND_FAILURE && scope.cleanupFailureCount() == 0) return;
    if (!reserveWarning()) return;
    Map<Series, Snapshot> totals = snapshot();
    long unexpected =
        totals.entrySet().stream()
            .filter(entry -> entry.getKey().outcome() == Outcome.BACKEND_FAILURE)
            .mapToLong(entry -> entry.getValue().completed())
            .sum();
    long cleanup = totals.values().stream().mapToLong(Snapshot::cleanupFailures).sum();
    long completed = totals.values().stream().mapToLong(Snapshot::completed).sum();
    warnings.accept(
        new FailureSummary(
            methodClass,
            outcome,
            traceId,
            elapsedNanos,
            scope.hasDeadline() ? scope.remainingDeadlineNanos() : -1,
            unexpected,
            cleanup,
            completed));
  }

  private boolean reserveWarning() {
    long now = nanoTime.getAsLong();
    long previous = lastWarningNanos.get();
    return now - previous >= WARNING_INTERVAL_NANOS
        && lastWarningNanos.compareAndSet(previous, now);
  }

  private static void logSummary(FailureSummary summary) {
    LOGGER.warn(
        "request_failure_summary method_class={} outcome={} elapsed_nanos={}"
            + " deadline_remaining_nanos={} trace_id={} unexpected_failure_total={}"
            + " cleanup_failure_total={} completed_total={}",
        summary.methodClass(),
        summary.outcome(),
        summary.elapsedNanos(),
        summary.remainingNanos(),
        summary.traceId(),
        summary.unexpectedFailures(),
        summary.cleanupFailures(),
        summary.completed());
  }

  public Map<Series, Snapshot> snapshot() {
    Map<Series, Snapshot> snapshot = new HashMap<>();
    counters.forEach(
        (series, counter) ->
            snapshot.put(
                series,
                new Snapshot(
                    counter.completed.sum(),
                    counter.totalElapsedNanos.sum(),
                    counter.cleanupFailures.sum())));
    return Map.copyOf(snapshot);
  }

  public record Series(MethodClass methodClass, Outcome outcome) {
    public Series {
      Objects.requireNonNull(methodClass, "methodClass");
      Objects.requireNonNull(outcome, "outcome");
    }
  }

  public record Snapshot(long completed, long totalElapsedNanos, long cleanupFailures) {}

  public record StageSnapshot(long count, long totalNanos) {}

  record FailureSummary(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      long elapsedNanos,
      long remainingNanos,
      long unexpectedFailures,
      long cleanupFailures,
      long completed) {}

  private static final class Counter {
    private final LongAdder cleanupFailures = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder totalElapsedNanos = new LongAdder();
  }

  private static final class StageCounter {
    private final LongAdder count = new LongAdder();
    private final LongAdder totalNanos = new LongAdder();
  }
}
