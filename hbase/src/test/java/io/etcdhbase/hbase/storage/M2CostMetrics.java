package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/** Application-port accounting for the public J29 MiniCluster workload. */
final class M2CostMetrics {
  record Value(long calls, long succeeded, long conflicts, long unknown, long failed,
      long inputBytes, long outputBytes, long elapsedNanos) {}

  private static final class Counter {
    private final LongAdder calls = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder conflicts = new LongAdder();
    private final LongAdder unknown = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder inputBytes = new LongAdder();
    private final LongAdder outputBytes = new LongAdder();
    private final LongAdder elapsedNanos = new LongAdder();

    Value snapshot() {
      return new Value(calls.sum(), succeeded.sum(), conflicts.sum(), unknown.sum(), failed.sum(),
          inputBytes.sum(), outputBytes.sum(), elapsedNanos.sum());
    }
  }

  private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();

  <T> CompletableFuture<T> observe(String name, long inputBytes,
      Supplier<CompletableFuture<T>> operation, java.util.function.ToLongFunction<T> outputBytes,
      java.util.function.Predicate<T> conflict) {
    Counter counter = counters.computeIfAbsent(name, ignored -> new Counter());
    counter.calls.increment();
    counter.inputBytes.add(Math.max(0, inputBytes));
    long started = System.nanoTime();
    CompletableFuture<T> future;
    try {
      future = operation.get();
      if (future == null) throw new IllegalStateException("measured operation returned a null future");
    } catch (RuntimeException | Error failure) {
      // Fatal synchronous failures still belong in the observation before they propagate.
      recordFailure(counter, failure, started);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return future.whenComplete((value, failure) -> {
      counter.elapsedNanos.add(Math.max(0, System.nanoTime() - started));
      if (failure != null) {
        classifyFailure(counter, failure);
      } else {
        counter.succeeded.increment();
        if (value != null) counter.outputBytes.add(Math.max(0, outputBytes.applyAsLong(value)));
        if (value != null && conflict.test(value)) counter.conflicts.increment();
      }
    });
  }

  private static void recordFailure(Counter counter, Throwable failure, long started) {
    counter.elapsedNanos.add(Math.max(0, System.nanoTime() - started));
    classifyFailure(counter, failure);
  }

  private static void classifyFailure(Counter counter, Throwable failure) {
    if (isUnknown(failure)) counter.unknown.increment();
    else counter.failed.increment();
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable current = CompletionFailures.unwrapCompletion(failure);
    if (!(current instanceof BackendFailure backend)) return false;
    return switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  Map<String, Value> snapshot() {
    var result = new java.util.LinkedHashMap<String, Value>();
    counters.entrySet().stream().sorted(Map.Entry.comparingByKey())
        .forEach(entry -> result.put(entry.getKey(), entry.getValue().snapshot()));
    return java.util.Collections.unmodifiableMap(result);
  }

  static final class Records implements io.etcdhbase.core.commit.SystemRecordStore {
    private final SystemRecordStore delegate;
    private final M2CostMetrics metrics;

    Records(SystemRecordStore delegate, M2CostMetrics metrics) {
      this.delegate = delegate;
      this.metrics = metrics;
    }

    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return metrics.observe("system." + type + ".read", 0,
          () -> delegate.read(type, identity, scope),
          value -> value.map(ByteKey::length).orElse(0), ignored -> false);
    }

    @Override public int bucket(SystemRecordType type, ByteKey identity) {
      return delegate.bucket(type, identity);
    }

    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      return metrics.observe("system." + type + ".create", value.length(),
          () -> delegate.createIfAbsent(type, identity, value, scope), ignored -> 0,
          result -> result == CreateResult.CONFLICT);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      long bytes = expected.map(ByteKey::length).orElse(0) + replacement.length();
      return metrics.observe("system." + type + ".cas", bytes,
          () -> delegate.compareAndSet(type, identity, expected, replacement, scope),
          ignored -> 0, updated -> !updated);
    }
  }

  static final class Control implements CommitControlStore {
    private final CommitControlStore delegate;
    private final M2CostMetrics metrics;

    Control(CommitControlStore delegate, M2CostMetrics metrics) {
      this.delegate = delegate;
      this.metrics = metrics;
    }

    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return metrics.observe("control.read", 0, () -> delegate.read(scope),
          value -> value.map(snapshot -> snapshot.comparisonToken().length).orElse(0),
          ignored -> false);
    }

    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState,
        RequestScope scope) {
      return metrics.observe("control.initialize", CommitControlMessages.encode(initialState).length,
          () -> delegate.initializeIfAbsent(initialState, scope), ignored -> 0, updated -> !updated);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed,
        CommitState replacement, RequestScope scope) {
      long bytes = observed.comparisonToken().length + CommitControlMessages.encode(replacement).length;
      return metrics.observe("control.cas", bytes,
          () -> delegate.compareAndSet(observed, replacement, scope), ignored -> 0,
          updated -> !updated);
    }
  }

  static final class Materializer implements MaterializationStore {
    private final MaterializationStore delegate;
    private final M2CostMetrics metrics;

    Materializer(MaterializationStore delegate, M2CostMetrics metrics) {
      this.delegate = delegate;
      this.metrics = metrics;
    }

    @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
      return metrics.observe("materialize.preflight", candidateBytes(candidate),
          () -> delegate.preflight(candidate, scope), ignored -> 0, ignored -> false);
    }

    @Override public CompletableFuture<ApplyResult> applySelected(
        io.etcdhbase.core.commit.CommitDecision decision, PreparedCandidate candidate,
        RequestScope scope) {
      return metrics.observe("materialize.apply", candidateBytes(candidate),
          () -> delegate.applySelected(decision, candidate, scope), ignored -> 0,
          result -> result == ApplyResult.CURRENT_SUPERSEDED);
    }

    private static long candidateBytes(PreparedCandidate candidate) {
      return (long) candidate.record().key().length()
          + candidate.record().value().map(value -> value.length).orElse(0);
    }
  }
}
