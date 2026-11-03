package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/** Times the durable pin lifecycle around a real SnapshotPinManager. */
final class M2CostPinObservations implements ReadViewPinRegistry {
  private final ReadViewPinRegistry delegate;
  private final LongAdder registrations = new LongAdder();
  private final LongAdder registrationNanos = new LongAdder();
  private final LongAdder renewals = new LongAdder();
  private final LongAdder renewalNanos = new LongAdder();
  private final LongAdder completions = new LongAdder();
  private final LongAdder completionNanos = new LongAdder();
  private final AtomicInteger activePins = new AtomicInteger();
  private final AtomicInteger peakPins = new AtomicInteger();

  M2CostPinObservations(ReadViewPinRegistry delegate) { this.delegate = delegate; }

  @Override
  public CompletableFuture<Pin> register(ReadView view, RequestScope scope) {
    long started = System.nanoTime();
    registrations.increment();
    return delegate.register(view, scope).thenApply(pin -> {
      registrationNanos.add(System.nanoTime() - started);
      int active = activePins.incrementAndGet();
      peakPins.accumulateAndGet(active, Math::max);
      AtomicBoolean released = new AtomicBoolean();
      return (Pin) new Pin() {
        @Override public long generation() { return pin.generation(); }

        @Override public CompletableFuture<Void> renew(RequestScope renewScope) {
          return measure(renewals, renewalNanos, () -> pin.renew(renewScope));
        }

        @Override public CompletableFuture<Void> complete(RequestScope completeScope) {
          return measure(completions, completionNanos, () -> pin.complete(completeScope))
              .whenComplete((ignored, failure) -> { if (failure == null) release(); });
        }

        @Override public void close() {
          try {
            pin.close();
          } finally {
            release();
          }
        }

        private void release() {
          if (released.compareAndSet(false, true)) activePins.decrementAndGet();
        }
      };
    }).whenComplete((ignored, failure) -> {
      if (failure != null) registrationNanos.add(System.nanoTime() - started);
    });
  }

  long registrationCount() { return registrations.sum(); }
  long registrationTimeNanos() { return registrationNanos.sum(); }
  long renewalCount() { return renewals.sum(); }
  long renewalTimeNanos() { return renewalNanos.sum(); }
  long completionCount() { return completions.sum(); }
  long completionTimeNanos() { return completionNanos.sum(); }
  int activePins() { return activePins.get(); }
  int peakPins() { return peakPins.get(); }

  private static CompletableFuture<Void> measure(LongAdder count, LongAdder nanos,
      Supplier<CompletableFuture<Void>> operation) {
    long started = System.nanoTime();
    count.increment();
    CompletableFuture<Void> future;
    try {
      future = operation.get();
    } catch (RuntimeException | Error failure) {
      nanos.add(System.nanoTime() - started);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return future.whenComplete((ignored, failure) -> nanos.add(System.nanoTime() - started));
  }
}
