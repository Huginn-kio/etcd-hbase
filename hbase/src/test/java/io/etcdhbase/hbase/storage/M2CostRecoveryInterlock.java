package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/** Deterministically overlaps recovery materialization with one-row read pages in the J29 test. */
final class M2CostRecoveryInterlock implements MaterializationStore {
  private final MaterializationStore delegate;
  private final int maximumInterleavedApplications;
  private final CompletableFuture<Void> firstRecoveryApplication = new CompletableFuture<>();
  private final Map<Integer, CompletableFuture<Void>> pageWaiters = new TreeMap<>();
  private boolean recoveryInFlight;
  private int recoveryApplications;
  private int readerPages;
  private int readerPagesDuringRecovery;

  M2CostRecoveryInterlock(MaterializationStore delegate, int maximumInterleavedApplications) {
    this.delegate = delegate;
    if (maximumInterleavedApplications <= 0) {
      throw new IllegalArgumentException("recovery interleave must include at least one page");
    }
    this.maximumInterleavedApplications = maximumInterleavedApplications;
  }

  synchronized void recoveryStarted() {
    recoveryInFlight = true;
  }

  synchronized void recoveryCompleted() {
    recoveryInFlight = false;
  }

  CompletableFuture<Void> firstRecoveryApplication() {
    return firstRecoveryApplication.copy();
  }

  void readerPageCompleted() {
    List<CompletableFuture<Void>> ready = new ArrayList<>();
    synchronized (this) {
      readerPages++;
      if (recoveryInFlight) readerPagesDuringRecovery++;
      var completed = pageWaiters.entrySet().iterator();
      while (completed.hasNext()) {
        Map.Entry<Integer, CompletableFuture<Void>> waiter = completed.next();
        if (waiter.getKey() > readerPages) break;
        ready.add(waiter.getValue());
        completed.remove();
      }
    }
    ready.forEach(waiter -> waiter.complete(null));
  }

  synchronized int recoveryApplications() { return recoveryApplications; }
  synchronized int readerPagesDuringRecovery() { return readerPagesDuringRecovery; }
  synchronized boolean recoveryInFlight() { return recoveryInFlight; }

  @Override
  public CompletableFuture<ApplyResult> applySelected(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
    return delegate.applySelected(decision, candidate, scope).thenCompose(result -> {
      int applicationNumber;
      CompletableFuture<Void> pageBarrier;
      synchronized (this) {
        applicationNumber = ++recoveryApplications;
        pageBarrier = applicationNumber > maximumInterleavedApplications
            || readerPages >= applicationNumber
                ? CompletableFuture.completedFuture(null)
                : pageWaiters.computeIfAbsent(applicationNumber, ignored -> new CompletableFuture<>());
      }
      firstRecoveryApplication.complete(null);
      return pageBarrier.thenApply(ignored -> result);
    });
  }

  @Override
  public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
    return delegate.preflight(candidate, scope);
  }
}
