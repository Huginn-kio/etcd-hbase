package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.commit.SystemRecordPage;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class LeaseExpiryStepExecutorTest {
  @Test
  void scanUsesItsPinnedSequenceToCoverCandidatesWrittenAfterThePriorCheckpoint() {
    LeaseExpiryCheckpoint priorCheckpoint = new LeaseExpiryCheckpoint(1, 12, 4, 11, 100,
        LeaseExpiryCheckpoint.idleAt(100));
    long candidateSequence = priorCheckpoint.sequence();
    assertEquals(11, priorCheckpoint.protectedSequence());
    assertTrue(candidateSequence > priorCheckpoint.protectedSequence(),
        "a Lease mutation stores its preceding sequence as the checkpoint's protection fence");

    assertTrue(LeaseExpiryStepExecutor.candidateVisibleAtPinnedSequence(candidateSequence, 12),
        "a selected Lease mutation is visible at the scan's fixed M2 barrier");
    assertTrue(LeaseExpiryStepExecutor.candidateVisibleAtPinnedSequence(11, 12),
        "previously selected candidates remain covered");
    assertFalse(LeaseExpiryStepExecutor.candidateVisibleAtPinnedSequence(13, 12),
        "a candidate beyond the fixed barrier may still be unselected");
  }

  @Test
  void candidateSequenceCannotBeZeroOrUseANegativeBarrier() {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryStepExecutor.candidateVisibleAtPinnedSequence(0, 1));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryStepExecutor.candidateVisibleAtPinnedSequence(1, -1));
  }

  @Test
  void expiryPageBudgetRemainsReservedUntilThePhysicalScanDrains() throws BackendFailure {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      BudgetFixture budgets = new BudgetFixture(scope);
      var physicalScan = new CompletableFuture<Void>();
      scope.trackPhysicalProducer(physicalScan);
      CompletableFuture<SystemRecordPage> page = LeaseExpiryStepExecutor.withExpiryPageMemory(
          scope, budgets.account, () -> CompletableFuture.completedFuture(
              new SystemRecordPage(List.of(), Optional.empty(), true, 0, 0, 1)));
      page.join();

      scope.closeAsync(Runnable::run);
      assertEquals(LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES,
          budgets.manager.snapshot().allocatedBytes());

      physicalScan.complete(null);
      scope.cleanupCompletion().toCompletableFuture().join();
      scope.physicalProducersCompletion().toCompletableFuture().join();
      assertEquals(0, budgets.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void synchronousScanFailureReleasesPageBudgetAfterScopeDrain() throws BackendFailure {
    RequestScope scope = RequestScope.withoutDeadline();
    BudgetFixture budgets = new BudgetFixture(scope);
    CompletableFuture<SystemRecordPage> page = LeaseExpiryStepExecutor.withExpiryPageMemory(
        scope, budgets.account, () -> {
          throw new IllegalStateException("scan could not be started");
        });

    CompletionException failure = assertThrows(CompletionException.class, page::join);
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertEquals(LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES,
        budgets.manager.snapshot().allocatedBytes());

    LeaseExpiryWorker.closeAndDrain(scope, Runnable::run).join();
    assertEquals(0, budgets.manager.snapshot().allocatedBytes());
  }

  private static final class BudgetFixture {
    private final BudgetManager manager;
    private final BudgetManager.Account account;

    private BudgetFixture(RequestScope scope) throws BackendFailure {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 2, 0, 64L << 10);
      manager = new BudgetManager(limits, Map.of("expiry", limits), limits);
      account = manager.admit("expiry", scope,
          new BudgetManager.Reservation(0, LeaseExpiryStepExecutor.SCAN_PAGE_MEMORY_BYTES, 1, 1));
    }
  }
}
