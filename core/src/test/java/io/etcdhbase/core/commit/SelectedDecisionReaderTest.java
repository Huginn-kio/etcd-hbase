package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SelectedDecisionReaderTest {
  @Test
  void readsSelectedSlotBeforeArchiveAndFinalizedArchiveAfterward() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      assertEquals(fixture.decision, fixture.reader().read(1, scope, fixture.budget(scope, account))
          .join().orElseThrow());
      fixture.state = fixture.state.finalizePublication(fixture.decision.transactionId());
      assertThrows(CompletionException.class,
          () -> fixture.reader().read(1, scope, fixture.budget(scope, account)).join());
      fixture.archived = Optional.of(fixture.decision);
      assertEquals(fixture.decision, fixture.reader().read(1, scope, fixture.budget(scope, account))
          .join().orElseThrow());
      assertTrue(fixture.reader().read(2, scope, fixture.budget(scope, account)).join().isEmpty());
    }
  }

  @Test
  void rejectsConflictingArchiveForSelectedSlot() throws Exception {
    Fixture fixture = new Fixture();
    fixture.archived = Optional.of(new CommitDecision(fixture.decision.transactionId(),
        InvocationId.create(), 1, 1, 2, fixture.decision.manifestRoot(),
        fixture.decision.manifestDigest(), true));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      assertThrows(CompletionException.class,
          () -> fixture.reader().read(1, scope, fixture.budget(scope, account)).join());
    }
  }

  private static final class Fixture {
    private final ByteKey digest = ByteKey.copyOf(new byte[32]);
    private final ByteKey identity = ByteKey.copyOf(new byte[] {1});
    private final PublicationSlot slot = new PublicationSlot(TransactionId.create(),
        InvocationId.create(), 1, 0, 1, 1, 2, true, identity, digest);
    private final CommitDecision decision = CommitDecision.from(slot);
    private CommitState state = CommitState.initial(digest, 1).claimOwner("owner", 1, 100).publish(slot);
    private Optional<CommitDecision> archived = Optional.empty();

    private SelectedDecisionReader reader() {
      CommitControlStore control = new CommitControlStore() {
        @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
        }
        @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
        @Override public CompletableFuture<Boolean> compareAndSet(
            ControlSnapshot observed, CommitState replacement, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
      };
      CommitDecisionRepository decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
          return CompletableFuture.completedFuture(archived);
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision decision, RequestScope scope) { throw new UnsupportedOperationException(); }
      };
      return new SelectedDecisionReader(control, decisions);
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
      var manager = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
      var reads = new BudgetManager.ReadLimits(1_000_000, 8L << 30, 100_000, 65_536, 65_536);
      return manager.admit("public", scope, reservation, reads);
    }

    private ManifestCatalog.ProofReadBudget budget(RequestScope scope, BudgetManager.Account account) {
      return new ManifestCatalog.ProofReadBudget(scope, account, 2, 512L << 20);
    }
  }
}
