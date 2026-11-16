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

class EventIndexBackfillPlannerTest {
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[16]);
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 3);

  @Test
  void capturesAnEmptyHistoryWithoutInventingARevision() throws Exception {
    try (Fixture fixture = new Fixture(0, 1, 0, Map.of())) {
      var result = fixture.plan().join();
      assertEquals(1, result.sequence());
      assertEquals(0, result.upperSequence());
      assertEquals(1, result.upperRevision());
      assertTrue(result.complete());
    }
  }

  @Test
  void locatesFirstRetainedDecisionIncludingMetadataAtTheCompactBoundary() throws Exception {
    var entries = Map.of(1L, decision(1, 1, true), 2L, decision(2, 2, false),
        3L, decision(3, 2, true), 4L, decision(4, 3, false), 5L, decision(5, 3, true));
    try (Fixture fixture = new Fixture(5, 4, 3, entries)) {
      var result = fixture.plan().join();
      assertEquals(3, result.firstSequence());
      assertEquals(3, result.nextSequence());
      assertEquals(2, result.firstBaseRevision());
      assertEquals(5, result.upperSequence());
      assertEquals(4, result.upperRevision());
      assertEquals(6, result.sequence());
    }
  }

  @Test
  void refusesMissingHistoryAndWrongOwner() throws Exception {
    try (Fixture fixture = new Fixture(5, 4, 3, Map.of())) {
      assertThrows(CompletionException.class, () -> fixture.plan().join());
      assertThrows(CompletionException.class, () -> fixture.planner.plan(
          new CommitOwnerService.OwnerToken("old-owner", 2), REGISTRY, fixture.scope, fixture.budget()).join());
    }
  }

  private static CommitDecision decision(long sequence, long base, boolean changes) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), sequence, base,
        base + (changes ? 1 : 0), ByteKey.copyOf(new byte[] {1}), DIGEST, changes);
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final BudgetManager.Account account;
    private final EventIndexBackfillPlanner planner;

    private Fixture(long sequence, long revision, long compact, Map<Long, CommitDecision> entries) throws Exception {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
      var manager = new BudgetManager(limits, Map.of("group", limits), limits);
      account = manager.admit("group", scope, new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192),
          new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000));
      CommitState state = CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, DIGEST,
          Optional.of("owner"), 3, Long.MAX_VALUE, 1, sequence, revision, sequence, revision,
          Optional.empty(), compact, Optional.of(REGISTRY), Optional.empty(), 1);
      CommitControlStore control = new CommitControlStore() {
        @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope ignored) {
          return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
        }
        @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope ignored) {
          throw new AssertionError("unexpected initialize");
        }
        @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot expected, CommitState replacement,
            RequestScope ignored) { throw new AssertionError("unexpected compare and set"); }
      };
      CommitDecisionRepository decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long index, RequestScope ignored) {
          return CompletableFuture.completedFuture(Optional.ofNullable(entries.get(index)));
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision value, RequestScope ignored) { throw new AssertionError("unexpected create"); }
      };
      planner = new EventIndexBackfillPlanner(control, decisions);
    }

    private ManifestCatalog.ProofReadBudget budget() {
      return new ManifestCatalog.ProofReadBudget(scope, account, 64, 4L << 30);
    }

    private CompletableFuture<EventIndexBackfillCheckpoint> plan() {
      return planner.plan(OWNER, REGISTRY, scope, budget());
    }

    @Override public void close() { scope.close(); }
  }
}
