package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventRevisionIndexSeekerTest {
  @Test
  void mergesBucketsAcrossBoundedStepsWithoutTreatingPartialResultsAsComplete() throws Exception {
    var repository = new Repository();
    repository.pages.put(0, page(11));
    repository.pages.put(1, page(8));
    repository.pages.put(2, new EventRevisionIndexPage(Optional.empty(), 0, 0, 1));
    var seeker = new EventRevisionIndexSeeker(repository, 3);
    try (var scope = RequestScope.withoutDeadline(); var account = account(scope)) {
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 4096);
      var first = seeker.step(seeker.start(5, scope), 2, 1024, scope, account, budget).join();
      assertFalse(seeker.complete(first));
      assertThrows(IllegalStateException.class, () -> seeker.candidate(first));
      var result = seeker.step(first, 2, 1024, scope, account, budget).join();
      assertTrue(seeker.complete(result));
      assertEquals(OptionalLong.of(8), seeker.candidate(result));
      assertEquals(3, budget.pages());
      assertEquals(32, budget.bytes());
    }
  }

  @Test
  void rejectsEarlierResultsAndExhaustedBudgets() throws Exception {
    var repository = new Repository();
    repository.pages.put(0, page(4));
    var seeker = new EventRevisionIndexSeeker(repository, 2);
    try (var scope = RequestScope.withoutDeadline(); var account = account(scope)) {
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 1, 4096);
      assertThrows(CompletionException.class,
          () -> seeker.step(seeker.start(5, scope), 1, 1024, scope, account, budget).join());
      assertThrows(CompletionException.class,
          () -> seeker.step(seeker.start(5, scope), 1, 1024, scope, account, budget).join());
    }
  }

  @Test
  void preventsResumingACursorInADifferentScopeOrWithAnotherAccount() throws Exception {
    var repository = new Repository();
    var seeker = new EventRevisionIndexSeeker(repository, 1);
    try (var scope = RequestScope.withoutDeadline(); var anotherScope = RequestScope.withoutDeadline();
        var account = account(scope); var otherAccount = account(scope)) {
      var cursor = seeker.start(5, scope);
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 1, 4096);
      assertThrows(CompletionException.class,
          () -> seeker.step(cursor, 1, 1024, scope, otherAccount, budget).join());
      var secondSeeker = new EventRevisionIndexSeeker(repository, 1);
      assertThrows(IllegalArgumentException.class, () -> secondSeeker.complete(cursor));
      assertThrows(CompletionException.class,
          () -> seeker.step(cursor, 1, 1024, anotherScope, account, budget).join());
    }
  }

  private static EventRevisionIndexPage page(long revision) {
    var identity = ByteKey.copyOf(new byte[] {1});
    var digest = ByteKey.copyOf(new byte[32]);
    var decision = new CommitDecision(TransactionId.create(), InvocationId.create(), 1,
        revision - 1, revision, identity, digest, true);
    var events = new EventReference(identity, digest, 0, 0, 1, 8, 1, 0);
    return new EventRevisionIndexPage(Optional.of(new EventRevisionIndex(decision, events)), 16, 1, 1);
  }

  private static final class Repository implements EventRevisionIndexRepository {
    private final Map<Integer, EventRevisionIndexPage> pages = new HashMap<>();

    @Override public CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        EventRevisionIndex index, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<EventRevisionIndexPage> firstAtOrAfter(
        int bucket, long revision, long maximumBytes, RequestScope scope) {
      return CompletableFuture.completedFuture(pages.get(bucket));
    }
  }

  private static BudgetManager.Account account(RequestScope scope) throws Exception {
    var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    var reads = new BudgetManager.ReadLimits(1_000_000, 8L << 30, 100_000, 65_536, 65_536);
    return manager.admit("public", scope, reservation, reads);
  }
}
