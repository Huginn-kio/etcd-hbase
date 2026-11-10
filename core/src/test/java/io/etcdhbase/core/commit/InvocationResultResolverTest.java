package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class InvocationResultResolverTest {
  @Test
  void resolvesLostTransitionAcknowledgementAndReplaysTheSamePersistedBytes() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.invocations.loseAcknowledgement = true;
      try (var first = fixture.resolve()) {
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(first));
      }
      assertEquals(InvocationBinding.State.SELECTED, fixture.invocations.binding.state());
      assertEquals(fixture.reference.token(), fixture.invocations.binding.resultReference().orElseThrow());
      try (var replay = fixture.resolve()) {
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(replay));
      }
      assertEquals(1, fixture.invocations.casCalls);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void refusesMissingResultWrongAuthorizationAndClosedQueryWindow() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.results.missingPage = true;
      assertThrows(CompletionException.class, fixture::resolve);
      assertEquals(InvocationBinding.State.SEALED, fixture.invocations.binding.state());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      byte[] wrongAuth = new byte[32];
      wrongAuth[0] = 1;
      assertThrows(CompletionException.class, () -> fixture.resolver.resolve(fixture.id, fixture.digest,
          ByteKey.copyOf(wrongAuth), fixture.scope, fixture.account).join());
      fixture.results.missingPage = false;
      fixture.invocations.closeAfterCas = true;
      assertThrows(CompletionException.class, fixture::resolve);
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, fixture.invocations.binding.state());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      assertThrows(CompletionException.class, fixture::resolve);
    }
  }

  private static byte[] bytes(InvocationResultResolver.ResolvedResult value) {
    var buffer = value.payload().buffer();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteKey digest = ByteKey.copyOf(new byte[32]);
    private final InvocationId id = InvocationId.create();
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final InvocationMemory invocations;
    private final ResultMemory results;
    private final ResultReference reference;
    private final InvocationResultResolver resolver;

    private Fixture() throws Exception {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
      InvocationBinding preparing = InvocationBinding.begin(id, digest, digest, TransactionId.create(), 1, 0, 1);
      ResultIdentity identity = ResultIdentity.from(preparing);
      ResultPage page = new ResultPage(identity, 0, new byte[] {1, 2, 3});
      ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
      reference = directory.reference();
      ManifestRoot root = ManifestRoot.createWithResult(identity.transactionId(), 1, 0, 1,
          0, 0, Optional.empty(), 0, 0, Optional.empty(), reference);
      invocations = new InvocationMemory(preparing.seal(root.systemIdentity()));
      results = new ResultMemory(directory, page);
      PublicationSlot slot = new PublicationSlot(identity.transactionId(), id, 1, 0, 1,
          1, 1, false, root.systemIdentity(), root.digest());
      CommitState finalized = CommitState.initial(digest, 1).claimOwner("owner", 0, 1000)
          .publish(slot).finalizePublication(identity.transactionId());
      CommitControlStore control = new CommitControlStore() {
        @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope ignored) {
          return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(finalized, new byte[] {1})));
        }
        @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope ignored) {
          throw new UnsupportedOperationException();
        }
        @Override public CompletableFuture<Boolean> compareAndSet(
            ControlSnapshot observed, CommitState state, RequestScope ignored) { throw new UnsupportedOperationException(); }
      };
      CommitDecision decision = CommitDecision.from(slot);
      CommitDecisionRepository decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope ignored) {
          return CompletableFuture.completedFuture(sequence == 1 ? Optional.of(decision) : Optional.empty());
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision value, RequestScope ignored) { throw new UnsupportedOperationException(); }
      };
      resolver = new InvocationResultResolver(invocations, new CommitOutcomeResolver(control, decisions),
          new ManifestCatalog(new RootMemory(root)), new ResultCatalog(results));
    }

    private InvocationResultResolver.ResolvedResult resolve() {
      return resolver.resolve(id, digest, digest, scope, account).join();
    }

    @Override public void close() { scope.close(); }
  }

  private static final class InvocationMemory implements InvocationRepository {
    private InvocationBinding binding;
    private boolean loseAcknowledgement;
    private boolean closeAfterCas;
    private int casCalls;
    private InvocationMemory(InvocationBinding binding) { this.binding = binding; }
    @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(binding));
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding binding, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      casCalls++;
      if (binding != expected) return CompletableFuture.completedFuture(false);
      binding = closeAfterCas ? replacement.closeQueryWindow() : replacement;
      if (loseAcknowledgement) {
        loseAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }

  private static final class ResultMemory implements ResultRepository {
    private final ResultDirectory directory;
    private final ResultPage page;
    private boolean missingPage;
    private ResultMemory(ResultDirectory directory, ResultPage page) { this.directory = directory; this.page = page; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage value, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      return CompletableFuture.completedFuture(missingPage ? Optional.empty() : Optional.of(page));
    }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(directory));
    }
  }

  private static final class RootMemory implements ManifestRepository {
    private final ManifestRoot root;
    private RootMemory(ManifestRoot root) { this.root = root; }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(root));
    }
    @Override public int candidateBucket(PreparedCandidate value) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(PreparedCandidate value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(ManifestDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(EventDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
  }
}
