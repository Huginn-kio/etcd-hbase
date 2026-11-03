package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CommitFinalizationServiceTest {
  @Test
  void archivesAndFinalizesSelectedMetadataOnlyCommitIdempotently() throws Exception {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      var result = fixture.service.finish(fixture.invocation, pending, scope).get();

      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
      assertEquals(1, fixture.decisions.createCalls.get());
      assertEquals(2, fixture.decisions.readCalls.get());
      assertEquals(0, fixture.materializationCalls.get());
    }
  }

  @Test
  void conflictingDecisionArchiveCannotAdvanceFinalizedSequence() {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.CONFLICT);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());

      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());
      assertEquals(0, fixture.decisions.readCalls.get());
    }
  }

  private static final class Fixture {
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final ManifestRoot root = ManifestRoot.create(transactionId, 1, 0, 1, 0, 0,
        Optional.empty());
    private final InvocationBinding invocation = InvocationBinding.begin(invocationId,
        repeatedDigest(), repeatedDigest(), transactionId, 1, 0, 1).seal(root.systemIdentity());
    private final CommitDecision decision = new CommitDecision(transactionId, invocationId,
        1, 1, 1, root.systemIdentity(), root.digest(), false);
    private final MemoryControl control = new MemoryControl();
    private final MemoryDecisions decisions;
    private final AtomicInteger materializationCalls = new AtomicInteger();
    private final CommitFinalizationService service;

    private Fixture(SystemRecordStore.CreateResult archiveResult) {
      CommitState state = CommitState.initial(repeatedDigest(), 1)
          .claimOwner("fixture-owner", 1, 100);
      PublicationSlot slot = new PublicationSlot(transactionId, invocationId, 1,
          0, 1, 1, 1, false, root.systemIdentity(), root.digest());
      control.state = state.publish(slot);
      decisions = new MemoryDecisions(decision, archiveResult);
      ManifestRepository repository = new EmptyManifestRepository(root);
      ManifestCatalog catalog = new ManifestCatalog(repository);
      MaterializationStore materialization = new MaterializationStore() {
        @Override
        public CompletableFuture<ApplyResult> applySelected(
            CommitDecision selected, PreparedCandidate candidate, RequestScope scope) {
          materializationCalls.incrementAndGet();
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }

        @Override
        public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
          return CompletableFuture.completedFuture(null);
        }
      };
      service = new CommitFinalizationService(control,
          new CommitOutcomeResolver(control, decisions), decisions, catalog, materialization);
    }
  }

  private static final class MemoryControl implements CommitControlStore {
    private CommitState state;
    private int version;

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(snapshot()));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (!java.util.Arrays.equals(observed.comparisonToken(), token())) {
        return CompletableFuture.completedFuture(false);
      }
      state = replacement;
      version++;
      return CompletableFuture.completedFuture(true);
    }

    private ControlSnapshot snapshot() {
      return new ControlSnapshot(state, token());
    }

    private byte[] token() {
      return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(version).array();
    }
  }

  private static final class MemoryDecisions implements CommitDecisionRepository {
    private final CommitDecision decision;
    private final SystemRecordStore.CreateResult createResult;
    private final AtomicInteger createCalls = new AtomicInteger();
    private final AtomicInteger readCalls = new AtomicInteger();

    private MemoryDecisions(CommitDecision decision, SystemRecordStore.CreateResult createResult) {
      this.decision = decision;
      this.createResult = createResult;
    }

    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      readCalls.incrementAndGet();
      return CompletableFuture.completedFuture(sequence == decision.sequence()
          ? Optional.of(decision) : Optional.empty());
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision value, RequestScope scope) {
      createCalls.incrementAndGet();
      return CompletableFuture.completedFuture(createResult);
    }
  }

  private static final class EmptyManifestRepository implements ManifestRepository {
    private final ManifestRoot root;

    private EmptyManifestRepository(ManifestRoot root) {
      this.root = root;
    }

    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(identity.equals(root.systemIdentity())
          ? Optional.of(root) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }

    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }

  private static ByteKey repeatedDigest() {
    return ByteKey.copyOf(new byte[32]);
  }
}
