package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class RecoveryManagerTest {
  @Test
  void takesOverSelectedSlotAndRepeatedStartupDoesNotAllocateAnotherRevision() throws Exception {
    Fixture fixture = new Fixture(false);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      RecoveryManager.Result recovered = fixture.manager.recover("recovery-owner", 200, 500, scope).get();

      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      assertEquals(2, recovered.owner().orElseThrow().epoch());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertEquals(7, fixture.control.state.finalizedRevision());
      assertEquals(Optional.empty(), fixture.control.state.publication());

      RecoveryManager.Result restarted = fixture.manager.recover("recovery-owner", 250, 600, scope).get();

      assertEquals(RecoveryManager.State.NO_PENDING_PUBLICATION, restarted.state());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertEquals(7, fixture.control.state.finalizedRevision());
      assertEquals(1, fixture.invocations.reads);
    }
  }

  @Test
  void recoveryFinalizesSelectedWorkAfterCallerResultWindowCloses() throws Exception {
    Fixture fixture = new Fixture(true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertEquals(CommitOutcomeResolver.State.WINDOW_CLOSED,
          fixture.outcomes.resolve(fixture.invocation, scope).get().state());

      RecoveryManager.Result recovered = fixture.manager.recover("recovery-owner", 200, 500, scope).get();

      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertEquals(7, fixture.control.state.finalizedRevision());
    }
  }

  @Test
  void recoveryResumesAfterTheDecisionArchiveWasPersisted() throws Exception {
    Fixture fixture = new Fixture(false);
    CommitFinalizer failOnce = new CommitFinalizer() {
      private boolean failed;

      @Override
      public CompletableFuture<CommitOutcomeResolver.Resolution> finish(
          InvocationBinding invocation, CommitOutcomeResolver.Resolution publication,
          RequestScope serviceScope) {
        if (!failed) {
          failed = true;
          CommitDecision selected = publication.decision().orElseThrow();
          return fixture.decisions.createIfAbsent(selected, serviceScope).thenCompose(ignored ->
              CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
        }
        return fixture.finalizer.finish(invocation, publication, serviceScope);
      }
    };
    RecoveryManager retryable = fixture.manager(failOnce);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertThrows(CompletionException.class,
          () -> retryable.recover("recovery-owner", 200, 500, scope).join());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertEquals(7, fixture.control.state.finalizedRevision());
      assertEquals(1, fixture.control.state.publication().orElseThrow().sequence());
      assertEquals(fixture.root.systemIdentity(), fixture.decisions.values.get(1L).manifestRoot());

      RecoveryManager.Result recovered = retryable.recover("recovery-owner", 250, 600, scope).get();

      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertEquals(Optional.empty(), fixture.control.state.publication());
    }
  }

  @Test
  void sealedButUnpublishedWorkStaysPrivateAfterOldOwnerIsFenced() throws Exception {
    Fixture fixture = new Fixture(false, 150, false);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      RecoveryManager.Result recovered = fixture.manager.recover("recovery-owner", 200, 500, scope).get();

      assertEquals(RecoveryManager.State.NO_PENDING_PUBLICATION, recovered.state());
      assertEquals(2, recovered.owner().orElseThrow().epoch());
      assertEquals(0, fixture.control.state.publishedSequence());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertEquals(7, fixture.control.state.publishedRevision());
      assertEquals(7, fixture.control.state.finalizedRevision());
      assertEquals(0, fixture.invocations.reads);
    }
  }

  @Test
  void doesNotPreemptAnotherLiveOwnerOrScanInvocations() throws Exception {
    Fixture fixture = new Fixture(false, 500);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      RecoveryManager.Result result = fixture.manager.recover("other-owner", 100, 300, scope).get();

      assertEquals(RecoveryManager.State.OWNER_ACTIVE, result.state());
      assertEquals(1, fixture.control.state.ownerEpoch());
      assertEquals(1, fixture.control.state.publishedSequence());
      assertEquals(0, fixture.invocations.reads);
    }
  }

  @Test
  void rejectsProfileMismatchBeforeClaimingAnOwnerEpoch() {
    Fixture fixture = new Fixture(false);
    RecoveryManager mismatched = new RecoveryManager(repeatedDigest(8), fixture.control,
        new CommitOwnerService(fixture.control), fixture.invocations, fixture.outcomes, fixture.finalizer);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      IllegalStateException failure = assertInstanceOf(IllegalStateException.class,
          assertThrows(CompletionException.class,
              () -> mismatched.recover("recovery-owner", 200, 500, scope).join()).getCause());

      assertEquals("M2 control profile differs from the recovery configuration", failure.getMessage());
      assertEquals(1, fixture.control.state.ownerEpoch());
      assertEquals(0, fixture.invocations.reads);
    }
  }

  @Test
  void missingSelectedInvocationFailsClosedAfterOwnerFence() {
    Fixture fixture = new Fixture(false);
    fixture.invocations.binding = null;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      IllegalStateException failure = assertInstanceOf(IllegalStateException.class,
          assertThrows(CompletionException.class,
              () -> fixture.manager.recover("recovery-owner", 200, 500, scope).join()).getCause());

      assertEquals("selected publication has no durable invocation binding", failure.getMessage());
      assertEquals(2, fixture.control.state.ownerEpoch());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertEquals(1, fixture.control.state.publication().orElseThrow().sequence());
    }
  }

  private static final class Fixture {
    private final ByteKey profile = repeatedDigest(3);
    private final TransactionId transaction = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final ManifestRoot root = ManifestRoot.create(transaction, 1, 0, 7,
        0, 0, Optional.empty());
    private final InvocationBinding invocation;
    private final MemoryControl control;
    private final MemoryInvocations invocations;
    private final MemoryDecisions decisions = new MemoryDecisions();
    private final CommitOutcomeResolver outcomes;
    private final CommitFinalizationService finalizer;
    private final RecoveryManager manager;

    private Fixture(boolean closedWindow) {
      this(closedWindow, 150);
    }

    private Fixture(boolean closedWindow, long oldOwnerDeadline) {
      this(closedWindow, oldOwnerDeadline, true);
    }

    private Fixture(boolean closedWindow, long oldOwnerDeadline, boolean publish) {
      InvocationBinding sealed = InvocationBinding.begin(invocationId, repeatedDigest(4),
          repeatedDigest(5), transaction, 1, 0, 7).seal(root.systemIdentity());
      invocation = closedWindow ? sealed.selected(repeatedDigest(6)).closeQueryWindow() : sealed;
      PublicationSlot slot = new PublicationSlot(transaction, invocationId, 1,
          0, 7, 1, 7, false, root.systemIdentity(), root.digest());
      CommitState claimed = CommitState.initial(profile, 7)
          .claimOwner("old-owner", 100, oldOwnerDeadline);
      CommitState selected = publish ? claimed.publish(slot) : claimed;
      control = new MemoryControl(selected);
      invocations = new MemoryInvocations(invocation);
      outcomes = new CommitOutcomeResolver(control, decisions);
      ManifestCatalog manifests = new ManifestCatalog(new EmptyManifestRepository(root));
      MaterializationStore materialization = new MaterializationStore() {
        @Override
        public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }

        @Override
        public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
          return CompletableFuture.completedFuture(null);
        }
      };
      finalizer = new CommitFinalizationService(
          control, outcomes, decisions, manifests, materialization);
      manager = manager(finalizer);
    }

    private RecoveryManager manager(CommitFinalizer commitFinalizer) {
      return new RecoveryManager(profile, control, new CommitOwnerService(control),
          invocations, outcomes, commitFinalizer);
    }
  }

  private static final class MemoryControl implements CommitControlStore {
    private CommitState state;
    private int version;

    private MemoryControl(CommitState state) { this.state = state; }

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

    private ControlSnapshot snapshot() { return new ControlSnapshot(state, token()); }
    private byte[] token() { return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(version).array(); }
  }

  private static final class MemoryInvocations implements InvocationRepository {
    private InvocationBinding binding;
    private int reads;

    private MemoryInvocations(InvocationBinding binding) { this.binding = binding; }

    @Override
    public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
      reads++;
      return CompletableFuture.completedFuture(binding != null && binding.invocationId().equals(id)
          ? Optional.of(binding) : Optional.empty());
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding value, RequestScope scope) {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.ALREADY_PRESENT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      if (binding == null || !binding.equals(expected)) return CompletableFuture.completedFuture(false);
      binding = replacement;
      return CompletableFuture.completedFuture(true);
    }
  }

  private static final class MemoryDecisions implements CommitDecisionRepository {
    private final Map<Long, CommitDecision> values = new HashMap<>();

    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(sequence)));
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision decision, RequestScope scope) {
      CommitDecision previous = values.putIfAbsent(decision.sequence(), decision);
      return CompletableFuture.completedFuture(previous == null
          ? SystemRecordStore.CreateResult.CREATED
          : previous.equals(decision)
              ? SystemRecordStore.CreateResult.ALREADY_PRESENT
              : SystemRecordStore.CreateResult.CONFLICT);
    }
  }

  private static final class EmptyManifestRepository implements ManifestRepository {
    private final ManifestRoot root;
    private EmptyManifestRepository(ManifestRoot root) { this.root = root; }
    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(identity.equals(root.systemIdentity())
          ? Optional.of(root) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return empty(); }

    private static <T> CompletableFuture<T> unsupported() {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("unexpected manifest write"));
    }
    private static <T> CompletableFuture<Optional<T>> empty() {
      return CompletableFuture.completedFuture(Optional.empty());
    }
  }

  private static ByteKey repeatedDigest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
