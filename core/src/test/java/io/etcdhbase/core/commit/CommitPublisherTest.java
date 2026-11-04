package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class CommitPublisherTest {
  private static final String OWNER = "publisher-owner";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void failedPreflightDoesNotTransferWorkOrAttemptCas() {
    Fixture fixture = new Fixture(false, ScriptedControlStore::write);

    assertThrows(CompletionException.class, () -> fixture.publish().join());

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void mismatchedOwnerTokenDoesNotTransferWorkOrAttemptCas() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);

    assertThrows(CompletionException.class, () -> fixture.publisher.publish(fixture.invocation,
        new CommitOwnerService.OwnerToken(OWNER, 2), fixture.manifest,
        RequestScope.withoutDeadline()).join());

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void successfulCasReturnsSelectedPendingObservation() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
    assertEquals(1, fixture.handoff.calls.get());
    assertEquals(1, fixture.control.casCalls.get());
  }

  @Test
  void repeatedCasNotUpdatedReturnsIndeterminateWithoutClaimingSelection() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> CompletableFuture.completedFuture(false));

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.INDETERMINATE, result.state());
    assertEquals(32, fixture.control.casCalls.get());
  }

  @Test
  void competingSelectedSlotProvesThisInvocationWasNotSelected() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> {
          CommitState observed = store.current.get();
          PublicationSlot competitor = new PublicationSlot(TransactionId.create(),
              new InvocationId(UUID.randomUUID()), 1, 0, 1, 1, 2, true,
              ByteKey.copyOf(new byte[] {9}), DIGEST);
          store.current.set(observed.publish(competitor));
          return CompletableFuture.completedFuture(false);
        });

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, result.state());
    assertTrue(result.notSelectedProof().isPresent());
  }

  @Test
  void unknownCasThatWasPersistedResolvesSelectedFromTheControlRow() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> {
          store.current.set(replacement);
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.UNKNOWN));
        });

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
  }

  @Test
  void unknownCasWithoutDurableEvidenceRemainsIndeterminate() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNKNOWN)));

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.INDETERMINATE, result.state());
  }

  private static ManifestRoot root(TransactionId transactionId, long ownerEpoch) {
    return ManifestRoot.create(transactionId, ownerEpoch, 0, 1, 0, 0, Optional.empty());
  }

  private static ControlSnapshot snapshot(CommitState state) {
    return new ControlSnapshot(state, new byte[] {1});
  }

  private static final class Fixture {
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationBinding invocation;
    private final CommitOwnerService.OwnerToken owner =
        new CommitOwnerService.OwnerToken(OWNER, 1);
    private final ManifestRoot manifest;
    private final ScriptedControlStore control;
    private final DirectHandoff handoff = new DirectHandoff();
    private final CommitPublisher publisher;

    private Fixture(boolean storedRootPresent,
        BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet) {
      manifest = root(transactionId, 1);
      invocation = InvocationBinding.begin(new InvocationId(UUID.randomUUID()), DIGEST, DIGEST,
          transactionId, 1, 0, 1).seal(manifest.systemIdentity());
      control = new ScriptedControlStore(CommitState.initial(FINGERPRINT, 1)
          .claimOwner(OWNER, 0, 10_000), compareAndSet);
      var outcomes = new CommitOutcomeResolver(control, new EmptyDecisionRepository());
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
      var verifier = new CommitPlanVerifier(new ManifestCatalog(new EmptyManifestRepository(
          storedRootPresent ? Optional.of(manifest) : Optional.empty())), materialization);
      CommitFinalizer finalizer = (invocation, publication, scope) ->
          CompletableFuture.completedFuture(publication);
      publisher = new CommitPublisher(control, outcomes, verifier, handoff, finalizer);
    }

    private CompletableFuture<CommitOutcomeResolver.Resolution> publish() {
      return publisher.publish(invocation, owner, manifest, RequestScope.withoutDeadline());
    }
  }

  private static final class DirectHandoff implements PublicationHandoff {
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public <T> CompletableFuture<T> transfer(InvocationBinding invocation,
        RequestScope requestScope, java.util.function.Function<RequestScope, CompletableFuture<T>> work) {
      calls.incrementAndGet();
      return work.apply(requestScope);
    }
  }

  private static final class ScriptedControlStore implements CommitControlStore {
    private final AtomicReference<CommitState> current;
    private final AtomicInteger casCalls = new AtomicInteger();
    private final BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet;

    private ScriptedControlStore(CommitState initial,
        BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet) {
      current = new AtomicReference<>(initial);
      this.compareAndSet = compareAndSet;
    }

    private static CompletableFuture<Boolean> write(
        ScriptedControlStore store, CommitState replacement) {
      store.current.set(replacement);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(snapshot(current.get())));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      casCalls.incrementAndGet();
      return compareAndSet.apply(this, replacement);
    }
  }

  private static final class EmptyDecisionRepository implements CommitDecisionRepository {
    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision decision, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class EmptyManifestRepository implements ManifestRepository {
    private final Optional<ManifestRoot> root;

    private EmptyManifestRepository(Optional<ManifestRoot> root) {
      this.root = root;
    }

    @Override public int candidateBucket(PreparedCandidate candidate) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(root.filter(value -> value.systemIdentity().equals(identity)));
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }
}
