package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class SealedEventCursorTest {
  @Test
  void readsEventsInOrderWithPauseAndBoundedPageReads() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      var firstBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(0, cursor.next(firstBudget).join().orElseThrow().ordinal());
      assertEquals(3, firstBudget.pages());
      var secondBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(1, cursor.next(secondBudget).join().orElseThrow().ordinal());
      assertTrue(cursor.next(secondBudget).join().isEmpty());
    }
  }

  @Test
  void missingPageAndExhaustedBudgetFailWithoutAdvancingCursor() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      var insufficient = new ManifestCatalog.ProofReadBudget(scope, account, 1, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(insufficient).join());
      fixture.missingPage = true;
      var missing = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(missing).join());
      fixture.missingPage = false;
      var retry = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(0, cursor.next(retry).join().orElseThrow().ordinal());
      cursor.close();
      assertThrows(CompletionException.class, () -> cursor.next(retry).join());
    }
  }

  @Test
  void corruptEventAndLateCompletionAfterCloseAreNotDelivered() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      fixture.wrongEvent = true;
      var corrupt = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(corrupt).join());
      fixture.wrongEvent = false;
      fixture.pendingEvent = new CompletableFuture<>();
      var delayed = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      var result = cursor.next(delayed);
      cursor.close();
      fixture.pendingEvent.complete(Optional.of(fixture.first));
      assertThrows(CompletionException.class, result::join);
    }
  }

  @Test
  void protectedCursorStopsDeliveryWhenPinExpiresAfterTheRead() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      AtomicInteger renewals = new AtomicInteger();
      AtomicInteger releases = new AtomicInteger();
      ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return 1; }
        @Override public CompletableFuture<Void> renew(RequestScope ignored) {
          return renewals.incrementAndGet() == 2
              ? CompletableFuture.failedFuture(new IllegalStateException("pin expired"))
              : CompletableFuture.completedFuture(null);
        }
        @Override public void close() { releases.incrementAndGet(); }
      };
      var cursor = new ProtectedEventCursor(fixture.cursor(scope), pin, scope);
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(budget).join());
      cursor.releaseCompletion().toCompletableFuture().join();
      assertEquals(1, releases.get());
      assertThrows(CompletionException.class, () -> cursor.next(budget).join());
    }
  }

  @Test
  void protectedCursorWaitsForPhysicalProducerBeforeReleasingPin() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      AtomicInteger releases = new AtomicInteger();
      ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return 1; }
        @Override public void close() { releases.incrementAndGet(); }
      };
      fixture.pendingEvent = new CompletableFuture<>();
      CompletableFuture<Void> physical = new CompletableFuture<>();
      scope.trackPhysicalProducer(physical);
      var cursor = new ProtectedEventCursor(fixture.cursor(scope), pin, scope);
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      var result = cursor.next(budget);
      cursor.close();
      fixture.pendingEvent.complete(Optional.of(fixture.first));
      assertThrows(CompletionException.class, result::join);
      assertEquals(0, releases.get());
      assertFalse(cursor.releaseCompletion().toCompletableFuture().isDone());
      physical.complete(null);
      cursor.releaseCompletion().toCompletableFuture().join();
      assertEquals(1, releases.get());
    }
  }

  @Test
  void cancellingEventFutureStopsCursorAndWaitsForPhysicalDrain() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      AtomicInteger releases = new AtomicInteger();
      ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return 1; }
        @Override public void close() { releases.incrementAndGet(); }
      };
      fixture.pendingEvent = new CompletableFuture<>();
      CompletableFuture<Void> physical = new CompletableFuture<>();
      scope.trackPhysicalProducer(physical);
      var cursor = new ProtectedEventCursor(fixture.cursor(scope), pin, scope);
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      var result = cursor.next(budget);
      assertTrue(result.cancel(false));
      assertTrue(scope.isClosed());
      assertEquals(0, releases.get());
      fixture.pendingEvent.complete(Optional.of(fixture.first));
      physical.complete(null);
      cursor.releaseCompletion().toCompletableFuture().join();
      assertEquals(1, releases.get());
    }
  }

  @Test
  void failedPhysicalDrainKeepsProtectionAndReportsCleanupFailure() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      AtomicInteger releases = new AtomicInteger();
      ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return 1; }
        @Override public void close() { releases.incrementAndGet(); }
      };
      CompletableFuture<Void> physical = new CompletableFuture<>();
      scope.trackPhysicalProducer(physical);
      var cursor = new ProtectedEventCursor(fixture.cursor(scope), pin, scope);
      cursor.close();
      physical.completeExceptionally(new IllegalStateException("producer drain failed"));
      assertThrows(CompletionException.class, () -> cursor.releaseCompletion().toCompletableFuture().join());
      assertEquals(0, releases.get());
    }
  }

  @Test
  void opensOnlyAfterPinAndSelectionAreVerified() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var reader = fixture.reader(pins, fixture.decision);
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        var openingBudget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        var cursor = reader.open(index, fixture.digest, scope, openingBudget).join();
        var eventBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
        assertEquals(0, cursor.next(eventBudget).join().orElseThrow().ordinal());
        cursor.close();
        cursor.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void rejectsIndexThatDoesNotMatchTheArchivedDecision() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        var different = new CommitDecision(fixture.decision.transactionId(), InvocationId.create(),
            fixture.decision.sequence(), fixture.decision.baseRevision(), fixture.decision.revision(),
            fixture.decision.manifestRoot(), fixture.decision.manifestDigest(), true);
        var openingBudget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        assertThrows(CompletionException.class,
            () -> fixture.reader(pins, different).open(index, fixture.digest, scope, openingBudget).join());
        assertTrue(scope.isClosed());
      }
    }
  }

  @Test
  void rejectsCompactedOrAlteredEventRootsAfterPinRegistration() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      EventReference original = index.events();
      var altered = new EventReference(original.identity(), ByteKey.copyOf(new byte[32]),
          original.firstOrdinal(), original.lastOrdinal(), original.itemCount(),
          original.totalBytes(), original.leafPageCount(), original.level());
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        assertThrows(CompletionException.class, () -> fixture.reader(pins, fixture.decision)
            .open(new EventRevisionIndex(fixture.decision, altered), fixture.digest, scope, budget).join());
      }
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
            Optional.of("owner"), 1, 100, 1, 4, 12, 4, 12, Optional.empty(), 12);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        assertThrows(CompletionException.class, () -> fixture.reader(pins, fixture.decision, state)
            .open(index, fixture.digest, scope, budget).join());
      }
    }
  }

  @Test
  void rechecksCompactBoundaryAfterReadingSelectedRoot() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        fixture.stateAfterRoot = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
            Optional.of("owner"), 1, 100, 2, 4, 12, 4, 12, Optional.empty(), 12);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        assertThrows(CompletionException.class,
            () -> fixture.reader(pins, fixture.decision).open(index, fixture.digest, scope, budget).join());
        assertTrue(fixture.controlReads.get() >= 3);
      }
    }
  }

  @Test
  void cancellationDuringPinRegistrationReleasesLateCreatedPin() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        records.pauseNextCas = new CompletableFuture<>();
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 5, 1024L << 20);
        var opening = fixture.reader(pins, fixture.decision).open(index, fixture.digest, scope, budget);
        assertFalse(opening.isDone());
        assertTrue(opening.cancel(false));
        records.resumeCas();
        assertTrue(scope.isClosed());
        assertTrue(pins.readBoundary(setup).join().protections().isEmpty());
      }
    }
  }

  @Test
  void sequenceReaderRequiresFinalizedIndexAndPreservesEventOrder() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var finalized = fixture.finalizedState();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var reader = fixture.sequenceReader(pins, finalized, Optional.empty());
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        assertThrows(CompletionException.class, () -> reader.open(3, 10, fixture.digest, scope, budget).join());
      }
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        var reader = fixture.sequenceReader(pins, finalized, Optional.of(index));
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var selected = reader.open(3, 10, fixture.digest, scope, budget).join().orElseThrow();
        var cursor = selected.events().orElseThrow();
        var eventBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
        assertEquals(0, cursor.next(eventBudget).join().orElseThrow().ordinal());
        cursor.close();
        cursor.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void sequenceReaderMayReadSelectedSlotBeforeItsIndexIsFinalized() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var decision = fixture.decision;
    var slot = new PublicationSlot(decision.transactionId(), decision.invocationId(), 1,
        2, 10, 3, 11, true, decision.manifestRoot(), decision.manifestDigest());
    var pending = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 3, 11, 2, 10, Optional.of(slot), 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var selected = fixture.sequenceReader(pins, pending, Optional.empty())
            .open(3, 10, fixture.digest, scope, budget).join().orElseThrow();
        var cursor = selected.events().orElseThrow();
        var eventBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
        assertEquals(0, cursor.next(eventBudget).join().orElseThrow().ordinal());
        cursor.close();
        cursor.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void sequenceReaderHandlesMetadataAndWaitsWhenNoSequenceIsSelected() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var metadata = ManifestRoot.create(fixture.transaction, 1, 2, 10, 0, 0, Optional.empty());
    var decision = new CommitDecision(fixture.transaction, InvocationId.create(),
        3, 10, 10, metadata.systemIdentity(), metadata.digest(), false);
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 3, 10, 3, 10, Optional.empty(), 0);
    fixture.rootReadOverride = metadata;
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 8, 1024L << 20);
        var result = fixture.sequenceReader(pins, state, decision, () -> Optional.empty())
            .open(3, 10, fixture.digest, scope, budget).join().orElseThrow();
        assertEquals(decision, result.decision());
        assertTrue(result.events().isEmpty());
        assertTrue(scope.isClosed());
      }
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 8, 1024L << 20);
        assertTrue(fixture.sequenceReader(pins, state, decision, () -> Optional.empty())
            .open(4, 10, fixture.digest, scope, budget).join().isEmpty());
        assertTrue(scope.isClosed());
      }
    }
  }

  @Test
  void sequenceReaderRereadsIndexWhenFinalizationOvertakesTheFirstLookup() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    AtomicInteger reads = new AtomicInteger();
    var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        Supplier<Optional<EventRevisionIndex>> delayedIndex = () -> reads.incrementAndGet() == 1
            ? Optional.empty() : Optional.of(index);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var selected = fixture.sequenceReader(pins, fixture.finalizedState(), fixture.decision, delayedIndex)
            .open(3, 10, fixture.digest, scope, budget).join().orElseThrow();
        assertEquals(2, reads.get());
        var cursor = selected.events().orElseThrow();
        cursor.close();
        cursor.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private CompletableFuture<Boolean> pauseNextCas;
    private CompletableFuture<Boolean> pausedCas;
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public synchronized CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(identity)));
    }
    @Override public synchronized CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      ByteKey old = rows.putIfAbsent(identity, value);
      return CompletableFuture.completedFuture(old == null ? CreateResult.CREATED
          : old.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public synchronized CompletableFuture<Boolean> compareAndSet(SystemRecordType type,
        ByteKey identity, Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      if (!Optional.ofNullable(rows.get(identity)).equals(expected)) {
        return CompletableFuture.completedFuture(false);
      }
      rows.put(identity, replacement);
      if (pauseNextCas != null) {
        pausedCas = pauseNextCas;
        pauseNextCas = null;
        return pausedCas;
      }
      return CompletableFuture.completedFuture(true);
    }
    private void resumeCas() { pausedCas.complete(true); }
  }

  private static final class Fixture {
    private final TransactionId transaction = TransactionId.create();
    private final PreparedEvent first = event(0, (byte) 'a');
    private final PreparedEvent second = event(1, (byte) 'b');
    private final EventPage page = EventPage.create(transaction, 0,
        List.of(PreparedEventReference.from(first), PreparedEventReference.from(second)));
    private final EventDirectoryPage directory = EventDirectoryPage.create(transaction, 0, 1,
        List.of(page.reference()));
    private final ByteKey key = ByteKey.copyOf(new byte[] {1});
    private final ByteKey digest = ByteKey.copyOf(new byte[32]);
    private final ManifestChildReference candidates =
        new ManifestChildReference(key, digest, 0, 0, key, key, 1, 0, 1, 0);
    private final ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 1, 0,
        Optional.of(candidates), 2, first.retainedBytes() + second.retainedBytes(),
        Optional.of(directory.reference()));
    private final CommitDecision decision = new CommitDecision(transaction, InvocationId.create(),
        3, 10, 11, root.systemIdentity(), root.digest(), true);
    private final AtomicInteger controlReads = new AtomicInteger();
    private CommitState stateAfterRoot;
    private ManifestRoot rootReadOverride;
    private boolean missingPage;
    private boolean wrongEvent;
    private CompletableFuture<Optional<PreparedEvent>> pendingEvent;

    private PreparedEvent event(long ordinal, byte key) {
      byte[] value = {1};
      StoredRecord current = StoredRecord.live(ByteKey.copyOf(new byte[] {key}),
          new StorageMetadata(11, 11, 1, 0, StorageMetadata.State.LIVE, 1), value);
      return new PreparedEvent(transaction, 1, 2, 10, ordinal, PreparedEvent.Kind.PUT,
          current, Optional.empty());
    }

    private ManifestRepository repository() {
      return (ManifestRepository) Proxy.newProxyInstance(
          ManifestRepository.class.getClassLoader(), new Class<?>[] {ManifestRepository.class},
          (proxy, method, arguments) -> switch (method.getName()) {
            case "readRoot" -> CompletableFuture.completedFuture(
                Optional.of(rootReadOverride != null ? rootReadOverride : root));
            case "readEventDirectoryPage" -> CompletableFuture.completedFuture(Optional.of(directory));
            case "readEventPage" -> CompletableFuture.completedFuture(missingPage
                ? Optional.empty() : Optional.of(page));
            case "readEvent" -> pendingEvent != null ? pendingEvent
                : CompletableFuture.completedFuture(Optional.of(wrongEvent ? second
                    : arguments[0].equals(first.systemIdentity()) ? first : second));
            default -> throw new UnsupportedOperationException(method.getName());
          });
    }

    private SealedEventCursor cursor(RequestScope scope) {
      return new SealedEventCursor(repository(), decision, root, scope);
    }

    private CommitState finalizedState() {
      return CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, digest,
          Optional.of("owner"), 1, 100, 1, 3, 11, 3, 11, Optional.empty(), 0);
    }

    private SelectedEventReader reader(EventHistoryPinManager pins, CommitDecision archived) {
      return reader(pins, archived, finalizedState());
    }

    private SelectedEventReader reader(EventHistoryPinManager pins, CommitDecision archived, CommitState state) {
      return new SelectedEventReader(pins, control(state), decisions(archived), repository());
    }

    private SelectedSequenceReader sequenceReader(EventHistoryPinManager pins, CommitState state,
        Optional<EventRevisionIndex> index) {
      return sequenceReader(pins, state, decision, () -> index);
    }

    private SelectedSequenceReader sequenceReader(EventHistoryPinManager pins, CommitState state,
        CommitDecision archived, Supplier<Optional<EventRevisionIndex>> index) {
      EventRevisionIndexRepository indexes = (EventRevisionIndexRepository) Proxy.newProxyInstance(
          EventRevisionIndexRepository.class.getClassLoader(), new Class<?>[] {EventRevisionIndexRepository.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) return CompletableFuture.completedFuture(index.get());
            throw new UnsupportedOperationException(method.getName());
          });
      return new SelectedSequenceReader(pins, control(state), decisions(archived), repository(), indexes);
    }

    private CommitControlStore control(CommitState state) {
      return (CommitControlStore) Proxy.newProxyInstance(
          CommitControlStore.class.getClassLoader(), new Class<?>[] {CommitControlStore.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) {
              CommitState current = controlReads.incrementAndGet() >= 3 && stateAfterRoot != null
                  ? stateAfterRoot : state;
              return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(current, new byte[] {1})));
            }
            throw new UnsupportedOperationException(method.getName());
          });
    }

    private CommitDecisionRepository decisions(CommitDecision archived) {
      return (CommitDecisionRepository) Proxy.newProxyInstance(
          CommitDecisionRepository.class.getClassLoader(), new Class<?>[] {CommitDecisionRepository.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) return CompletableFuture.completedFuture(Optional.of(archived));
            throw new UnsupportedOperationException(method.getName());
          });
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
      var manager = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
      var reads = new BudgetManager.ReadLimits(1_000_000, 8L << 30, 100_000, 65_536, 65_536);
      return manager.admit("public", scope, reservation, reads);
    }
  }
}
