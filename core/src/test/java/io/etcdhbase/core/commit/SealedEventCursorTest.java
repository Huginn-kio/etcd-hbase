package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
  void exhaustedRegistryAttemptsRemainFencedAfterOwnerReplacement() {
    var state = new Fixture().finalizedState();
    for (int attempt = 0; attempt < CommitState.MAX_EVENT_REGISTRY_ATTEMPTS; attempt++) {
      byte[] namespace = new byte[16];
      namespace[0] = (byte) attempt;
      state = state.reserveEventRegistry("owner", state.ownerEpoch(), 20, ByteKey.copyOf(namespace));
    }
    ByteKey pending = state.pendingEventRegistryNamespace().orElseThrow();
    state = state.claimOwner("replacement", 100, 200);
    var control = new MutableControl(state);
    var records = new MemoryRecords();
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var upgrade = new EventHistoryRegistryUpgrade(records, control,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)),
          Clock.fixed(Instant.ofEpochMilli(101), ZoneOffset.UTC));
      var owner = new CommitOwnerService.OwnerToken("replacement", state.ownerEpoch());
      assertThrows(CompletionException.class, () -> upgrade.upgrade(owner, scope).join());
      assertEquals(CommitState.MAX_EVENT_REGISTRY_ATTEMPTS, control.state.eventRegistryAttempts());
      assertEquals(Optional.of(pending), control.state.pendingEventRegistryNamespace());
      assertTrue(records.rows.isEmpty(), "exhausted reservation must not start staging");
    }
  }

  @Test
  void cancelledRegistryAttemptDoesNotSelectAfterItsPhysicalWritesDrain() {
    var records = new MemoryRecords();
    var control = new MutableControl(new Fixture().finalizedState());
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    var clock = Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC);
    var physical = new CompletableFuture<Void>();
    records.pendingProducer = physical;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var selecting = new EventHistoryRegistryUpgrade(records, control, configuration, clock)
          .upgrade(new CommitOwnerService.OwnerToken("owner", 1), scope);
      scope.cancel();
      physical.complete(null);
      assertThrows(CompletionException.class, selecting::join);
      assertEquals(CommitState.EVENT_INDEX_FORMAT_VERSION, control.state.formatVersion());
      assertTrue(control.state.eventRegistryNamespace().isEmpty());
    } finally {
      physical.complete(null);
    }
  }

  @Test
  void replacedOwnerCannotSelectItsLateStagingNamespace() {
    var records = new MemoryRecords();
    var control = new MutableControl(new Fixture().finalizedState());
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    var physical = new CompletableFuture<Void>();
    records.pendingProducer = physical;
    try (RequestScope first = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope second = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var oldUpgrade = new EventHistoryRegistryUpgrade(records, control, configuration,
          Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC));
      var oldSelection = oldUpgrade.upgrade(new CommitOwnerService.OwnerToken("owner", 1), first);
      ByteKey oldNamespace = control.state.pendingEventRegistryNamespace().orElseThrow();

      control.state = control.state.claimOwner("replacement", 100, 200);
      var newUpgrade = new EventHistoryRegistryUpgrade(records, control, configuration,
          Clock.fixed(Instant.ofEpochMilli(101), ZoneOffset.UTC));
      var selected = newUpgrade.upgrade(new CommitOwnerService.OwnerToken("replacement", 3), second).join();
      ByteKey newNamespace = control.state.eventRegistryNamespace().orElseThrow();
      assertFalse(oldNamespace.equals(newNamespace));
      assertEquals(5, selected.owner().epoch());

      physical.complete(null);
      assertThrows(CompletionException.class, oldSelection::join);
      assertEquals(Optional.of(newNamespace), control.state.eventRegistryNamespace());
    } finally {
      physical.complete(null);
    }
  }

  @Test
  void eventRegistrySelectionWaitsForStagingProducersAndIgnoresLateLosingNamespaces() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var control = new MutableControl(fixture.finalizedState());
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    var clock = Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC);
    var physical = new CompletableFuture<Void>();
    records.pendingProducer = physical;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var upgrade = new EventHistoryRegistryUpgrade(records, control, configuration, clock);
      var selecting = upgrade.upgrade(new CommitOwnerService.OwnerToken("owner", 1), scope);
      assertFalse(selecting.isDone());
      assertEquals(CommitState.EVENT_INDEX_FORMAT_VERSION, control.state.formatVersion());
      assertTrue(control.state.pendingEventRegistryNamespace().isPresent());

      physical.complete(null);
      var selection = selecting.join();
      assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, control.state.formatVersion());
      assertEquals(3, selection.owner().epoch());
      var reopened = EventHistoryPinManager.openSelected(records, control, configuration, clock, scope).join();
      assertEquals(selection.registry().registryIdentity(), reopened.registryIdentity());
      assertEquals(control.state.eventRegistryNamespace(), reopened.namespace());

      byte[] losingBytes = control.state.eventRegistryNamespace().orElseThrow().bytes();
      losingBytes[0] ^= 1;
      var losingNamespace = ByteKey.copyOf(losingBytes);
      EventHistoryPinManager.stage(records, losingNamespace, configuration, clock, scope).join();
      assertEquals(reopened.registryIdentity(),
          EventHistoryPinManager.openSelected(records, control, configuration, clock, scope).join().registryIdentity());

      records.deleteEventNamespace(control.state.eventRegistryNamespace().orElseThrow());
      assertThrows(CompletionException.class,
          () -> EventHistoryPinManager.openSelected(records, control, configuration, clock, scope).join());
    } finally {
      physical.complete(null);
    }
  }

  @Test
  void eventCollectionWaitsForBackfillAndRetainsSelectedWindowAcrossReopen() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 4, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var snapshotPins = SnapshotPinManager.initializeFresh(records, configuration, setup).join();
      var eventPins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      var manifests = new ManifestCatalog(fixture.repository());
      var views = new ReadViewService(fixture.control(fixture.finalizedState()),
          manifests, snapshotPins, fixture.digest);
      var retention = new EventHistoryRetention(eventPins, views, new EventIndexBackfillReader(records, manifests));

      var beforeCapture = collectionBoundary(retention, fixture);
      assertEquals(0, beforeCapture.safeSequence());
      assertEquals(0, beforeCapture.safeRevision());

      var checkpoint = new EventIndexBackfillCheckpoint(1, 3, 1, 2, 1, 1, 1, 11, 0,
          eventPins.registryIdentity());
      records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
          EventIndexBackfillCheckpoint.versionIdentity(3), checkpoint.encode(), setup).join();
      var reopened = EventHistoryPinManager.open(records, configuration, Clock.systemUTC(), setup).join();
      var afterRestart = new EventHistoryRetention(reopened, views,
          new EventIndexBackfillReader(records, manifests));
      var pending = collectionBoundary(afterRestart, fixture);
      assertEquals(1, pending.safeSequence());
      assertEquals(2, pending.safeRevision());
      assertTrue(pending.protections().isEmpty());

      var complete = new EventIndexBackfillCheckpoint(1, 4, 1, 2, 3, 1, 11, 11, 0,
          eventPins.registryIdentity());
      records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
          EventIndexBackfillCheckpoint.versionIdentity(4), complete.encode(), setup).join();
      var finalized = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
          Optional.of("owner"), 1, 100, 2, 4, 11, 4, 11, Optional.empty(), 0);
      var currentViews = new ReadViewService(fixture.control(finalized), manifests, snapshotPins, fixture.digest);
      var finished = collectionBoundary(new EventHistoryRetention(reopened, currentViews,
          new EventIndexBackfillReader(records, manifests)), fixture);
      assertEquals(100, finished.safeSequence());
      assertEquals(100, finished.safeRevision());
    }
  }

  private static SnapshotGcBoundary collectionBoundary(EventHistoryRetention retention, Fixture fixture)
      throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 512L << 20);
      return retention.advance(100, 100, fixture.digest, scope, budget).join();
    }
  }

  @Test
  void captureSelectedAfterGcReadCannotRaiseAbsentCheckpointBoundary() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 4, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var snapshotPins = SnapshotPinManager.initializeFresh(records, configuration, setup).join();
      var eventPins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      var manifests = new ManifestCatalog(fixture.repository());
      var views = new ReadViewService(fixture.control(fixture.finalizedState()), manifests, snapshotPins, fixture.digest);
      var retention = new EventHistoryRetention(eventPins, views, new EventIndexBackfillReader(records, manifests));
      records.pendingCheckpointRead = new CompletableFuture<>();
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 512L << 20);
      var collecting = retention.advance(100, 100, fixture.digest, scope, budget);
      assertFalse(collecting.isDone());

      var newlySelected = new EventIndexBackfillCheckpoint(1, 4, 1, 3, 1, 1, 1, 11, 0,
          eventPins.registryIdentity());
      records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
          EventIndexBackfillCheckpoint.versionIdentity(4), newlySelected.encode(), setup).join();
      records.pendingCheckpointRead.complete(Optional.empty());
      var boundary = collecting.join();
      assertEquals(0, boundary.safeSequence());
      assertEquals(0, boundary.safeRevision());
    }
  }

  @Test
  void cancellingCollectionStopsThePendingCheckpointRead() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 4, Duration.ofMinutes(1));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var snapshotPins = SnapshotPinManager.initializeFresh(records, configuration, setup).join();
      var eventPins = EventHistoryPinManager.initializeFresh(records, configuration, Clock.systemUTC(), setup).join();
      var manifests = new ManifestCatalog(fixture.repository());
      var views = new ReadViewService(fixture.control(fixture.finalizedState()), manifests, snapshotPins, fixture.digest);
      var retention = new EventHistoryRetention(eventPins, views, new EventIndexBackfillReader(records, manifests));
      records.pendingCheckpointRead = new CompletableFuture<>();
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 512L << 20);
      var collecting = retention.advance(100, 100, fixture.digest, scope, budget);
      assertTrue(collecting.cancel(false));
      assertTrue(scope.isCancelled());
      records.pendingCheckpointRead.complete(Optional.empty());
      assertEquals(0, eventPins.readBoundary(setup).join().safeSequence());
    }
  }

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
  void backfillProposesProgressOnlyAfterEveryEventIsVerified() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 4, 11, 4, 11, Optional.empty(), 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, pins.registryIdentity());
      try (RequestScope windowScope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account windowAccount = fixture.account(windowScope);
           RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var windowBudget = new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20);
        var window = EventBackfillWindow.open(pins, fixture.control(state), checkpoint, fixture.digest,
            windowScope, windowBudget).join();
        try (window) {
          var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
          var step = EventIndexBackfillStep.open(fixture.sequenceReader(pins, state, Optional.empty()),
              fixture.control(state), window, checkpoint, fixture.digest, 1, 5, scope, budget,
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
          for (int i = 0; i < 2; i++) {
            assertTrue(step.next(new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20),
                new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20))
                .join().isEmpty());
          }
          var proposal = step.next(new ManifestCatalog.ProofReadBudget(scope, account, 1, 512L << 20),
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join().orElseThrow();
          assertTrue(proposal.checkpoint().complete());
          assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), proposal.index().orElseThrow());
          window.checkPublication(proposal.checkpoint(), 1, 5,
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
          assertFalse(pins.readBoundary(setup).join().protections().isEmpty());
          assertThrows(CompletionException.class, () -> step.next(
              new ManifestCatalog.ProofReadBudget(scope, account, 1, 512L << 20),
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join());
        }
        window.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void capturesHistoryProtectionBeforeInitialCheckpointSelection() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, pins.registryIdentity());
      var checkpointTemplate = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, ByteKey.copyOf(new byte[16]));
      var control = fixture.control(fixture.finalizedState());
      assertThrows(CompletionException.class, () -> EventBackfillWindow.openCapture(pins, control,
          checkpointTemplate, fixture.digest, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20)).join());
      assertTrue(pins.readBoundary(setup).join().protections().isEmpty());
      var window = EventBackfillWindow.openCapture(pins, control, checkpoint, fixture.digest, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20)).join();
      assertFalse(pins.readBoundary(setup).join().protections().isEmpty());
      window.checkPublication(checkpoint, 1, 4,
          new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20)).join();
      window.close();
      window.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void initialCaptureRejectsAnOutdatedUpperBoundary() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 2, 2, 2, 10, 10, 10, 0, pins.registryIdentity());
      assertThrows(CompletionException.class, () -> EventBackfillWindow.openCapture(pins,
          fixture.control(fixture.finalizedState()), checkpoint, fixture.digest, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20)).join());
    }
  }

  @Test
  void initialCaptureVerifiesRetainedDecisionBoundaries() throws Exception {
    Fixture fixture = new Fixture();
    CommitDecision previous = new CommitDecision(TransactionId.create(), InvocationId.create(),
        2, 10, 10, fixture.key, fixture.digest, false);
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 3, 11, 3, 11, Optional.empty(), 11);
    var decisions = fixture.decisionsBySequence(Map.of(2L, previous, 3L, fixture.decision));
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 11, pins.registryIdentity());
      var capture = new EventIndexBackfillCapture(pins, fixture.control(state), decisions);
      var window = capture.open(checkpoint, fixture.digest, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 16, 1024L << 20)).join();
      assertFalse(pins.readBoundary(setup).join().protections().isEmpty());
      window.close();
      window.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void initialCaptureRejectsSkippedOrMissingRetainedHistory() throws Exception {
    Fixture fixture = new Fixture();
    CommitDecision previous = new CommitDecision(TransactionId.create(), InvocationId.create(),
        2, 10, 10, fixture.key, fixture.digest, false);
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 3, 11, 3, 11, Optional.empty(), 10);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 10, pins.registryIdentity());
      for (var decisions : List.of(fixture.decisionsBySequence(Map.of(2L, previous, 3L, fixture.decision)),
          fixture.decisionsBySequence(Map.of(2L, previous)))) {
        try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
             BudgetManager.Account account = fixture.account(scope)) {
          var capture = new EventIndexBackfillCapture(pins, fixture.control(state), decisions);
          assertThrows(CompletionException.class, () -> capture.open(checkpoint, fixture.digest, scope,
              new ManifestCatalog.ProofReadBudget(scope, account, 16, 1024L << 20)).join());
        }
      }
    }
  }

  @Test
  void backfillRejectsMissingEventPageWithoutAdvancingCheckpoint() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 4, 11, 4, 11, Optional.empty(), 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, pins.registryIdentity());
      try (RequestScope windowScope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account windowAccount = fixture.account(windowScope);
           RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var window = EventBackfillWindow.open(pins, fixture.control(state), checkpoint, fixture.digest,
            windowScope, new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
        try (window) {
          var step = EventIndexBackfillStep.open(fixture.sequenceReader(pins, state, Optional.empty()),
              fixture.control(state), window, checkpoint, fixture.digest, 1, 5, scope,
              new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20),
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
          assertTrue(step.next(new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20),
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join().isEmpty());
          fixture.missingPage = true;
          assertThrows(CompletionException.class, () -> step.next(
              new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20),
              new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join());
          assertEquals(3, checkpoint.nextSequence());
          assertTrue(scope.isClosed());
        }
        window.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void losingWindowProtectionPreventsBackfillProposal() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 4, 11, 4, 11, Optional.empty(), 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope windowScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account windowAccount = fixture.account(windowScope);
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, pins.registryIdentity());
      var window = EventBackfillWindow.open(pins, fixture.control(state), checkpoint, fixture.digest,
          windowScope, new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
      var step = EventIndexBackfillStep.open(fixture.sequenceReader(pins, state, Optional.empty()),
          fixture.control(state), window, checkpoint, fixture.digest, 1, 5, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20),
          new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join();
      for (int i = 0; i < 2; i++) {
        assertTrue(step.next(new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20),
            new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join().isEmpty());
      }
      window.close();
      assertThrows(CompletionException.class, () -> step.next(
          new ManifestCatalog.ProofReadBudget(scope, account, 1, 512L << 20),
          new ManifestCatalog.ProofReadBudget(windowScope, windowAccount, 3, 512L << 20)).join());
      assertTrue(scope.isClosed());
      window.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void legacyReaderAllowsMissingIndexOnlyInsideCapturedUnchangedWindow() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 4, 11, 4, 11, Optional.empty(), 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0, pins.registryIdentity());
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var wrongRegistry = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0,
            ByteKey.copyOf(new byte[16]));
        assertThrows(CompletionException.class, () -> fixture.sequenceReader(pins, state, Optional.empty())
            .openLegacy(wrongRegistry, fixture.digest, scope, budget).join());
        var selected = fixture.sequenceReader(pins, state, Optional.empty())
            .openLegacy(checkpoint, fixture.digest, scope, budget).join().orElseThrow();
        assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), selected.index().orElseThrow());
        var cursor = selected.events().orElseThrow();
        var eventBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
        assertEquals(0, cursor.next(eventBudget).join().orElseThrow().ordinal());
        cursor.close();
        cursor.releaseCompletion().toCompletableFuture().join();
      }
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var compacted = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
            Optional.of("owner"), 1, 100, 2, 4, 11, 4, 11, Optional.empty(), 11);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        assertThrows(CompletionException.class, () -> fixture.sequenceReader(pins, compacted, Optional.empty())
            .openLegacy(checkpoint, fixture.digest, scope, budget).join());
      }
    }
  }

  @Test
  void sequenceReaderCanReadAnEventAtTheCompactBoundary() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
        Optional.of("owner"), 1, 100, 1, 3, 11, 3, 11, Optional.empty(), 11);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var selected = fixture.sequenceReader(pins, state, Optional.of(index))
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

  @Test
  void sharedTailerReadsOnceAndIsolatesSlowSubscriptions() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope fastScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope slowScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope lateScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account fastMemory = fixture.account(fastScope);
         BudgetManager.Account slowMemory = fixture.account(slowScope);
         BudgetManager.Account lateMemory = fixture.account(lateScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 3, 4, 8192)) {
        var fast = tailer.subscribe(KeyRange.all(), fastMemory, 4, 8192);
        var slow = tailer.subscribe(KeyRange.all(), slowMemory, 1, 8192);
        assertEquals(SharedEventTailer.Step.OPENED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join());
        assertEquals(SharedEventTailer.Step.BUFFERED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join());
        var lateStart = tailer.subscribeWithCheckpoint(KeyRange.all(), lateMemory, 4, 8192);
        assertEquals(2, lateStart.sequence());
        assertEquals(10, lateStart.revision());
        assertEquals(SharedEventTailer.Step.BUFFERED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join());
        assertTrue(fast.poll().isEmpty());
        assertEquals(SharedEventTailer.Step.PUBLISHED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 1, 512L << 20)).join());
        assertEquals(2, fixture.eventReads.get());
        assertEquals(3, tailer.completedSequence());
        assertEquals(11, tailer.completedRevision());
        assertEquals(Optional.of(SharedEventTailer.Termination.RESOURCE_LIMIT), slow.termination());
        assertTrue(slow.poll().isEmpty());
        try (var first = fast.poll().orElseThrow(); var second = fast.poll().orElseThrow()) {
          assertEquals(0, first.event().ordinal());
          assertEquals(1, second.event().ordinal());
          assertEquals(11, first.revision());
        }
        try (var first = lateStart.subscription().poll().orElseThrow();
             var second = lateStart.subscription().poll().orElseThrow()) {
          assertEquals(0, first.event().ordinal());
          assertEquals(1, second.event().ordinal());
          assertEquals(3, first.sequence());
        }
        var afterPublish = tailer.subscribeWithCheckpoint(KeyRange.all(), slowMemory, 4, 8192);
        assertEquals(3, afterPublish.sequence());
        assertEquals(11, afterPublish.revision());
        assertTrue(afterPublish.subscription().poll().isEmpty());
        assertTrue(fast.poll().isEmpty());
      }
    }
  }

  @Test
  void sharedTailerDoesNotPublishAnIncompleteCorruptRevision() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope watchScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account watchMemory = fixture.account(watchScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 2, 4, 8192)) {
        var watch = tailer.subscribe(KeyRange.all(), watchMemory, 4, 8192);
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join();
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join();
        fixture.missingPage = true;
        assertThrows(CompletionException.class, () -> tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join());
        assertTrue(watch.poll().isEmpty());
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED), watch.termination());
        assertEquals(2, tailer.completedSequence());
      }
    }
  }

  @Test
  void closingSharedTailerDuringBackendReadDoesNotPublishLateEvent() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope watchScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account watchMemory = fixture.account(watchScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 2, 4, 8192)) {
        var watch = tailer.subscribe(KeyRange.all(), watchMemory, 4, 8192);
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join();
        fixture.pendingEvent = new CompletableFuture<>();
        var reading = tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20));
        tailer.close();
        fixture.pendingEvent.complete(Optional.of(fixture.first));
        assertThrows(CompletionException.class, reading::join);
        assertTrue(watch.poll().isEmpty());
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED), watch.termination());
      }
    }
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private CompletableFuture<Boolean> pauseNextCas;
    private CompletableFuture<Boolean> pausedCas;
    private CompletableFuture<Optional<SystemRecordEntry>> pendingCheckpointRead;
    private CompletableFuture<Void> pendingProducer;
    private boolean producerTracked;
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public synchronized CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(identity)));
    }
    @Override public synchronized CompletableFuture<Optional<SystemRecordEntry>> latestAtOrBefore(
        SystemRecordType type, ByteKey route, long sequence, RequestScope scope) {
      if (type != SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT) {
        throw new UnsupportedOperationException("unsupported versioned record");
      }
      if (pendingCheckpointRead != null) return pendingCheckpointRead;
      return CompletableFuture.completedFuture(rows.entrySet().stream()
          .filter(row -> row.getKey().length() == EventIndexBackfillCheckpoint.IDENTITY_BYTES)
          .filter(row -> java.util.Arrays.equals(route.bytes(),
              java.util.Arrays.copyOf(row.getKey().bytes(), route.length())))
          .filter(row -> EventIndexBackfillCheckpoint.sequence(row.getKey()) <= sequence)
          .max(Map.Entry.comparingByKey())
          .map(row -> new SystemRecordEntry(row.getKey(), row.getValue())));
    }
    @Override public synchronized CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      if (pendingProducer != null && !producerTracked && type == SystemRecordType.SNAPSHOT_PIN
          && identity.bytes()[0] == 0x45) {
        producerTracked = true;
        scope.trackPhysicalProducer(pendingProducer);
      }
      ByteKey old = rows.putIfAbsent(identity, value);
      return CompletableFuture.completedFuture(old == null ? CreateResult.CREATED
          : old.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    private synchronized void deleteEventNamespace(ByteKey namespace) {
      byte[] prefix = java.nio.ByteBuffer.allocate(23)
          .put(new byte[] {0x45, 0x56, 0x50, 0x49, 0x4e, 0x01, 0x01}).put(namespace.bytes()).array();
      rows.keySet().removeIf(key -> key.length() >= prefix.length
          && java.util.Arrays.equals(prefix, java.util.Arrays.copyOf(key.bytes(), prefix.length)));
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

  private static final class MutableControl implements CommitControlStore {
    private CommitState state;
    private MutableControl(CommitState state) { this.state = state; }
    @Override public synchronized CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public synchronized CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (observed.state() != state) return CompletableFuture.completedFuture(false);
      state = replacement;
      return CompletableFuture.completedFuture(true);
    }
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
    private final AtomicInteger eventReads = new AtomicInteger();
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
            case "readEvent" -> {
              eventReads.incrementAndGet();
              yield pendingEvent != null ? pendingEvent
                  : CompletableFuture.completedFuture(Optional.of(wrongEvent ? second
                      : arguments[0].equals(first.systemIdentity()) ? first : second));
            }
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

    private CommitDecisionRepository decisionsBySequence(Map<Long, CommitDecision> archived) {
      return (CommitDecisionRepository) Proxy.newProxyInstance(
          CommitDecisionRepository.class.getClassLoader(), new Class<?>[] {CommitDecisionRepository.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) {
              return CompletableFuture.completedFuture(Optional.ofNullable(archived.get(arguments[0])));
            }
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
