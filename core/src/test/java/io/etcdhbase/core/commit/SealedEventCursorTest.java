package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class SealedEventCursorTest {
  private static Object invokeDefault(Object proxy, java.lang.reflect.Method method,
      Object[] arguments) throws Throwable {
    if (!method.isDefault()) throw new UnsupportedOperationException(method.getName());
    return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, arguments);
  }

  @Test
  void measuredProofReadsChargeActualEncodedBytesAndAllowNinthSmallRead() throws Exception {
    var limits = new BudgetManager.Limits(16, 16, 16, 16, 16, 1_000_000, 1024L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192,
        ManifestCatalog.maximumProofReadPageBytes(), 8192, 8192);
    var reads = new BudgetManager.ReadLimits(4096, 512L << 20, 4096, 4096, 4096);
    long encodedRecordBytes = 1024;

    try (var scope = RequestScope.withoutDeadline();
         var account = manager.admit("public", scope, reservation, reads)) {
      var proof = new ManifestCatalog.ProofReadBudget(scope, account, 4096, 512L << 20);
      for (int read = 0; read < 9; read++) {
        int recordValue = read;
        proof.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
            () -> CompletableFuture.completedFuture(new SystemRecordRead<>(
                Optional.of(recordValue), encodedRecordBytes))).join();
      }

      assertEquals(9, proof.pages());
      assertEquals(9 * encodedRecordBytes, proof.bytes());
      assertEquals(0, manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void measuredProofReadsStillRejectActualBytesBeyondTheirCumulativeLimit() throws Exception {
    var limits = new BudgetManager.Limits(16, 16, 16, 16, 16, 1_000_000, 1024L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192,
        ManifestCatalog.maximumProofReadPageBytes(), 8192, 8192);
    var reads = new BudgetManager.ReadLimits(4096, 512L << 20, 4096, 4096, 4096);

    try (var scope = RequestScope.withoutDeadline();
         var account = manager.admit("public", scope, reservation, reads)) {
      var proof = new ManifestCatalog.ProofReadBudget(scope, account, 4096, 3);
      proof.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
          () -> CompletableFuture.completedFuture(new SystemRecordRead<>(Optional.of(1), 2))).join();
      var failure = assertThrows(CompletionException.class,
          () -> proof.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
              () -> CompletableFuture.completedFuture(new SystemRecordRead<>(Optional.of(2), 2))).join());

      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) failure.getCause()).category());
      assertEquals(2, proof.pages());
      assertEquals(2, proof.bytes());
      assertEquals(0, manager.snapshot().allocatedBytes());
    }
  }

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
  void scopeCleanupWaitsForDurableEventPinRelease() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      CompletableFuture<Void> durableRelease = new CompletableFuture<>();
      AtomicInteger releases = new AtomicInteger();
      ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return 1; }
        @Override public void close() { releases.incrementAndGet(); }
        @Override public java.util.concurrent.CompletionStage<Void> releaseCompletion() {
          return durableRelease;
        }
      };
      var cursor = new ProtectedEventCursor(fixture.cursor(scope), pin, scope);
      scope.close();
      assertTrue(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertEquals(1, releases.get());
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertFalse(cursor.releaseCompletion().toCompletableFuture().isDone());
      durableRelease.complete(null);
      scope.cleanupCompletion().toCompletableFuture().join();
      cursor.releaseCompletion().toCompletableFuture().join();
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
        var reader = fixture.sequenceReader(pins, pending, Optional.empty());
        var readAhead = reader.openNext(0, 10, fixture.digest, scope, budget).join();
        assertEquals(2, readAhead.metadataThroughSequence());
        var selected = readAhead.nextEvent().orElseThrow();
        assertEquals(3, selected.decision().sequence());
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
  void initialCaptureKeepsItsPlannedLegacyUpperWhenNewerEventFinalizes() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      // The planner captured legacy history through sequence 2. A newer J44-format key change
      // may finalize before pin registration; the captured interval must remain the protected one.
      var checkpoint = new EventIndexBackfillCheckpoint(1, 4, 2, 2, 2, 10, 10, 10, 0, pins.registryIdentity());
      CommitState later = fixture.finalizedState();
      assertEquals(3, later.finalizedSequence());
      assertEquals(11, later.finalizedRevision());
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      var window = EventBackfillWindow.openCapture(pins,
          fixture.control(later), checkpoint, fixture.digest, scope, budget).join();
      try {
        SnapshotGcBoundary.Protection protection = pins.readBoundary(setup).join()
            .protections().getFirst();
        assertEquals(11, protection.requiredRevision());
        assertEquals(2, protection.requiredSequence());
        window.checkHistory(checkpoint, 1, budget).join();
      } finally {
        window.close();
        window.releaseCompletion().toCompletableFuture().join();
      }
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
  void sequenceReaderSkipsMetadataOnlySequencesButFailsClosedOnMissingEventIndex() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var noEventState = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
            Optional.of("owner"), 1, 100, 1, 100_000, 10, 100_000, 10, Optional.empty(), 0);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 8, 1024L << 20);
        var readAhead = fixture.sequenceReader(pins, noEventState, Optional.empty())
            .openNext(3, 10, fixture.digest, scope, budget).join();
        assertEquals(100_000, readAhead.metadataThroughSequence());
        assertTrue(readAhead.nextEvent().isEmpty());
        assertEquals(0, fixture.decisionReads.get());
        assertEquals(0, fixture.indexReads.get());
      }
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var missingIndexState = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
            Optional.of("owner"), 1, 100, 1, 100_000, 11, 100_000, 11, Optional.empty(), 0);
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 8, 1024L << 20);
        assertThrows(CompletionException.class, () -> fixture.sequenceReader(pins,
            missingIndexState, Optional.empty()).openNext(3, 10, fixture.digest, scope, budget).join());
      }
    }
  }

  @Test
  void sequenceReaderWaitsAtMaximumSelectedSequenceWithoutOverflow() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var maximumState = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, fixture.digest,
          Optional.of("owner"), 1, 100, 1, Long.MAX_VALUE, 10, Long.MAX_VALUE, 10,
          Optional.empty(), 1);
      var reader = fixture.sequenceReader(pins, maximumState, Optional.empty());
      try (var tailer = new SharedEventTailer(reader, fixture.digest, tailMemory,
          Long.MAX_VALUE, 10, 2, 4, 8192)) {
        var budget = new ManifestCatalog.ProofReadBudget(readScope, readMemory, 8, 1024L << 20);
        assertEquals(SharedEventTailer.Step.WAITING, tailer.step(readScope, budget).join());
        assertEquals(Long.MAX_VALUE, tailer.completedSequence());
        assertEquals(10, tailer.completedRevision());
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
  void sequenceReaderPersistsSelectedEventRootBeforeReadingItsIndex() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    AtomicInteger indexReads = new AtomicInteger();
    var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      fixture.indexReadObserver = scope -> {
        SnapshotGcBoundary boundary = pins.readBoundary(scope).join();
        SnapshotGcBoundary.Protection protection = boundary.protections().getFirst();
        assertEquals(11, protection.requiredRevision());
        assertEquals(3, protection.requiredSequence());
        assertEquals(Optional.of(fixture.decision.transactionId()), protection.transactionId());
        assertEquals(Optional.of(index.events().identity()), protection.rootIdentity());
        assertEquals(Optional.of(index.events().digest()), protection.rootDigest());
        indexReads.incrementAndGet();
      };
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account account = fixture.account(scope)) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1024L << 20);
        var selected = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index))
            .open(3, 10, fixture.digest, scope, budget).join().orElseThrow();
        assertEquals(1, indexReads.get());
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
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 4, 4, 8192)) {
        var fast = tailer.subscribe(KeyRange.all(), fastMemory, 4, 8192);
        var slow = tailer.subscribe(KeyRange.all(), slowMemory, 1, 8192);
        long byteLimit = fixture.first.retainedBytes() + fixture.second.retainedBytes() - 1;
        var byteLimited = tailer.subscribe(KeyRange.all(), lateMemory, 4, byteLimit);
        assertEquals(3, tailer.subscriptionCount());
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
        assertEquals(2, tailer.lastPublishedEventCount());
        assertTrue(tailer.lastPublishedEventBytes() > 0);
        assertEquals(3, tailer.observedPublishedSequence());
        assertEquals(Optional.of(SharedEventTailer.Termination.RESOURCE_LIMIT), slow.termination());
        assertTrue(slow.poll().isEmpty());
        assertEquals(Optional.of(SharedEventTailer.Termination.RESOURCE_LIMIT), byteLimited.termination());
        assertTrue(byteLimited.poll().isEmpty(), "a byte-limit failure clears the partial revision");
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
        assertEquals(3, tailer.subscriptionCount());
        assertTrue(fast.poll().isEmpty());
      }
    }
  }

  @Test
  void sharedTailerUsesRevisionIndexToSkipMetadataBeforeTheNextEvent() throws Exception {
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
      var locatorProtection = new AtomicReference<SnapshotGcBoundary>();
      fixture.indexReadObserver = scope -> locatorProtection.set(pins.readBoundary(scope).join());
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 0, 10, 2, 4, 8192)) {
        var watch = tailer.subscribe(KeyRange.all(), watchMemory, 4, 8192);
        var budget = new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20);
        assertEquals(SharedEventTailer.Step.OPENED, tailer.step(readScope, budget).join());
        assertEquals(2, tailer.completedSequence(), "metadata sequences before the event are skipped");
        assertEquals(10, tailer.completedRevision());
        assertEquals(1, fixture.decisionReads.get(), "only the indexed event decision is opened");
        var locatorBoundary = locatorProtection.get();
        assertTrue(locatorBoundary != null, "the locator index was read under a durable pin");
        assertEquals(1, locatorBoundary.protections().size());
        assertEquals(11, locatorBoundary.protections().getFirst().requiredRevision());
        assertEquals(1, locatorBoundary.protections().getFirst().requiredSequence());
        assertEquals(1, fixture.indexReads.get(), "the located index is reused after opening the decision");
        assertEquals(SharedEventTailer.Step.BUFFERED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join());
        assertEquals(SharedEventTailer.Step.BUFFERED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join());
        assertEquals(SharedEventTailer.Step.PUBLISHED, tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 1, 512L << 20)).join());
        try (var delivery = watch.poll().orElseThrow()) {
          assertEquals(3, delivery.sequence());
          assertEquals(11, delivery.revision());
          assertEquals(fixture.first, delivery.event());
        }
        assertEquals(1, fixture.decisionReads.get());
      }
    }
  }

  @Test
  void watchRangeIndexMemoryIsAccountedAndReleasedOnCancellation() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    var watchLimits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 1L << 20);
    var watchBudgets = new BudgetManager(watchLimits, Map.of("watch", watchLimits), watchLimits);
    var watchReservation = new BudgetManager.Reservation(0, 1, 1, 800, 0);
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (RequestScope watchScope = RequestScope.withTimeout(Duration.ofSeconds(5));
           BudgetManager.Account watchMemory = watchBudgets.admit("watch", watchScope, watchReservation);
           var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 4, 4, 8192)) {
        long baselineBytes = watchBudgets.snapshot().allocatedBytes();
        var subscription = tailer.subscribe(
            KeyRange.between(ByteKey.copyOf(new byte[80]), ByteKey.copyOf(new byte[120])),
            watchMemory, 4, 8192);
        long expectedIndexBytes = SharedEventTailer.SUBSCRIPTION_INDEX_ENTRY_OVERHEAD_BYTES + 200;
        assertEquals(expectedIndexBytes,
            watchBudgets.snapshot().allocatedBytes() - baselineBytes);

        BackendFailure exhausted = assertThrows(BackendFailure.class,
            () -> tailer.subscribe(KeyRange.point(ByteKey.copyOf(new byte[100])), watchMemory, 4, 8192));
        assertEquals(BackendFailure.Category.RESOURCE_LIMIT, exhausted.category());
        assertEquals(1, tailer.subscriptionCount(), "a failed reservation must not install an index entry");

        subscription.close();
        assertEquals(baselineBytes, watchBudgets.snapshot().allocatedBytes(),
            "cancelling a watch releases its range and index reservation");
        assertEquals(0, tailer.subscriptionCount());
      }
    }
  }

  @Test
  void subscriptionCapacityIsResourceLimitAndClosedTailerIsUnavailable() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope firstWatchScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope rejectedWatchScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account firstWatchMemory = fixture.account(firstWatchScope);
         BudgetManager.Account rejectedWatchMemory = fixture.account(rejectedWatchScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 1, 4, 8192)) {
        var existing = tailer.subscribeWithCheckpoint(KeyRange.all(), firstWatchMemory, 4, 8192, 0);

        BackendFailure atCapacity = assertThrows(BackendFailure.class,
            () -> tailer.subscribeWithCheckpoint(
                KeyRange.point(fixture.first.current().key()), rejectedWatchMemory, 4, 8192, 0));

        assertEquals(BackendFailure.Category.RESOURCE_LIMIT, atCapacity.category());
        assertEquals(1, tailer.subscriptionCount(), "capacity rejection must not replace the existing watch");
        assertTrue(existing.subscription().termination().isEmpty(),
            "capacity rejection leaves the existing watch active");

        tailer.close();
        BackendFailure afterClose = assertThrows(BackendFailure.class,
            () -> tailer.subscribeWithCheckpoint(KeyRange.all(), rejectedWatchMemory, 4, 8192, 0));
        assertEquals(BackendFailure.Category.UNAVAILABLE, afterClose.category());
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED),
            existing.subscription().termination());
      }
    }
  }

  @Test
  void sharedTailerDeliversOnlySparsePointAndRangeMatches() throws Exception {
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
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 6, 4, 8192)) {
        ByteKey firstKey = fixture.first.current().key();
        ByteKey secondKey = fixture.second.current().key();
        var first = tailer.subscribe(KeyRange.point(firstKey), watchMemory, 4, 8192);
        var second = tailer.subscribe(KeyRange.between(firstKey, secondKey), watchMemory, 4, 8192);
        var last = tailer.subscribe(KeyRange.point(secondKey), watchMemory, 4, 8192);
        var missing = tailer.subscribe(KeyRange.point(ByteKey.copyOf(new byte[] {'c'})), watchMemory, 4, 8192);
        var cancelled = tailer.subscribe(KeyRange.all(), watchMemory, 4, 8192);
        cancelled.close();
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join();
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join();
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join();
        tailer.step(readScope, new ManifestCatalog.ProofReadBudget(readScope, readMemory, 1, 512L << 20)).join();
        try (var pointEvent = first.poll().orElseThrow(); var rangeEvent = second.poll().orElseThrow()) {
          assertEquals(firstKey, pointEvent.event().current().key());
          assertEquals(0, pointEvent.event().ordinal());
          assertEquals(firstKey, rangeEvent.event().current().key());
          assertEquals(0, rangeEvent.event().ordinal());
        }
        try (var pointEvent = last.poll().orElseThrow()) {
          assertEquals(secondKey, pointEvent.event().current().key());
          assertEquals(1, pointEvent.event().ordinal());
        }
        assertTrue(first.poll().isEmpty());
        assertTrue(second.poll().isEmpty());
        assertTrue(last.poll().isEmpty());
        assertTrue(missing.poll().isEmpty());
        assertTrue(cancelled.poll().isEmpty());
        assertEquals(2, fixture.eventReads.get());
      }
    }
  }

  @Test
  void sharedTailerAppliesInclusiveRevisionFloorsAndDropsEventsBeforeCreateBarrier() throws Exception {
    Fixture fixture = new Fixture();
    var records = new MemoryRecords();
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope tailScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope firstScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope secondScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope zeroScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope futureScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope negativeScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope readScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account tailMemory = fixture.account(tailScope);
         BudgetManager.Account firstMemory = fixture.account(firstScope);
         BudgetManager.Account secondMemory = fixture.account(secondScope);
         BudgetManager.Account zeroMemory = fixture.account(zeroScope);
         BudgetManager.Account futureMemory = fixture.account(futureScope);
         BudgetManager.Account negativeMemory = fixture.account(negativeScope);
         BudgetManager.Account readMemory = fixture.account(readScope)) {
      var pins = EventHistoryPinManager.initializeFresh(records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 5, 4, 8192)) {
        var first = tailer.subscribeWithCheckpoint(KeyRange.all(), firstMemory, 4, 8192, 11);
        AtomicInteger wakeups = new AtomicInteger();
        AtomicInteger callbacksUnderTailerLock = new AtomicInteger();
        first.subscription().setWakeup(() -> {
          if (Thread.holdsLock(tailer)) callbacksUnderTailerLock.incrementAndGet();
          wakeups.incrementAndGet();
        });
        var matchingSubscription = tailer.subscribeWithCheckpoint(KeyRange.all(), secondMemory, 4, 8192, 11);
        AtomicInteger secondWakeups = new AtomicInteger();
        matchingSubscription.subscription().setWakeup(() -> {
          if (Thread.holdsLock(tailer)) callbacksUnderTailerLock.incrementAndGet();
          secondWakeups.incrementAndGet();
        });
        var zero = tailer.subscribeWithCheckpoint(KeyRange.all(), zeroMemory, 4, 8192, 0);
        var future = tailer.subscribeWithCheckpoint(KeyRange.all(), futureMemory, 4, 8192, 12);
        var negative = tailer.subscribeWithCheckpoint(KeyRange.all(), negativeMemory, 4, 8192, -1);
        assertEquals(10, first.revision());
        assertEquals(10, zero.revision());
        assertEquals(10, future.revision());
        assertEquals(10, negative.revision());

        tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join();
        tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join();
        tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 3, 512L << 20)).join();
        tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 1, 512L << 20)).join();
        assertEquals(1, wakeups.get(),
            "one completed revision wakes the subscription once after all matching events are queued");
        assertEquals(1, secondWakeups.get(),
            "one publication wakes every matching subscription without a shared polling timer");
        assertEquals(0, callbacksUnderTailerLock.get(), "publication callbacks run outside the tailer lock");

        AtomicInteger immediateWakeups = new AtomicInteger();
        first.subscription().setWakeup(() -> {
          if (Thread.holdsLock(tailer)) callbacksUnderTailerLock.incrementAndGet();
          immediateWakeups.incrementAndGet();
        });
        assertEquals(1, immediateWakeups.get(),
            "installing a listener after publication signals already queued work immediately");
        assertEquals(0, callbacksUnderTailerLock.get(), "immediate callbacks also run outside the tailer lock");

        try (var inclusive = first.subscription().poll().orElseThrow()) {
          assertEquals(11, inclusive.revision(), "a positive start is inclusive at the live tailer layer");
          assertEquals(0, inclusive.event().ordinal());
        }
        try (var second = first.subscription().poll().orElseThrow()) {
          assertEquals(11, second.revision());
          assertEquals(1, second.event().ordinal(), "both events in the revision are delivered exactly once");
        }
        try (var firstDelivery = matchingSubscription.subscription().poll().orElseThrow();
             var secondDelivery = matchingSubscription.subscription().poll().orElseThrow()) {
          assertEquals(11, firstDelivery.revision());
          assertEquals(11, secondDelivery.revision());
          assertEquals(0, firstDelivery.event().ordinal());
          assertEquals(1, secondDelivery.event().ordinal());
        }
        assertTrue(matchingSubscription.subscription().poll().isEmpty(),
            "the second subscriber receives each event once");
        assertTrue(first.subscription().poll().isEmpty(), "a notification does not duplicate a delivery");
        assertTrue(future.subscription().poll().isEmpty(), "a future start must not receive earlier revisions");

        zero.subscription().requireRevisionAtLeast(12);
        negative.subscription().requireRevisionAtLeast(12);
        assertTrue(zero.subscription().poll().isEmpty(), "zero start drops events at the creation barrier");
        assertTrue(negative.subscription().poll().isEmpty(), "negative start begins after the creation barrier");

        AtomicInteger cancellationWakeups = new AtomicInteger();
        zero.subscription().setWakeup(cancellationWakeups::incrementAndGet);
        zero.subscription().close();
        zero.subscription().close();
        assertEquals(1, cancellationWakeups.get(), "subscription cancellation wakes its consumer only once");

        AtomicInteger removedListenerWakeups = new AtomicInteger();
        negative.subscription().setWakeup(removedListenerWakeups::incrementAndGet);
        negative.subscription().setWakeup(null);
        negative.subscription().close();
        assertEquals(0, removedListenerWakeups.get(), "cleared listeners are not retained for cancellation");

        tailer.close();
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED), first.subscription().termination());
        assertEquals(2, immediateWakeups.get(), "tailer close wakes a still-registered consumer");
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED),
            matchingSubscription.subscription().termination());
        assertEquals(2, secondWakeups.get(), "tailer close also wakes the second active consumer");
        tailer.close();
        assertEquals(2, immediateWakeups.get(), "repeated tailer close does not duplicate notification");
        assertEquals(2, secondWakeups.get(), "repeated tailer close does not duplicate the second notification");
        assertEquals(0, callbacksUnderTailerLock.get(), "close callbacks run outside the tailer lock");
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

  @Test
  void synchronousSequenceOpenFailureClosesTailerAndSubscriptions() throws Exception {
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
      records.failPointReadsSynchronously = true;
      var index = EventRevisionIndex.fromSelected(fixture.decision, fixture.root);
      var source = fixture.sequenceReader(pins, fixture.finalizedState(), Optional.of(index));
      try (var tailer = new SharedEventTailer(source, fixture.digest, tailMemory, 2, 10, 2, 4, 8192)) {
        var watch = tailer.subscribe(KeyRange.all(), watchMemory, 4, 8192);

        assertThrows(CompletionException.class, () -> tailer.step(readScope,
            new ManifestCatalog.ProofReadBudget(readScope, readMemory, 10, 1024L << 20)).join());

        assertTrue(tailer.isClosed(), "a synchronous open failure must not leave a stuck tailer");
        assertEquals(0, tailer.subscriptionCount());
        assertEquals(Optional.of(SharedEventTailer.Termination.TAILER_CLOSED), watch.termination());
        assertTrue(watch.poll().isEmpty());
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
    private boolean failPointReadsSynchronously;
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public synchronized CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      if (failPointReadsSynchronously) throw new IllegalStateException("synchronous record read failure");
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
    private final AtomicInteger decisionReads = new AtomicInteger();
    private final AtomicInteger indexReads = new AtomicInteger();
    private CommitState stateAfterRoot;
    private ManifestRoot rootReadOverride;
    private boolean missingPage;
    private boolean wrongEvent;
    private CompletableFuture<Optional<PreparedEvent>> pendingEvent;
    private Consumer<RequestScope> indexReadObserver = ignored -> {};

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
            default -> invokeDefault(proxy, method, arguments);
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
            if (method.getName().equals("read")) {
              indexReads.incrementAndGet();
              indexReadObserver.accept((RequestScope) arguments[1]);
              return CompletableFuture.completedFuture(index.get());
            }
            return invokeDefault(proxy, method, arguments);
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
            return invokeDefault(proxy, method, arguments);
          });
    }

    private CommitDecisionRepository decisions(CommitDecision archived) {
      return (CommitDecisionRepository) Proxy.newProxyInstance(
          CommitDecisionRepository.class.getClassLoader(), new Class<?>[] {CommitDecisionRepository.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) {
              decisionReads.incrementAndGet();
              return CompletableFuture.completedFuture(Optional.of(archived));
            }
            return invokeDefault(proxy, method, arguments);
          });
    }

    private CommitDecisionRepository decisionsBySequence(Map<Long, CommitDecision> archived) {
      return (CommitDecisionRepository) Proxy.newProxyInstance(
          CommitDecisionRepository.class.getClassLoader(), new Class<?>[] {CommitDecisionRepository.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) {
              return CompletableFuture.completedFuture(Optional.ofNullable(archived.get(arguments[0])));
            }
            return invokeDefault(proxy, method, arguments);
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
