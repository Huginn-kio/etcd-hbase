package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class EventTailerCycleTest {
  private static final String TABLE_GROUP = "group";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final ByteKey NAMESPACE = ByteKey.copyOf(new byte[16]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0});

  @Test
  void installsLiveTailerAtCapturedFinalizedBarrierInsteadOfLegacyWindowUpperBound() throws Exception {
    var captured = state(40, 30);
    var later = state(100, 80);
    var owner = new CommitOwnerService.OwnerToken("owner", 7);
    var control = mock(HBaseCommitControlStore.class);
    var controlReads = new AtomicInteger();
    when(control.read(any(RequestScope.class))).thenAnswer(ignored -> {
      CommitState state = controlReads.getAndIncrement() < 2 ? captured : later;
      return CompletableFuture.completedFuture(Optional.of(snapshot(state)));
    });

    var readPin = mock(ReadViewPinRegistry.Pin.class);
    when(readPin.generation()).thenReturn(1L);
    when(readPin.renew(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(null));
    when(readPin.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));
    var viewPins = mock(ReadViewPinRegistry.class);
    when(viewPins.registerAuxiliary(any(ReadView.class), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(readPin));
    var manifests = mock(ManifestCatalog.class);
    var views = new ReadViewService(control, manifests, viewPins, FINGERPRINT);

    var legacy = new EventIndexBackfillCheckpoint(7, 5, 1, 2, 3, 1, 4, 4, 1, REGISTRY);
    var records = mock(HBaseSystemRecordStore.class);
    when(records.latestAtOrBefore(eq(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT),
        eq(EventIndexBackfillCheckpoint.routePrefix()), eq(40L), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new SystemRecordEntry(
            EventIndexBackfillCheckpoint.versionIdentity(legacy.sequence()), legacy.encode()))));

    var stores = mock(TableGroupStores.class);
    when(stores.control()).thenReturn(control);
    when(stores.views()).thenReturn(views);
    when(stores.records()).thenReturn(records);
    when(stores.manifests()).thenReturn(manifests);
    when(stores.decisions()).thenReturn(mock(HBaseCommitDecisionRepository.class));
    when(stores.manifestRepository()).thenReturn(mock(HBaseManifestRepository.class));
    when(stores.eventIndexes()).thenReturn(new HBaseEventRevisionIndexRepository(records));

    var eventPins = mock(EventHistoryPinManager.class);
    when(eventPins.namespace()).thenReturn(Optional.of(NAMESPACE));
    when(eventPins.registryIdentity()).thenReturn(REGISTRY);
    var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 64L << 20, 1L << 30);
    var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 128L << 20);
    var budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
        reserved, Map.of(TABLE_GROUP, reserved));
    var memoryScope = RequestScope.withoutDeadline();
    var memory = budgets.admitReserved(TABLE_GROUP, memoryScope,
        LeaseBudgetProfile.EVENT_TAILER_MEMORY, LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);

    var ownerLane = mock(SerialMutationQueue.class);
    doAnswer(invocation -> {
      RequestScope laneScope = invocation.getArgument(0);
      @SuppressWarnings("unchecked")
      Function<RequestScope, CompletableFuture<SharedEventTailer>> work = invocation.getArgument(1);
      return work.apply(laneScope);
    }).when(ownerLane).submitBackground(any(RequestScope.class), any());
    var pending = mock(M3TableGroupRuntime.Pending.class);
    when(pending.eventTailer()).thenReturn(Optional.empty());
    when(pending.eventTailerMemory(budgets, TABLE_GROUP)).thenReturn(memory);
    var installed = new AtomicReference<SharedEventTailer>();
    doAnswer(invocation -> {
      SharedEventTailer candidate = invocation.getArgument(0);
      installed.set(candidate);
      return candidate;
    }).when(pending).installEventTailer(any());

    var cycle = new EventTailerCycle(TABLE_GROUP, ownerLane, () -> Optional.of(owner), stores,
        eventPins, budgets, AUTHORIZATION, pending, mock(RequestMetrics.class));
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      cycle.run(Optional.of(owner), scope).join();

      SharedEventTailer tailer = installed.get();
      assertNotNull(tailer);
      assertEquals(40, tailer.completedSequence());
      assertEquals(30, tailer.completedRevision());
      assertEquals(100, tailer.observedPublishedSequence());
      assertEquals(2, legacy.upperSequence());
      assertEquals(4, legacy.upperRevision());
      tailer.close();
    } finally {
      memoryScope.close();
    }
  }

  @Test
  void absentCheckpointWaitsForPhysicalReadWithoutBlockingItsCompletionThread() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(Optional.empty(), scope);
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      CompletableFuture.runAsync(() -> fixture.request.complete(Optional.empty())).get(5, TimeUnit.SECONDS);
      assertFalse(result.isDone());
      verify(fixture.pin, never()).close();
      assertTrue(fixture.budgets.snapshot().reservedBytes() > 0);
      fixture.physical.complete(null);
      verify(fixture.pin).close();
      assertFalse(result.isDone());
      fixture.released.complete(null);
      var value = result.get(5, TimeUnit.SECONDS);
      assertFalse(value.progressed());
      verify(fixture.pin).close();
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  @Test
  void asyncReadFailurePreservesCauseAndWaitsForPhysicalRead() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(Optional.empty(), scope);
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      CompletableFuture.runAsync(() -> fixture.request.completeExceptionally(failure)).get(5, TimeUnit.SECONDS);
      assertFalse(result.isDone());
      verify(fixture.pin, never()).close();
      assertTrue(fixture.budgets.snapshot().reservedBytes() > 0);
      fixture.physical.complete(null);
      verify(fixture.pin).close();
      assertFalse(result.isDone());
      fixture.released.complete(null);
      var observed = assertThrows(CompletionException.class, result::join);
      assertSame(failure, io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(observed));
      verify(fixture.pin).close();
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  @Test
  void successfulReadReportsPinReleaseFailure() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    var releaseFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(Optional.empty(), scope);
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      CompletableFuture.runAsync(() -> fixture.request.complete(Optional.empty())).get(5, TimeUnit.SECONDS);
      fixture.physical.complete(null);
      fixture.pinClosed.get(5, TimeUnit.SECONDS);
      assertFalse(result.isDone());

      fixture.released.completeExceptionally(releaseFailure);

      var observed = assertThrows(CompletionException.class, result::join);
      assertSame(releaseFailure, io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(observed));
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  @Test
  void readFailureRemainsPrimaryWhenPinReleaseAlsoFails() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    var readFailure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    var releaseFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(Optional.empty(), scope);
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      CompletableFuture.runAsync(() -> fixture.request.completeExceptionally(readFailure))
          .get(5, TimeUnit.SECONDS);
      fixture.physical.complete(null);
      fixture.pinClosed.get(5, TimeUnit.SECONDS);
      assertFalse(result.isDone());

      fixture.released.completeExceptionally(releaseFailure);

      var observed = assertThrows(CompletionException.class, result::join);
      assertSame(readFailure, io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(observed));
      assertEquals(1, readFailure.getSuppressed().length);
      assertSame(releaseFailure, readFailure.getSuppressed()[0]);
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  @Test
  void workerStopReportsChildPinReleaseFailureAfterDrain() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    var timer = new ScheduledThreadPoolExecutor(1);
    var worker = new EventHistoryWorker(timer, Optional::empty, fixture.cycle::run, mock(RequestMetrics.class));
    var releaseFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try {
      worker.start();
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      worker.stop();
      assertFalse(worker.drained().isDone());
      verify(fixture.pin, never()).close();

      fixture.physical.complete(null);
      fixture.pinClosed.get(5, TimeUnit.SECONDS);
      assertFalse(worker.drained().isDone());
      fixture.released.completeExceptionally(releaseFailure);

      var observed = assertThrows(ExecutionException.class,
          () -> worker.drained().get(5, TimeUnit.SECONDS));
      assertSame(releaseFailure,
          io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(observed.getCause()));
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
      worker.stop();
      timer.shutdownNow();
      timer.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void workerStopWaitsForPhysicalReadAndPinRelease() throws Exception {
    var fixture = new AsyncCheckpointFixture();
    var timer = new ScheduledThreadPoolExecutor(1);
    var worker = new EventHistoryWorker(timer, Optional::empty, fixture.cycle::run, mock(RequestMetrics.class));
    try {
      worker.start();
      assertTrue(fixture.started.await(5, TimeUnit.SECONDS));
      worker.stop();
      assertFalse(worker.drained().isDone());
      verify(fixture.pin, never()).close();
      assertTrue(fixture.budgets.snapshot().reservedBytes() > 0);
      fixture.physical.complete(null);
      fixture.pinClosed.get(5, TimeUnit.SECONDS);
      verify(fixture.pin).close();
      assertFalse(worker.drained().isDone());
      fixture.released.complete(null);
      worker.drained().get(5, TimeUnit.SECONDS);
      verify(fixture.pin).close();
      assertEquals(0, fixture.budgets.snapshot().reservedBytes());
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
      worker.stop();
      timer.shutdownNow();
      timer.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static final class AsyncCheckpointFixture {
    final CompletableFuture<Optional<SystemRecordEntry>> request = new CompletableFuture<>();
    final CompletableFuture<Void> physical = new CompletableFuture<>();
    final CompletableFuture<Void> released = new CompletableFuture<>();
    final CompletableFuture<Void> pinClosed = new CompletableFuture<>();
    final CountDownLatch started = new CountDownLatch(1);
    final ReadViewPinRegistry.Pin pin = mock(ReadViewPinRegistry.Pin.class);
    final BudgetManager budgets;
    final EventTailerCycle cycle;

    AsyncCheckpointFixture() {
      var control = mock(HBaseCommitControlStore.class);
      when(control.read(any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(snapshot(state(40, 30)))));
      when(pin.generation()).thenReturn(1L);
      when(pin.renew(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(null));
      when(pin.releaseCompletion()).thenReturn(released);
      doAnswer(invocation -> { pinClosed.complete(null); return null; }).when(pin).close();
      var viewPins = mock(ReadViewPinRegistry.class);
      when(viewPins.registerAuxiliary(any(ReadView.class), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(pin));
      var manifests = mock(ManifestCatalog.class);
      var records = mock(HBaseSystemRecordStore.class);
      when(records.latestAtOrBefore(eq(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT),
          eq(EventIndexBackfillCheckpoint.routePrefix()), eq(40L), any(RequestScope.class)))
          .thenAnswer(invocation -> {
            RequestScope child = invocation.getArgument(3);
            child.trackPhysicalProducer(physical);
            child.registerCleanup(() -> request.completeExceptionally(
                new BackendFailure(BackendFailure.Category.CANCELLED)));
            started.countDown();
            return request;
          });
      var stores = mock(TableGroupStores.class);
      when(stores.views()).thenReturn(new ReadViewService(control, manifests, viewPins, FINGERPRINT));
      when(stores.records()).thenReturn(records);
      when(stores.manifests()).thenReturn(manifests);
      var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 64L << 20, 1L << 30);
      var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 128L << 20);
      budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
          reserved, Map.of(TABLE_GROUP, reserved));
      var pending = mock(M3TableGroupRuntime.Pending.class);
      when(pending.eventTailer()).thenReturn(Optional.empty());
      cycle = new EventTailerCycle(TABLE_GROUP, mock(SerialMutationQueue.class), Optional::empty,
          stores, mock(EventHistoryPinManager.class), budgets, AUTHORIZATION, pending, mock(RequestMetrics.class));
    }
  }

  private static ControlSnapshot snapshot(CommitState state) {
    return new ControlSnapshot(state, new byte[] {1});
  }

  private static CommitState state(long finalizedSequence, long finalizedRevision) {
    return CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
        Optional.of("owner"), 7, 10_000, 1, finalizedSequence, finalizedRevision,
        finalizedSequence, finalizedRevision, Optional.empty(), 1,
        Optional.of(NAMESPACE), Optional.empty(), 1);
  }
}
