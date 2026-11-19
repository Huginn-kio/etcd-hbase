package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ProtectedEventCursor;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class WatchHistoryReplayTest {
  private static final String TABLE_GROUP = "watch-history-test";
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final int RESPONSE_BYTES = 1 << 20;

  @Test
  void closeAttemptsRemainingDeliveryReleasesAndPropagatesAggregatedFailures() throws Exception {
    var fixture = new ReplayFixture();
    var firstFailure = new AssertionError("first delivery release failed");
    var secondFailure = new IllegalStateException("second delivery release failed");
    var thirdReleased = new AtomicBoolean();
    var deliveries = List.of(
        delivery(() -> { throw firstFailure; }),
        delivery(() -> { throw secondFailure; }),
        delivery(() -> thirdReleased.set(true)));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      ArrayDeque<WatchStream.Delivery> pending = pendingDeliveries(replay);
      pending.addAll(deliveries);

      AssertionError actual = assertThrows(AssertionError.class, replay::close);

      assertSame(firstFailure, actual);
      assertEquals(1, actual.getSuppressed().length);
      assertSame(secondFailure, actual.getSuppressed()[0]);
      assertTrue(thirdReleased.get(), "later delivery release must run after an Error");
      assertTrue(pending.isEmpty(), "pending deliveries must be cleared after release attempts");
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void replaysInclusiveStartInRevisionAndOrdinalOrder() throws Exception {
    var fixture = new ReplayFixture();
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);

      List<Position> actual = drainReplay(replay);

      assertEquals(List.of(
          new Position(4, 3, 0), new Position(4, 3, 1),
          new Position(6, 4, 0), new Position(6, 4, 1)), actual);
      assertTrue(replay.complete());
      verify(fixture.pin).close();
      replay.close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void replayToLiveHandoffDropsTheWholeBarrierSequenceWithoutGapsOrDuplicates()
      throws Exception {
    var fixture = new ReplayFixture();
    try (var startup = RequestScope.withoutDeadline(); var streamScope = RequestScope.withoutDeadline()) {
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(fixture.snapshot)));
      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, fixture.budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          fixture.pins, fixture.selected, AUTHORIZATION);
      SharedEventTailer tailer = mock(SharedEventTailer.class);
      SharedEventTailer.Subscription live = mock(SharedEventTailer.Subscription.class);
      when(tailer.isClosed()).thenReturn(false);
      var queued = new ArrayDeque<SharedEventTailer.Delivery>();
      SharedEventTailer.Delivery barrierFirst = liveDelivery(fixture.events.get(6L).get(0), 6);
      SharedEventTailer.Delivery barrierSecond = liveDelivery(fixture.events.get(6L).get(1), 6);
      SharedEventTailer.Delivery afterBarrier = liveDelivery(fixture.events.get(7L).get(0), 7);
      queued.add(barrierFirst);
      queued.add(barrierSecond);
      queued.add(afterBarrier);
      when(live.poll()).thenAnswer(ignored -> Optional.ofNullable(queued.pollFirst()));
      when(live.termination()).thenReturn(Optional.empty());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(live, 0, 1));

      Rpc.WatchCreateRequest request = Rpc.WatchCreateRequest.newBuilder()
          .setStartRevision(3).build();
      WatchStream.OpenedWatch opened = tailerSession(gateway, tailer, streamScope)
          .subscribe(KeyRange.all(), request).toCompletableFuture().join();
      assertTrue(opened.historicalReplaySupported());

      List<Position> actual = new ArrayList<>();
      for (int attempt = 0; attempt < 16 && actual.size() < 5; attempt++) {
        Optional<WatchStream.Delivery> next = opened.subscription().poll();
        if (next.isEmpty()) continue;
        try (WatchStream.Delivery delivery = next.orElseThrow()) {
          actual.add(new Position(delivery.sequence(), delivery.revision(), delivery.ordinal()));
        }
      }

      assertEquals(List.of(
          new Position(4, 3, 0), new Position(4, 3, 1),
          new Position(6, 4, 0), new Position(6, 4, 1),
          new Position(7, 5, 0)), actual);
      verify(barrierFirst).close();
      verify(barrierSecond).close();
      verify(afterBarrier).close();
      opened.subscription().close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void compactAdvancingDuringReplayRegistrationReturnsPerWatchCancellationBoundary()
      throws Exception {
    var fixture = new ReplayFixture();
    CommitState compacted = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION,
        ByteKey.copyOf(new byte[32]), Optional.empty(), 0, 0, 2, 8, 6, 8, 6,
        Optional.empty(), 4);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(compacted));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay.OpenResult result = WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 3,
          fixture.snapshot, 6, AUTHORIZATION, fixture.pins, fixture.selected,
          fixture.budgets, watchAccount).join();

      assertEquals(6, result.revision());
      assertEquals(8, result.sequence());
      assertEquals(4, result.compactRevision());
      assertTrue(result.replay().isEmpty(), "a compacted registration has no replay cursor");
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  private static List<Position> drainReplay(WatchHistoryReplay replay) {
    List<Position> actual = new ArrayList<>();
    for (int attempt = 0; attempt < 16 && !replay.complete(); attempt++) {
      Optional<WatchStream.Delivery> next = replay.poll();
      if (next.isEmpty()) {
        assertTrue(replay.terminationReason().isEmpty());
        continue;
      }
      try (WatchStream.Delivery delivery = next.orElseThrow()) {
        actual.add(new Position(delivery.sequence(), delivery.revision(), delivery.ordinal()));
      }
    }
    assertFalse(actual.isEmpty());
    return actual;
  }

  private static SharedEventTailer.Delivery liveDelivery(PreparedEvent event, long sequence) {
    SharedEventTailer.Delivery delivery = mock(SharedEventTailer.Delivery.class);
    when(delivery.event()).thenReturn(event);
    when(delivery.sequence()).thenReturn(sequence);
    when(delivery.revision()).thenReturn(event.current().metadata().modificationRevision());
    return delivery;
  }

  private static WatchStream.Delivery delivery(Runnable release) {
    PreparedEvent event = event(2, 1, 0, 61);
    return new WatchStream.Delivery(1, 2, event, release);
  }

  @SuppressWarnings("unchecked")
  private static ArrayDeque<WatchStream.Delivery> pendingDeliveries(WatchHistoryReplay replay)
      throws ReflectiveOperationException {
    Field pendingField = WatchHistoryReplay.class.getDeclaredField("pending");
    pendingField.setAccessible(true);
    return (ArrayDeque<WatchStream.Delivery>) pendingField.get(replay);
  }

  private static WatchStream.Session tailerSession(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope) {
    try {
      Class<?> sessionType = java.util.Arrays.stream(WatchGateway.class.getDeclaredClasses())
          .filter(type -> type.getSimpleName().equals("TailerSession"))
          .findFirst().orElseThrow();
      Constructor<?> constructor = sessionType.getDeclaredConstructor(
          WatchGateway.class, SharedEventTailer.class, RequestScope.class);
      constructor.setAccessible(true);
      return (WatchStream.Session) constructor.newInstance(gateway, tailer, scope);
    } catch (InvocationTargetException invocation) {
      Throwable cause = invocation.getCause();
      if (cause instanceof RuntimeException runtimeFailure) throw runtimeFailure;
      if (cause instanceof Error fatal) throw fatal;
      throw new AssertionError("could not construct the Watch tailer session", cause);
    } catch (ReflectiveOperationException reflectionFailure) {
      throw new AssertionError("could not invoke the Watch tailer session", reflectionFailure);
    }
  }

  private static void assertNoBudgetLeaks(BudgetManager budgets) {
    BudgetManager.Snapshot snapshot = budgets.snapshot();
    assertEquals(0, snapshot.requests());
    assertEquals(0, snapshot.reservedBytes());
    assertEquals(0, snapshot.allocatedBytes());
  }

  private record Position(long sequence, long revision, long ordinal) {}

  private static final class ReplayFixture {
    private final EventHistoryPinManager pins = mock(EventHistoryPinManager.class);
    private final EventHistoryPinManager.Pin pin = mock(EventHistoryPinManager.Pin.class);
    private final SelectedSequenceReader selected = mock(SelectedSequenceReader.class);
    private final BudgetManager budgets = budgets();
    private final Map<Long, Long> sequenceByRevision = Map.of(3L, 4L, 4L, 6L);
    private final Map<Long, List<PreparedEvent>> events = Map.of(
        4L, List.of(event(3, 4, 0, 31), event(3, 4, 1, 32)),
        6L, List.of(event(4, 6, 0, 41), event(4, 6, 1, 42)),
        7L, List.of(event(5, 7, 0, 51)));
    private final Map<Long, SelectedSequenceReader.Selected> selectedBySequence;
    private final CommitState state = CommitState.restore(
        CommitState.EVENT_INDEX_FORMAT_VERSION, ByteKey.copyOf(new byte[32]), Optional.empty(),
        0, 0, 1, 6, 4, 6, 4, Optional.empty(), 0);
    private final ControlSnapshot snapshot = new ControlSnapshot(state, new byte[] {1});

    private ReplayFixture() {
      selectedBySequence = Map.of(
          4L, selected(3, 4, events.get(4L)),
          6L, selected(4, 6, events.get(6L)));
      when(pins.register(any(EventHistoryPinManager.Requirement.class), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(pin));
      when(pin.renew(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(null));
      when(selected.readPublishedState(any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.completedFuture(state));
      when(selected.sequenceForRevision(anyLong(), any(CommitState.class),
          any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
          .thenAnswer(invocation -> CompletableFuture.completedFuture(
              sequenceByRevision.get(invocation.<Long>getArgument(0))));
      when(selected.open(anyLong(), anyLong(), any(ByteKey.class), any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenAnswer(invocation -> CompletableFuture.completedFuture(Optional.of(
              selectedBySequence.get(invocation.<Long>getArgument(0)))));
    }

    private BudgetManager.Account admitWatchAccount(RequestScope scope) throws BackendFailure {
      return budgets.admit(TABLE_GROUP, scope,
          new BudgetManager.Reservation(0, 1, 1, RESPONSE_BYTES, 0));
    }

    private WatchHistoryReplay openReplay(BudgetManager.Account watchAccount,
        KeyRange range, long startRevision) {
      return WatchHistoryReplay.open(TABLE_GROUP, range, startRevision, snapshot, 6,
          AUTHORIZATION, pins, selected, budgets, watchAccount).join().replay().orElseThrow();
    }

    private static SelectedSequenceReader.Selected selected(long revision, long sequence,
        List<PreparedEvent> events) {
      TransactionId transaction = new TransactionId(new UUID(1, sequence));
      CommitDecision decision = new CommitDecision(transaction,
          new InvocationId(new UUID(2, sequence)), sequence, revision - 1, revision,
          ByteKey.copyOf(new byte[] {(byte) revision}), ByteKey.copyOf(new byte[32]), true);
      long totalBytes = events.stream().mapToLong(PreparedEvent::retainedBytes).sum();
      EventReference reference = new EventReference(ByteKey.copyOf(new byte[] {(byte) sequence}),
          ByteKey.copyOf(new byte[32]), 0, events.size() - 1, events.size(), totalBytes, 1, 0);
      var index = new EventRevisionIndex(decision, reference);
      ProtectedEventCursor cursor = mock(ProtectedEventCursor.class);
      var remaining = new ArrayDeque<>(events);
      when(cursor.next(any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(ignored ->
          CompletableFuture.completedFuture(Optional.ofNullable(remaining.pollFirst())));
      when(cursor.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));
      return new SelectedSequenceReader.Selected(decision, Optional.of(cursor), Optional.of(index));
    }

    private static BudgetManager budgets() {
      long memory = 128L << 20;
      var limits = new BudgetManager.Limits(16, 16, 16, 8, 8, 1L << 20, memory);
      var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 80L << 20);
      return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
          reserved, Map.of(TABLE_GROUP, reserved));
    }
  }

  private static PreparedEvent event(long revision, long sequence, long ordinal, int key) {
    byte[] keyBytes = {(byte) key};
    byte[] value = {(byte) (key + 1)};
    StoredRecord current = StoredRecord.live(ByteKey.copyOf(keyBytes),
        new StorageMetadata(1, revision, ordinal + 1, 0, StorageMetadata.State.LIVE,
            value.length), value);
    return new PreparedEvent(new TransactionId(new UUID(1, sequence)), 1, sequence - 1,
        revision - 1, ordinal, PreparedEvent.Kind.PUT, current, Optional.empty());
  }
}
