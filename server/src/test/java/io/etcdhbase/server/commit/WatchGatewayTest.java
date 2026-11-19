package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WatchGatewayTest {
  private static final String TABLE_GROUP = "watch-test";
  private static final int RESPONSE_BYTES = 64 * 1024;

  @Test
  void mapsEveryTailerTerminationToItsWatchCancellationReason() {
    assertEquals("etcd-hbase: Watch subscription was cancelled",
        WatchGateway.cancelReason(SharedEventTailer.Termination.CANCELLED));
    assertEquals("etcd-hbase: Watch subscription resource limit exceeded",
        WatchGateway.cancelReason(SharedEventTailer.Termination.RESOURCE_LIMIT));
    assertEquals("etcd-hbase: Watch event source is unavailable",
        WatchGateway.cancelReason(SharedEventTailer.Termination.TAILER_CLOSED));
  }

  @Test
  void tailerSubscribeFailureReleasesWatchReservationAndStateMemory() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var failure = new IllegalStateException("tailer rejected subscription");
      doThrow(failure).when(tailer).subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());

      assertSame(failure, assertThrows(IllegalStateException.class,
          () -> subscribe(gateway, tailer, scope)));
      assertReleased(budgets);
    }
  }

  @Test
  void tailerShutdownRacePreservesUnavailableAndReleasesWatchReservation() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      doThrow(failure).when(tailer).subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());

      assertSame(failure, assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope)));
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure.category());
      assertReleased(budgets);
    }
  }

  @Test
  void checkedAdmissionFailureReleasesAccountBeforeItEscapes() throws Exception {
    AtomicLong clock = new AtomicLong();
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withTimeoutNanos(2, clock::getAndIncrement)) {
      var tailer = openTailer();
      var budgets = budgets();
      var gateway = gateway(startup, budgets);

      BackendFailure failure = assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope));

      assertEquals(BackendFailure.Category.TIMEOUT, failure.category());
      assertReleased(budgets);
    }
  }

  @Test
  void cleanupFailuresAreSuppressedAndDoNotPreventOtherResourcesFromReleasing() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      var budgets = budgets();
      var control = mock(CommitControlStore.class);
      var barrierFailure = new IllegalStateException("control barrier read failed");
      when(control.read(scope)).thenReturn(CompletableFuture.failedFuture(barrierFailure));
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var cleanupFailure = new IllegalStateException("subscription cleanup failed");
      doThrow(cleanupFailure).when(subscription).close();
      when(tailer.subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 1));

      CompletionException failure = assertThrows(CompletionException.class,
          () -> subscribe(gateway, tailer, scope));

      assertEquals(1, failure.getSuppressed().length);
      assertSame(cleanupFailure, failure.getSuppressed()[0]);
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void gatewayChargesRetainedRangeBytesAndReleasesThemOnClose() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      byte[] lower = new byte[1_024];
      byte[] upper = new byte[1_024];
      upper[0] = 1;
      KeyRange range = KeyRange.between(ByteKey.copyOf(lower), ByteKey.copyOf(upper));
      long retainedBytes = 512 + lower.length + upper.length;
      var budgets = budgetsWithMemory(4_096);
      var gateway = gateway(startup, budgets, 1);

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, range);
      assertEquals(retainedBytes, budgets.snapshot().allocatedBytes(),
          "Gateway accounts for the retained range bounds and active watch state");
      opened.subscription().close();
      assertReleased(budgets);

      var smallBudgets = budgetsWithMemory(1_024);
      var tooSmall = gateway(startup, smallBudgets, 1);
      var rejectedTailer = openTailer();
      BackendFailure rejected = assertThrows(BackendFailure.class,
          () -> subscribe(tooSmall, rejectedTailer, scope, range));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, rejected.category());
      verify(rejectedTailer, never()).subscribeWithCheckpoint(any(KeyRange.class),
          any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());
      assertReleased(smallBudgets);
    }
  }

  @Test
  void passesInclusiveStartRevisionToTailerSoEarlierEventsDoNotConsumeWatchBudget() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 3, 12));
      var gateway = gateway(startup, budgets());
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(15).build();

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, KeyRange.all(), request);

      assertEquals(12, opened.revision());
      verify(tailer).subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH), eq((long) RESPONSE_BYTES), eq(15L));
      opened.subscription().close();
    }
  }

  @Test
  void currentRevisionReadsTheAuthoritativeControlSnapshotInsteadOfTheTailerCheckpoint()
      throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      when(tailer.completedRevision()).thenReturn(3L);
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 2, 3));
      var control = mock(CommitControlStore.class);
      when(control.read(scope)).thenReturn(
          CompletableFuture.completedFuture(Optional.of(snapshot(5, 0))),
          CompletableFuture.completedFuture(Optional.of(snapshot(9, 0))));
      var gateway = gateway(startup, budgets(), RESPONSE_BYTES, control);
      var session = session(gateway, tailer, scope);
      var opened = session.subscribe(KeyRange.all(), Rpc.WatchCreateRequest.getDefaultInstance())
          .toCompletableFuture().join();

      assertEquals(5, opened.revision(), "create captures the current control revision");
      assertEquals(3, session.completedRevision(), "the tailer remains at its older completed checkpoint");
      assertEquals(9, session.currentRevision().toCompletableFuture().join(),
          "cancel acknowledgements read the current control revision");
      verify(control, org.mockito.Mockito.times(2)).read(scope);
      opened.subscription().close();
    }
  }

  @Test
  void passesNegativeStartRevisionAndCompactBoundaryAndReleasesBudgetsOnClose() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 3, 4));
      var budgets = budgetsWithMemory(515);
      var gateway = gateway(startup, budgets, 1, control(4, 2));
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(-2).build();

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, KeyRange.all(), request);

      assertEquals(4, opened.revision());
      assertEquals(2, opened.compactRevision());
      assertEquals(512, budgets.snapshot().allocatedBytes());
      verify(tailer).subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH), eq(1L), eq(-2L));

      opened.subscription().close();
      assertReleased(budgets);
      opened.subscription().close();
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void subscriptionCloseErrorStillReleasesBudgetsAndIsPropagated() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var opened = subscribe(gateway, tailer, scope);
      var closeFailure = new AssertionError("subscription cleanup failed");
      doThrow(closeFailure).when(subscription).close();

      assertSame(closeFailure,
          assertThrows(AssertionError.class, opened.subscription()::close));
      assertReleased(budgets);

      opened.subscription().close();
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1})
  void nonPositiveStartRegistersBeforeTheBarrierAndDropsOnlyEventsAtOrBeforeIt(
      long startRevision) throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      when(tailer.completedRevision()).thenReturn(3L);
      var sharedSubscription = mock(SharedEventTailer.Subscription.class);
      var eventAtBarrier = mock(SharedEventTailer.Delivery.class);
      var eventAfterBarrier = mock(SharedEventTailer.Delivery.class);
      var queued = new java.util.ArrayDeque<SharedEventTailer.Delivery>();
      queued.add(eventAtBarrier);
      queued.add(eventAfterBarrier);
      when(eventAtBarrier.revision()).thenReturn(4L);
      when(eventAtBarrier.event()).thenReturn(event(4));
      when(eventAfterBarrier.revision()).thenReturn(5L);
      when(eventAfterBarrier.event()).thenReturn(event(5));
      when(sharedSubscription.poll()).thenAnswer(ignored -> Optional.ofNullable(queued.pollFirst()));
      when(sharedSubscription.termination()).thenReturn(Optional.empty());
      doAnswer(invocation -> {
        long minimumRevision = invocation.getArgument(0);
        var iterator = queued.iterator();
        while (iterator.hasNext()) {
          var delivery = iterator.next();
          if (delivery.revision() < minimumRevision) {
            iterator.remove();
            delivery.close();
          }
        }
        return null;
      }).when(sharedSubscription).requireRevisionAtLeast(anyLong());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(sharedSubscription, 2, 3));

      var control = mock(CommitControlStore.class);
      var barrier = new CompletableFuture<Optional<ControlSnapshot>>();
      when(control.read(scope)).thenReturn(barrier);
      var budgets = budgets();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var request = Rpc.WatchCreateRequest.newBuilder().setWatchId(19)
          .setStartRevision(startRevision).build();

      var opening = subscribeAsync(gateway, tailer, scope, KeyRange.all(), request);
      assertFalse(opening.toCompletableFuture().isDone(),
          "the create cannot publish before the authoritative barrier read completes");
      var order = inOrder(tailer, control);
      order.verify(tailer).subscribeWithCheckpoint(any(KeyRange.class),
          any(BudgetManager.Account.class), eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH),
          eq((long) RESPONSE_BYTES), eq(startRevision));
      order.verify(control).read(scope);

      barrier.complete(Optional.of(snapshot(4, 2)));
      WatchStream.OpenedWatch opened = opening.toCompletableFuture().join();

      assertEquals(4, opened.revision());
      assertEquals(2, opened.compactRevision(), "compact boundary comes from the same control snapshot");
      verify(sharedSubscription).requireRevisionAtLeast(5L);
      verify(eventAtBarrier).close();
      try (var delivery = opened.subscription().poll().orElseThrow()) {
        assertEquals(5, delivery.revision(), "B+1 is retained after raising the floor");
      }
      verify(eventAfterBarrier).close();
      opened.subscription().close();
      assertReleased(budgets);
    }
  }

  @Test
  void pollCancellationHandoffClosesTheDetachedDeliveryBeforeReleasingItsAccount() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var sharedSubscription = mock(SharedEventTailer.Subscription.class);
      var sharedDelivery = mock(SharedEventTailer.Delivery.class);
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var wrappedSubscription = new AtomicReference<WatchStream.Subscription>();
      var memory = new AtomicReference<BudgetManager.Lease>();
      PreparedEvent event = event();
      when(sharedDelivery.revision()).thenReturn(4L);
      when(sharedDelivery.event()).thenReturn(event);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong())).thenAnswer(invocation -> {
            BudgetManager.Account account = invocation.getArgument(1);
            memory.set(account.tryRetain(BudgetManager.Memory.OUTPUT, event.retainedBytes()).orElseThrow());
            doAnswer(closing -> {
              memory.get().close();
              return null;
            }).when(sharedDelivery).close();
            return new SharedEventTailer.SubscriptionStart(sharedSubscription, 3, 3);
          });
      when(sharedSubscription.poll()).thenAnswer(invocation -> {
        wrappedSubscription.get().close();
        return Optional.of(sharedDelivery);
      });

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope);
      wrappedSubscription.set(opened.subscription());
      assertTrue(opened.subscription().poll().isEmpty());

      verify(sharedDelivery).close();
      assertReleased(budgets);
    }
  }

  @Test
  void workerRejectionAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources()
      throws Exception {
    assertWorkerFailureAfterWatchOpensReleasesResources(
        new RejectedExecutionException("completion worker rejected the Watch"),
        Status.Code.RESOURCE_EXHAUSTED, "backend resource limit was reached");
  }

  @Test
  void workerErrorAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources() throws Exception {
    assertWorkerFailureAfterWatchOpensReleasesResources(
        new AssertionError("completion worker failed fatally"),
        Status.Code.INTERNAL, "backend operation failed");
  }

  private static void assertWorkerFailureAfterWatchOpensReleasesResources(
      Throwable workerFailure, Status.Code expectedCode, String expectedDescription)
      throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      var barrier = new CompletableFuture<Optional<ControlSnapshot>>();
      var control = mock(CommitControlStore.class);
      when(control.read(scope)).thenReturn(barrier);
      var budgets = budgets();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var worker = new ToggleRejectingExecutor();
      var observer = new RecordingWatchObserver();
      var stream = new WatchStream(observer, scope, worker, mock(ScheduledExecutorService.class),
          () -> CompletableFuture.completedFuture(session(gateway, tailer, scope)),
          11, 12, RESPONSE_BYTES, new CompactedWatchIdBudget(8));

      stream.onNext(Rpc.WatchRequest.newBuilder().setCreateRequest(
          Rpc.WatchCreateRequest.newBuilder().setWatchId(7)
              .setKey(com.google.protobuf.ByteString.copyFromUtf8("key"))).build());

      verify(control).read(scope);
      assertTrue(budgets.snapshot().allocatedBytes() > 0,
          "the delayed control barrier leaves the accepted subscription and state memory retained");
      worker.rejection = workerFailure;
      barrier.complete(Optional.of(snapshot(4, 0)));

      verify(subscription).close();
      assertReleased(budgets);
      assertNotNull(observer.failure, "the worker failure is delivered through the RPC error path");
      Status status = Status.fromThrowable(observer.failure);
      assertEquals(expectedCode, status.getCode());
      assertEquals(expectedDescription, status.getDescription());
      assertFalse(status.getDescription().contains(workerFailure.getMessage()),
          "the RPC error must not expose the executor failure details");
    }
  }

  private static SharedEventTailer openTailer() {
    SharedEventTailer tailer = mock(SharedEventTailer.class);
    when(tailer.isClosed()).thenReturn(false);
    return tailer;
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 2L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static BudgetManager budgetsWithMemory(long memoryBytes) {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, memoryBytes);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets) {
    return gateway(startup, budgets, RESPONSE_BYTES);
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets,
      int maximumResponseBytes) {
    return gateway(startup, budgets, maximumResponseBytes, control(12, 0));
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets,
      int maximumResponseBytes, CommitControlStore control) {
    var pending = new M3TableGroupRuntime.Pending(startup, null, true);
    return new WatchGateway(TABLE_GROUP, pending, control, budgets, Runnable::run,
        mock(ScheduledExecutorService.class), 11, 12, maximumResponseBytes);
  }

  private static CommitControlStore control(long revision, long compactRevision) {
    var control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(snapshot(revision, compactRevision))));
    return control;
  }

  private static ControlSnapshot snapshot(long revision, long compactRevision) {
    CommitState state = CommitState.restore(CommitState.FORMAT_VERSION, ByteKey.copyOf(new byte[32]),
        Optional.empty(), 0, 0, 1, 0, revision, 0, revision, Optional.empty(), compactRevision);
    return new ControlSnapshot(state, new byte[] {1});
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope) throws BackendFailure {
    return subscribe(gateway, tailer, scope, KeyRange.all(), Rpc.WatchCreateRequest.getDefaultInstance());
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope, KeyRange range) throws BackendFailure {
    return subscribe(gateway, tailer, scope, range, Rpc.WatchCreateRequest.getDefaultInstance());
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope, KeyRange range,
      Rpc.WatchCreateRequest request) throws BackendFailure {
    return subscribeAsync(gateway, tailer, scope, range, request).toCompletableFuture().join();
  }

  private static java.util.concurrent.CompletionStage<WatchStream.OpenedWatch> subscribeAsync(
      WatchGateway gateway, SharedEventTailer tailer, RequestScope scope, KeyRange range,
      Rpc.WatchCreateRequest request) throws BackendFailure {
    return session(gateway, tailer, scope).subscribe(range, request);
  }

  private static WatchStream.Session session(WatchGateway gateway, SharedEventTailer tailer,
      RequestScope scope) {
    try {
      Class<?> sessionType = Arrays.stream(WatchGateway.class.getDeclaredClasses())
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

  private static void assertReleased(BudgetManager budgets) {
    BudgetManager.Snapshot snapshot = budgets.snapshot();
    assertEquals(0, snapshot.requests());
    assertEquals(0, snapshot.reservedBytes());
    assertEquals(0, snapshot.allocatedBytes());
  }

  private static final class ToggleRejectingExecutor implements Executor {
    private Throwable rejection;

    @Override public void execute(Runnable command) {
      if (rejection instanceof RuntimeException runtimeFailure) throw runtimeFailure;
      if (rejection instanceof Error fatal) throw fatal;
      command.run();
    }
  }

  private static final class RecordingWatchObserver implements StreamObserver<Rpc.WatchResponse> {
    private Throwable failure;

    @Override public void onNext(Rpc.WatchResponse response) {}

    @Override public void onError(Throwable cause) { failure = cause; }

    @Override public void onCompleted() {}
  }

  private static PreparedEvent event() { return event(4); }

  private static PreparedEvent event(long revision) {
    byte[] key = {1};
    byte[] value = {2, 3};
    var stored = StoredRecord.live(ByteKey.copyOf(key),
        new StorageMetadata(1, revision, revision, 0, StorageMetadata.State.LIVE, value.length), value);
    return new PreparedEvent(new TransactionId(new UUID(0, revision)), 1, revision - 1,
        revision - 1, 0,
        PreparedEvent.Kind.PUT, stored, Optional.empty());
  }
}
