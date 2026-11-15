package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseExpiryWorkerTest {
  @Test
  void cleanupExecutorRejectionStillWaitsForPhysicalProducerDrain() {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      CompletableFuture<Void> physicalProducer = new CompletableFuture<>();
      scope.trackPhysicalProducer(physicalProducer);

      CompletableFuture<Void> drain = LeaseExpiryWorker.closeAndDrain(scope, ignored -> {
        throw new RejectedExecutionException("cleanup executor is stopping");
      });

      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone(),
          "RequestScope must run its rejected cleanup batch inline");
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertFalse(drain.isDone(), "worker drain must retain ownership of the physical producer");

      physicalProducer.complete(null);

      CompletionException failure = assertThrows(CompletionException.class, drain::join);
      assertTrue(failure.getCause() instanceof RejectedExecutionException);
    }
  }

  @Test
  void successfulCloseWaitsForBothCleanupAndPhysicalProducerCompletion() {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      CompletableFuture<Void> physicalProducer = new CompletableFuture<>();
      scope.trackPhysicalProducer(physicalProducer);

      CompletableFuture<Void> drain = LeaseExpiryWorker.closeAndDrain(scope, Runnable::run);

      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertFalse(drain.isDone());
      physicalProducer.complete(null);
      drain.join();
      assertTrue(drain.isDone());
    }
  }

  @Test
  void staleExpiryWorkerDoesNotForwardAfterOwnerChangesDuringCheckpointRead() throws Exception {
    CommitOwnerService.OwnerToken oldToken = new CommitOwnerService.OwnerToken("owner-a", 7);
    CommitOwnerService.OwnerToken newToken = new CommitOwnerService.OwnerToken("owner-b", 8);
    AtomicReference<CommitOwnerService.OwnerToken> currentToken = new AtomicReference<>(oldToken);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenAnswer(ignored -> Optional.of(currentToken.get()));
    CompletableFuture<Optional<ControlSnapshot>> checkpointRead = new CompletableFuture<>();
    CommitControlStore control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class))).thenReturn(checkpointRead);
    SystemRecordStore records = mock(SystemRecordStore.class);
    MutationRelay relay = mock(MutationRelay.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    AtomicReference<Runnable> scheduledTick = new AtomicReference<>();
    AtomicInteger scheduledCount = new AtomicInteger();
    var retryScheduled = new java.util.concurrent.CountDownLatch(1);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(invocation -> {
          scheduledTick.set(invocation.getArgument(0));
          if (scheduledCount.getAndIncrement() > 0) retryScheduled.countDown();
          return future;
        });
    BudgetManager.Limits limits = new BudgetManager.Limits(2, 0, 2, 1, 2, 0, 64L << 10);
    BudgetManager budgets = new BudgetManager(limits, java.util.Map.of("test-group", limits), limits);
    var identity = new AuthorizedBackendIdentity("test-provider", "test-subject", "fixture-context",
        new AuthenticationScope("test-group", "test-profile"), Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, relay, identity, (LeaseTimeSource) () -> observation,
        budgets, new BudgetManager.Reservation(1, 1, 1, 1, 1), Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    worker.start();
    scheduledTick.get().run();
    assertFalse(checkpointRead.isDone(), "the old owner's checkpoint read must be held at the barrier");

    currentToken.set(newToken);
    checkpointRead.complete(Optional.of(new ControlSnapshot(checkpointState(0), new byte[] {1})));
    assertTrue(retryScheduled.await(1, TimeUnit.SECONDS),
        "the expired owner cycle must finish before another poll is scheduled");
    worker.stop();
    worker.drained().get(1, TimeUnit.SECONDS);

    verify(relay, never()).forward(any(), any());
  }

  @Test
  void asynchronousCheckpointReadFailureClosesPreviouslyEstablishedOwnerReadiness() {
    CommitOwnerService.OwnerToken token = new CommitOwnerService.OwnerToken("owner-a", 7);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(token));
    CommitControlStore control = mock(CommitControlStore.class);
    SystemRecordStore records = mock(SystemRecordStore.class);
    CommitState initialState = checkpointState(0);
    CommitState advancedState = checkpointState(1);
    when(control.read(any(RequestScope.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(initialState, new byte[] {1}))),
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(advancedState, new byte[] {2}))));
    when(records.latestAtOrBefore(eq(SystemRecordType.LEASE_EXPIRY_CHECKPOINT),
        any(ByteKey.class), eq(1L), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("checkpoint read failed")));
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledTick = mock(ScheduledFuture.class);
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(ignored -> scheduledTick);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, mock(MutationRelay.class), identity,
        (LeaseTimeSource) () -> observation, mock(BudgetManager.class),
        new BudgetManager.Reservation(1, 1, 1, 1, 1), Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    CompletableFuture<Void> initialReadiness = worker.initialReadiness();
    worker.start();
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertFalse(initialReadiness.isDone(),
          "an owner runtime must not become ready before its persisted checkpoint is inspected");
      worker.inspectCheckpoint(token, observation, scope).join();
      initialReadiness.join();
      assertTrue(worker.isReady(), "a valid checkpoint read establishes readiness for this owner epoch");

      CompletionException failure = assertThrows(CompletionException.class,
          () -> worker.inspectCheckpoint(token, observation, scope).join());
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertFalse(worker.isReady(), "an asynchronous checkpoint read failure must revoke readiness");
    } finally {
      worker.stop();
    }
  }

  @Test
  void healthyStandbyWorkerSignalsInitialReadinessWithoutAnOwnerCheckpoint() throws Exception {
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.empty());
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    try (ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1)) {
      var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
          10_000, mock(CommitControlStore.class), mock(SystemRecordStore.class), owner,
          mock(MutationRelay.class), identity, (LeaseTimeSource) () -> observation,
          mock(BudgetManager.class), new BudgetManager.Reservation(1, 1, 1, 1, 1),
          Runnable::run, timer, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
          new RequestMetrics());

      worker.start();
      try {
        worker.initialReadiness().get(1, TimeUnit.SECONDS);
        assertTrue(worker.isReady(),
            "a healthy standby may serve local protocol paths without owning expiry mutations");
      } finally {
        worker.stop();
        worker.drained().get(1, TimeUnit.SECONDS);
      }
    }
  }

  private static CommitState checkpointState(long finalizedSequence) {
    CommitState state = mock(CommitState.class);
    when(state.supportsLeaseExpiryCheckpoints()).thenReturn(true);
    when(state.ownerEpoch()).thenReturn(7L);
    when(state.ownerInstance()).thenReturn(Optional.of("owner-a"));
    when(state.ownerLeaseExpired(anyLong())).thenReturn(false);
    when(state.publication()).thenReturn(Optional.empty());
    when(state.finalizedSequence()).thenReturn(finalizedSequence);
    return state;
  }
}
