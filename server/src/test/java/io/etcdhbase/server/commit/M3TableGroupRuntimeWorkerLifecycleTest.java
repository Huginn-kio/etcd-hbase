package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.request.RequestScope;
import org.junit.jupiter.api.Test;

class M3TableGroupRuntimeWorkerLifecycleTest {
  @Test
  void closedTailerCanBeDetachedAndRecreatedWithoutRemovingTheNewGeneration() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, true);
      var first = mock(SharedEventTailer.class);
      when(first.isClosed()).thenReturn(false, true);
      assertSame(first, pending.installEventTailer(first));
      var firstReady = pending.eventTailerReady();
      assertSame(first, firstReady.join());

      assertFalse(pending.detachClosedEventTailer(first));
      assertSame(first, pending.eventTailer().orElseThrow());
      assertTrue(pending.detachClosedEventTailer(first));
      assertTrue(pending.eventTailer().isEmpty());

      var nextReady = pending.eventTailerReady();
      assertFalse(nextReady.isDone());
      var replacement = mock(SharedEventTailer.class);
      assertSame(replacement, pending.installEventTailer(replacement));
      assertSame(replacement, nextReady.join());
      assertSame(first, firstReady.join());
      assertFalse(pending.detachClosedEventTailer(first));
      assertSame(replacement, pending.eventTailer().orElseThrow());
    }
  }

  @Test
  void ownerLossBetweenWorkerInstallationsStopsExistingWorkerAndRejectsLateWorker() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var history = mock(EventHistoryWorker.class);
      pending.startExpiryWorker(expiry);

      pending.ownerLost();

      verify(expiry).stop();
      assertThrows(IllegalStateException.class, () -> pending.startEventHistoryWorker(history));
      verify(history, never()).start();
    }
  }

  @Test
  void ownerLossBeforeInstallationRejectsBothWorkers() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var history = mock(EventHistoryWorker.class);
      pending.ownerLost();

      assertThrows(IllegalStateException.class, () -> pending.startExpiryWorker(expiry));
      assertThrows(IllegalStateException.class, () -> pending.startEventHistoryWorker(history));
      verify(expiry, never()).start();
      verify(history, never()).start();
    }
  }
}
