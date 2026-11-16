package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.request.RequestScope;
import org.junit.jupiter.api.Test;

class M3TableGroupRuntimeWorkerLifecycleTest {
  @Test
  void ownerLossBetweenWorkerInstallationsStopsExistingWorkerAndRejectsLateWorker() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var backfill = mock(EventBackfillWorker.class);
      pending.startExpiryWorker(expiry);

      pending.ownerLost();

      verify(expiry).stop();
      assertThrows(IllegalStateException.class, () -> pending.startBackfillWorker(backfill));
      verify(backfill, never()).start();
    }
  }

  @Test
  void ownerLossBeforeInstallationRejectsBothWorkers() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var backfill = mock(EventBackfillWorker.class);
      pending.ownerLost();

      assertThrows(IllegalStateException.class, () -> pending.startExpiryWorker(expiry));
      assertThrows(IllegalStateException.class, () -> pending.startBackfillWorker(backfill));
      verify(expiry, never()).start();
      verify(backfill, never()).start();
    }
  }
}
