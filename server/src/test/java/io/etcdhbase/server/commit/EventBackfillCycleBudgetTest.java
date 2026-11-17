package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventBackfillCycleBudgetTest {
  @Test
  void cycleFailsClosedWhenItsReservedAccountCannotBeAdmitted() {
    var limits = new BudgetManager.Limits(16, 16, 32, 16, 32, 1L << 20, 256L << 20);
    var reserve = new BudgetManager.ReservedCapacity(1, 0, 1, 0, 1, 0, 2);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits,
        reserve, Map.of("group", reserve));
    var stores = mock(TableGroupStores.class);
    when(stores.control()).thenReturn(mock(HBaseCommitControlStore.class));
    var pins = mock(EventHistoryPinManager.class);
    when(pins.namespace()).thenReturn(Optional.of(ByteKey.copyOf(new byte[16])));
    var cycle = new EventBackfillCycle("group", mock(SerialMutationQueue.class), stores, pins,
        mock(InvocationService.class),
        mock(EventBackfillPreparation.class), mock(CommitPublisher.class), budgets,
        ByteKey.copyOf(new byte[32]), Clock.systemUTC());
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var failure = assertThrows(CompletionException.class,
          () -> cycle.run(new CommitOwnerService.OwnerToken("owner", 1), scope).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          assertInstanceOf(BackendFailure.class, failure.getCause()).category());
    }
  }
}
