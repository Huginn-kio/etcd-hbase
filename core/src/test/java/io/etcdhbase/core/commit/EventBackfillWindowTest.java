package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class EventBackfillWindowTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final ByteKey NAMESPACE = ByteKey.copyOf(new byte[16]);

  @Test
  void permitsFinalizedMetadataSequenceChurnButKeepsPublicationAndHistoryFences() throws Exception {
    try (RequestScope setup = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var account = account(scope);
      try (account) {
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 64, 4L << 30);
        var state = new AtomicReference<>(state(7, 2, 2, 4, 4, 1, NAMESPACE));
        var control = control(state);
        var pins = EventHistoryPinManager.stage(new MemoryRecords(), NAMESPACE,
            new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), Clock.systemUTC(), setup).join();
        var planned = new EventIndexBackfillCheckpoint(7, 3, 1, 2, 1, 1, 1, 4, 1,
            pins.registryIdentity());
        var window = EventBackfillWindow.openCapture(pins, control, planned, AUTHORIZATION, scope, budget).join();
        try (window) {
          state.set(state(7, 3, 3, 4, 4, 1, NAMESPACE));

          assertDoesNotThrow(() -> window.checkHistory(planned, 7, budget).join());

          var rebound = planned.rebindPublication(7, 4);
          assertDoesNotThrow(() -> window.checkPublication(rebound, 7, 4, budget).join());
          assertThrows(CompletionException.class,
              () -> window.checkPublication(planned, 7, 3, budget).join());

          state.set(state(8, 3, 3, 4, 4, 1, NAMESPACE));
          assertThrows(CompletionException.class, () -> window.checkHistory(planned, 7, budget).join());
          state.set(state(7, 3, 3, 4, 4, 2, NAMESPACE));
          assertThrows(CompletionException.class, () -> window.checkHistory(planned, 7, budget).join());
          state.set(state(7, 3, 3, 4, 4, 1, ByteKey.copyOf(new byte[] {2, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 0, 0, 0})));
          assertThrows(CompletionException.class, () -> window.checkHistory(planned, 7, budget).join());
          state.set(state(7, 1, 1, 4, 4, 1, NAMESPACE));
          assertThrows(CompletionException.class, () -> window.checkHistory(planned, 7, budget).join());
        } finally {
          window.close();
          window.releaseCompletion().toCompletableFuture().join();
        }
      }
    }
  }

  private static BudgetManager.Account account(RequestScope scope) throws Exception {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 1024L << 20);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
    return budgets.admit("group", scope,
        new BudgetManager.Reservation(8192, 8192, 512L << 20, 8192, 8192),
        new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000));
  }

  private static CommitControlStore control(AtomicReference<CommitState> state) {
    return new CommitControlStore() {
      @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
        return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state.get(), new byte[] {1})));
      }

      @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope scope) {
        throw new AssertionError("unexpected control initialization");
      }

      @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot expected,
          CommitState replacement, RequestScope scope) {
        throw new AssertionError("unexpected control mutation");
      }
    };
  }

  private static CommitState state(long ownerEpoch, long publishedSequence, long finalizedSequence,
      long publishedRevision, long finalizedRevision, long compactRevision, ByteKey namespace) {
    return CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
        Optional.of("owner"), ownerEpoch, 10_000, 1, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, Optional.empty(), compactRevision,
        Optional.of(namespace), Optional.empty(), 1);
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<RecordKey, ByteKey> rows = new HashMap<>();

    @Override public synchronized CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(new RecordKey(type, identity))));
    }

    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }

    @Override public synchronized CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      RecordKey key = new RecordKey(type, identity);
      ByteKey existing = rows.get(key);
      if (existing == null) {
        rows.put(key, value);
        return CompletableFuture.completedFuture(CreateResult.CREATED);
      }
      return CompletableFuture.completedFuture(existing.equals(value)
          ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }

    @Override public synchronized CompletableFuture<Boolean> compareAndSet(SystemRecordType type,
        ByteKey identity, Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      RecordKey key = new RecordKey(type, identity);
      ByteKey existing = rows.get(key);
      if (!expected.equals(Optional.ofNullable(existing))) {
        return CompletableFuture.completedFuture(false);
      }
      rows.put(key, replacement);
      return CompletableFuture.completedFuture(true);
    }

    private record RecordKey(SystemRecordType type, ByteKey identity) { }
  }
}
