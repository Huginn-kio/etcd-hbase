package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventIndexMaterializerTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {1});

  @Test
  void initialCheckpointMustCaptureTheCurrentFinalizedBarrier() {
    Fixture fixture = new Fixture();
    var sequence = new java.util.concurrent.atomic.AtomicLong(4);
    var materializer = backfill(fixture, new MemoryIndexes(), sequence, true);
    var stale = new EventIndexBackfillCheckpoint(1, 5, 3, 3, 3, 10, 10, 11, 0);
    var current = new EventIndexBackfillCheckpoint(1, 5, 5, 4, 5, 11, 11, 11, 0);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertThrows(CompletionException.class,
          () -> materializer.preflight(stale, OptionalLong.empty(), scope).join());
      materializer.preflight(current, OptionalLong.empty(), scope).join();
    }
  }

  @Test
  void backfillVerifiesLegacyDecisionBeforeSelectionAndRecoversIndexAfterSelection() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    var sequence = new java.util.concurrent.atomic.AtomicLong(4);
    var materializer = backfill(fixture, indexes, sequence, true);
    var checkpoint = new EventIndexBackfillCheckpoint(1, 5, 3, 3, 4, 10, 11, 11, 0);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertThrows(CompletionException.class, () -> materializer.apply(checkpoint, scope).join());
      sequence.set(3);
      assertThrows(CompletionException.class,
          () -> materializer.preflight(checkpoint, OptionalLong.of(10), scope).join());
      sequence.set(4);
      materializer.preflight(checkpoint, OptionalLong.of(10), scope).join();
      assertEquals(0, indexes.writes);
      assertThrows(CompletionException.class,
          () -> materializer.preflight(checkpoint, OptionalLong.of(9), scope).join());
      indexes.unknownOnce = true;
      sequence.set(5);
      materializer.apply(checkpoint, scope).join();
      materializer.apply(checkpoint, scope).join();
      assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), indexes.stored);
      assertEquals(2, indexes.writes);
      assertThrows(CompletionException.class, () -> backfill(fixture, indexes, sequence, false)
          .apply(checkpoint, scope).join());
    }
  }

  @Test
  void failedIndexWriteCannotMaterializeCheckpointAndSelectedRecoveryRetries() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    var sequence = new java.util.concurrent.atomic.AtomicLong(4);
    var backfill = backfill(fixture, indexes, sequence, true);
    var previous = new EventIndexBackfillCheckpoint(1, 4, 3, 3, 3, 10, 10, 11, 0);
    var checkpoint = new EventIndexBackfillCheckpoint(1, 5, 3, 3, 4, 10, 11, 11, 0);
    TransactionId transaction = TransactionId.create();
    var record = PreparedAuxiliaryRecord.create(transaction, 1, 4, 0,
        SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
        EventIndexBackfillCheckpoint.versionIdentity(5), checkpoint.encode());
    var selected = new CommitDecision(transaction, InvocationId.create(), 5, 11, 11,
        KEY, DIGEST, false);
    var records = new CheckpointRecords(previous);
    var store = new SystemRecordAuxiliaryMaterializationStore(records, ignored -> null, null, backfill);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      store.preflight(record, scope).join();
      sequence.set(5);
      indexes.failOnce = true;
      assertThrows(CompletionException.class, () -> store.applySelected(selected, record, scope).join());
      assertTrue(records.created == null);
      store.applySelected(selected, record, scope).join();
      assertEquals(checkpoint.encode(), records.created);
      assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), indexes.stored);
    }
  }

  private static final class CheckpointRecords implements SystemRecordStore {
    private final EventIndexBackfillCheckpoint previous;
    private ByteKey created;
    private CheckpointRecords(EventIndexBackfillCheckpoint previous) { this.previous = previous; }
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<Optional<ByteKey>> read(SystemRecordType type,
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(created)); }
    @Override public CompletableFuture<Optional<SystemRecordEntry>> latestAtOrBefore(SystemRecordType type,
        ByteKey route, long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new SystemRecordEntry(
          EventIndexBackfillCheckpoint.versionIdentity(previous.sequence()), previous.encode())));
    }
    @Override public CompletableFuture<CreateResult> createIfAbsent(SystemRecordType type,
        ByteKey identity, ByteKey value, RequestScope scope) {
      created = value;
      return CompletableFuture.completedFuture(CreateResult.CREATED);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey value, RequestScope scope) { throw new UnsupportedOperationException(); }
  }

  private static EventIndexBackfillMaterializer backfill(Fixture fixture, MemoryIndexes indexes,
      java.util.concurrent.atomic.AtomicLong sequence, boolean decisionAvailable) {
    CommitControlStore control = new CommitControlStore() {
      @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
        long current = sequence.get();
        var state = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, DIGEST,
            Optional.of("owner"), 1, 100, 1, current, 11, current, 11, Optional.empty(), 0);
        return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
      }
      @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
        throw new UnsupportedOperationException();
      }
      @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot previous,
          CommitState replacement, RequestScope scope) { throw new UnsupportedOperationException(); }
    };
    CommitDecisionRepository decisions = new CommitDecisionRepository() {
      @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
        return CompletableFuture.completedFuture(decisionAvailable && sequence == fixture.decision.sequence()
            ? Optional.of(fixture.decision) : Optional.empty());
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          CommitDecision decision, RequestScope scope) { throw new UnsupportedOperationException(); }
    };
    var manifests = (ManifestRepository) java.lang.reflect.Proxy.newProxyInstance(
        ManifestRepository.class.getClassLoader(), new Class<?>[] {ManifestRepository.class},
        (proxy, method, arguments) -> {
          if (method.getName().equals("readRoot")) {
            return CompletableFuture.completedFuture(arguments[0].equals(fixture.root.systemIdentity())
                ? Optional.of(fixture.root) : Optional.empty());
          }
          throw new UnsupportedOperationException(method.getName());
        });
    return new EventIndexBackfillMaterializer(control, decisions, new ManifestCatalog(manifests), indexes);
  }

  @Test
  void persistsSelectedIndexAndReplaysIdentically() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      EventIndexMaterializer materializer = new EventIndexMaterializer(indexes);
      materializer.apply(fixture.decision, fixture.root, scope).join();
      materializer.apply(fixture.decision, fixture.root, scope).join();
      assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), indexes.stored);
      assertEquals(2, indexes.writes);
    }
  }

  @Test
  void resolvesUnknownWriteFromDurableValueAndRejectsConflicts() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    indexes.unknownOnce = true;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      EventIndexMaterializer materializer = new EventIndexMaterializer(indexes);
      materializer.apply(fixture.decision, fixture.root, scope).join();
      assertEquals(1, indexes.writes);
      Fixture conflicting = new Fixture();
      indexes.stored = EventRevisionIndex.fromSelected(conflicting.decision, conflicting.root);
      assertThrows(CompletionException.class,
          () -> new EventIndexMaterializer(indexes).apply(fixture.decision, fixture.root, scope).join());
    }
  }

  @Test
  void metadataOnlyDoesNotCreateAnIndex() {
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 0, 0, Optional.empty());
    CommitDecision decision = new CommitDecision(transaction, InvocationId.create(), 3, 10, 10,
        root.systemIdentity(), root.digest(), false);
    MemoryIndexes indexes = new MemoryIndexes();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      new EventIndexMaterializer(indexes).apply(decision, root, scope).join();
      assertEquals(0, indexes.writes);
      assertTrue(indexes.stored == null);
    }
  }

  private static final class Fixture {
    private final TransactionId transaction = TransactionId.create();
    private final ManifestChildReference candidates =
        new ManifestChildReference(KEY, DIGEST, 0, 0, KEY, KEY, 1, 0, 1, 0);
    private final EventReference events = new EventReference(KEY, DIGEST, 0, 0, 1, 1, 1, 0);
    private final ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 1, 0,
        Optional.of(candidates), 1, 1, Optional.of(events));
    private final CommitDecision decision = new CommitDecision(transaction, InvocationId.create(),
        3, 10, 11, root.systemIdentity(), root.digest(), true);
  }

  private static final class MemoryIndexes implements EventRevisionIndexRepository {
    private EventRevisionIndex stored;
    private int writes;
    private boolean unknownOnce;
    private boolean failOnce;

    @Override public CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(stored));
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        EventRevisionIndex index, RequestScope scope) {
      writes++;
      if (failOnce) {
        failOnce = false;
        return CompletableFuture.failedFuture(new IllegalStateException("index write failed"));
      }
      if (stored != null) return CompletableFuture.completedFuture(stored.equals(index)
          ? SystemRecordStore.CreateResult.ALREADY_PRESENT : SystemRecordStore.CreateResult.CONFLICT);
      stored = index;
      if (unknownOnce) {
        unknownOnce = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.TIMEOUT));
      }
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }
}
