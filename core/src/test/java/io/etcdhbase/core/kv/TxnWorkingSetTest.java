package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.EventDirectoryPage;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestDirectoryPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class TxnWorkingSetTest {
  @Test
  void orderedWritesShareOneRevisionAndOnlyTheWorkingViewSeesThem() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      StoredRecord base = live("a", "old");
      assertEquals(base, working.current(key("a"), Optional.of(base)).join().orElseThrow());
      PutResult first = working.put(command("a", "new", true), Optional.of(base)).join();
      working.put(command("b", "second", false), Optional.empty()).join();
      assertArrayEquals(bytes("old"), first.previous().orElseThrow().value().orElseThrow());
      assertArrayEquals(bytes("new"), working.current(key("a"), Optional.of(base)).join()
          .orElseThrow().value().orElseThrow());
      assertArrayEquals(bytes("old"), base.value().orElseThrow());
      working.freeze();
      var candidates = working.candidates();
      assertEquals(List.of(key("b"), key("a")), List.of(candidates.next().join().orElseThrow().record().key(),
          candidates.next().join().orElseThrow().record().key()));
      assertTrue(candidates.next().join().isEmpty());
      var events = working.events();
      assertEquals(List.of(0L, 1L), List.of(events.next().join().orElseThrow().ordinal(),
          events.next().join().orElseThrow().ordinal()));
      assertTrue(events.next().join().isEmpty());
      assertEquals(2, working.candidateCount());
      assertEquals(2, working.eventCount());
      assertTrue(working.hasChanges());
    }
  }

  @Test
  void persistedPayloadCanExceedTheWorkingMemoryBound() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      byte[] payload = new byte[1024];
      for (int i = 0; i < 100; i++) {
        working.put(new PutCommand(key(String.format("key-%03d", i)), payload, 0, false, false, false),
            Optional.empty()).join();
      }
      assertTrue((long) payload.length * working.eventCount() > fixture.workingBytes);
      assertEquals(100, fixture.manifests.events.size());
      assertTrue(fixture.records.rows.size() > 100, "index nodes and checkpoints must be persisted");
      assertEquals(fixture.workingBytes, fixture.manager.snapshot().allocatedBytes());
      working.freeze();
      var cursor = working.candidates();
      int count = 0;
      while (cursor.next().join().isPresent()) count++;
      assertEquals(100, count);
      assertEquals(fixture.workingBytes, fixture.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void mergesCompletedOverlayAndFixedBaseInBucketKeyOrder() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      working.put(command("b", "new-b", false), Optional.of(live("b", "old-b"))).join();
      working.put(command("c", "new-c", false), Optional.empty()).join();
      working.put(command("a", "new-a", false), Optional.of(live("a", "old-a"))).join();
      var base = List.of(live("b", "old-b"), live("d", "old-d"));
      var merged = working.merge(0, KeyRange.between(key("b"), key("e")),
          AsyncCursor.fromIterator(base.iterator()));
      var actual = new java.util.ArrayList<String>();
      for (var value = merged.next().join(); value.isPresent(); value = merged.next().join()) {
        actual.add(new String(value.orElseThrow().value().orElseThrow(), StandardCharsets.UTF_8));
      }
      assertEquals(List.of("new-b", "new-c", "old-d"), actual);
      var otherBucket = working.merge(1, KeyRange.all(),
          AsyncCursor.fromIterator(List.of(live("a", "old-a")).iterator()));
      assertArrayEquals(bytes("new-a"), otherBucket.next().join().orElseThrow().value().orElseThrow());
      assertTrue(otherBucket.next().join().isEmpty());
    }
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      var outOfOrder = working.merge(0, KeyRange.all(),
          AsyncCursor.fromIterator(List.of(live("d", "one"), live("b", "two")).iterator()));
      assertTrue(outOfOrder.next().join().isPresent());
      assertThrows(CompletionException.class, () -> outOfOrder.next().join());
      assertThrows(IllegalStateException.class, working::events);
    }
  }

  @Test
  void failedPartialOperationFencesTheAttempt() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      working.put(command("a", "one", false), Optional.empty()).join();
      fixture.records.failNextCreate = true;
      assertThrows(CompletionException.class, () -> working.put(command("b", "two", false), Optional.empty()).join());
      assertEquals(1, working.candidateCount());
      assertEquals(1, working.eventCount());
      assertEquals(2, fixture.manifests.events.size(), "an uncheckpointed event may remain private");
      assertThrows(CompletionException.class, () -> working.current(key("b"), Optional.empty()).join());
      assertThrows(CompletionException.class, () -> working.put(command("c", "three", false), Optional.empty()).join());
    }
  }

  @Test
  void frozenCheckpointCannotRaceWithAnUnfinishedOrLaterWrite() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      fixture.manifests.blockedEvent = new CompletableFuture<>();
      var pending = working.put(command("a", "one", false), Optional.empty());
      assertFalse(pending.isDone());
      assertThrows(IllegalStateException.class, working::freeze);
      fixture.manifests.blockedEvent.complete(null);
      pending.join();
      working.freeze();
      assertThrows(CompletionException.class, () -> working.put(command("b", "two", false), Optional.empty()).join());
      var candidates = working.candidates();
      assertEquals(key("a"), candidates.next().join().orElseThrow().record().key());
      assertTrue(candidates.next().join().isEmpty());
    }
  }

  @Test
  void changedEventIsRejectedBeforeTheCandidateOrEventStreamCanComplete() throws Exception {
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      working.put(command("a", "one", false), Optional.empty()).join();
      PreparedEvent original = fixture.manifests.events.values().iterator().next();
      StoredRecord changed = StoredRecord.live(original.current().key(), original.current().metadata(), bytes("two"));
      fixture.manifests.events.put(original.systemIdentity(), new PreparedEvent(original.transactionId(),
          original.ownerEpoch(), original.baseSequence(), original.baseRevision(), original.ordinal(),
          original.kind(), changed, original.previous()));
      working.freeze();
      var events = working.events();
      assertTrue(events.next().join().isPresent());
      assertThrows(CompletionException.class, () -> events.next().join());
      assertThrows(IllegalStateException.class, working::candidates);
    }
    try (var fixture = new Fixture()) {
      var working = fixture.working();
      working.put(command("a", "one", false), Optional.empty()).join();
      PreparedEvent original = fixture.manifests.events.values().iterator().next();
      StoredRecord changed = StoredRecord.live(original.current().key(), original.current().metadata(), bytes("two"));
      fixture.manifests.events.put(original.systemIdentity(), new PreparedEvent(original.transactionId(),
          original.ownerEpoch(), original.baseSequence(), original.baseRevision(), original.ordinal(),
          original.kind(), changed, original.previous()));
      working.freeze();
      assertThrows(CompletionException.class, () -> working.candidates().next().join());
      assertThrows(IllegalStateException.class, working::events);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final long workingBytes = 64 * 1024;
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(20));
    private final BudgetManager.Limits limits = new BudgetManager.Limits(2, 0, 1, 1, 1, 0, 1L << 22);
    private final BudgetManager manager = new BudgetManager(limits, Map.of("public", limits), limits);
    private final BudgetManager.Account account;
    private final BudgetManager.Lease workingLease;
    private final MemoryRecords records = new MemoryRecords();
    private final MemoryManifests manifests = new MemoryManifests();

    private Fixture() throws Exception {
      account = manager.admit("public", scope, new BudgetManager.Reservation(0, 1, 1, 0, workingBytes));
      workingLease = account.tryRetain(BudgetManager.Memory.CANDIDATE, workingBytes).orElseThrow();
    }
    private TxnWorkingSet working() {
      var working = new TxnWorkingSet(invocation(), workingBytes, records, manifests, scope, account,
          key -> key.equals(key("a")) ? 1 : 0);
      working.begin().join();
      return working;
    }
    @Override public void close() {
      workingLease.close();
      account.close();
      scope.close();
    }
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private boolean failNextCreate;
    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(identity)));
    }
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      if (failNextCreate) {
        failNextCreate = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      ByteKey old = rows.putIfAbsent(identity, value);
      return CompletableFuture.completedFuture(old == null ? CreateResult.CREATED
          : old.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) { return unsupported(); }
  }

  private static final class MemoryManifests implements ManifestRepository {
    private final Map<ByteKey, PreparedEvent> events = new HashMap<>();
    private CompletableFuture<Void> blockedEvent;
    @Override public int candidateBucket(PreparedCandidate candidate) {
      return candidate.record().key().equals(key("a")) ? 1 : 0;
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent event, RequestScope scope) {
      var ready = blockedEvent == null ? CompletableFuture.<Void>completedFuture(null) : blockedEvent;
      return ready.thenApply(ignored -> {
        PreparedEvent old = events.putIfAbsent(event.systemIdentity(), event);
        return old == null ? SystemRecordStore.CreateResult.CREATED
            : old.digest().equals(event.digest()) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
                : SystemRecordStore.CreateResult.CONFLICT;
      });
    }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(events.get(id)));
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(PreparedCandidate value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(ManifestDirectoryPage value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(EventDirectoryPage value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot value, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey id, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey id, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey id, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey id, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey id, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(ByteKey id, RequestScope scope) { return unsupported(); }
  }

  private static <T> CompletableFuture<T> unsupported() {
    return CompletableFuture.failedFuture(new UnsupportedOperationException());
  }
  private static InvocationBinding invocation() {
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    return InvocationBinding.begin(InvocationId.create(), digest, digest, TransactionId.create(), 1, 3, 5);
  }
  private static PutCommand command(String key, String value, boolean previous) {
    return new PutCommand(key(key), bytes(value), 0, previous, false, false);
  }
  private static StoredRecord live(String key, String value) {
    byte[] encoded = bytes(value);
    return StoredRecord.live(key(key), new StorageMetadata(1, 5, 2, 0, StorageMetadata.State.LIVE, encoded.length), encoded);
  }
  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
