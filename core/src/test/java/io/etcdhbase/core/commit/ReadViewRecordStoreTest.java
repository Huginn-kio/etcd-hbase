package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BucketCursor;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ExistsExecutor;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReadViewRecordStoreTest {
  @Test
  void currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge() throws Exception {
    Fixture fixture = currentReadFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      assertSelectedPointReads(view);
      assertCurrentScanPage(view, pinned);
      assertRangeMergeAndExists(view, scope);
    }
    assertEquals(1, fixture.pinCloses.get());
  }

  private static Fixture currentReadFixture() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("beta", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("doomed", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("gamma", 2, 2, 1, StorageMetadata.State.LIVE, bytes("new-gamma"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old-alpha"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "old-beta"));
    fixture.physical.add(live("doomed", 1, 1, 1, 0, "old-doomed"));
    fixture.unselectedCandidate(candidate("leak", 2, 2, 1,
        StorageMetadata.State.LIVE, bytes("private-only")));
    fixture.unselectedCandidate(candidate("gamma", 2, 2, 1,
        StorageMetadata.State.LIVE, bytes("failed-attempt-value")));
    return fixture;
  }

  private static void assertSelectedPointReads(ReadViewRecordStore view) {
    StoredRecord selected = view.get(key("gamma"), 2, ReadProjection.FULL).join().orElseThrow();
    assertEquals(2, selected.metadata().modificationRevision());
    assertEquals(1, selected.metadata().version());
    assertEquals(2, selected.metadata().createRevision());
    assertEquals("new-gamma", new String(selected.value().orElseThrow()));
    assertTrue(view.get(key("leak"), 2, ReadProjection.FULL).join().isEmpty());
  }

  private static void assertCurrentScanPage(ReadViewRecordStore view, PinnedReadView pinned) {
    StoragePage page = view.startScan(new ScanRequest(0, KeyRange.all(), Optional.empty(),
        2, ReadProjection.FULL, 16, 8192, 64)).completion().toCompletableFuture().join();
    assertEquals(List.of("alpha", "beta", "doomed", "gamma"),
        page.records().stream().map(record -> text(record.key())).toList());
    assertEquals(StorageMetadata.State.TOMBSTONE, page.records().get(1).metadata().state());
    assertEquals(StorageMetadata.State.TOMBSTONE, page.records().get(2).metadata().state());
    assertEquals("new-gamma", new String(page.records().getLast().value().orElseThrow()));
    assertTrue(page.exhausted());
    assertEquals(1, page.cost().observations().get(StoragePage.Observation.MANIFEST_LEAF_PAGES));
    assertEquals(3, page.cost().observations().get(StoragePage.Observation.MANIFEST_CANDIDATE_RECORDS));
    assertEquals(2, pinned.view().targetRevision());
    assertEquals(1, pinned.view().stableRevision());
  }

  private static void assertRangeMergeAndExists(ReadViewRecordStore view, RequestScope scope)
      throws BackendFailure {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 4096, 8192, 8192);
    var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
        ReadProjection.FULL, 1, 1024, 64);
    try (var account = budgets.admit("public", scope, reservation);
        var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
        var cursor = SortedMergeCursor.values(view, scheduler.bind(view, account), request,
            1, account, scope);
        var response = cursor.read(0).join()) {
      assertEquals(List.of("alpha", "gamma"), response.records().stream()
          .map(record -> text(record.key())).toList());
      assertEquals("new-gamma", new String(response.records().getLast().value().orElseThrow()));
      assertEquals(2, response.pointReads(), "selected heads fetch values through the same pinned view");
      assertEquals(1, ExistsExecutor.point(view, key("gamma"), 2, account).join());
      assertEquals(0, ExistsExecutor.point(view, key("doomed"), 2, account).join());
      try (var exists = new ExistsExecutor(scheduler.bind(view, account),
          new ScanRequest(0, KeyRange.point(key("gamma")), Optional.empty(), 2,
              ReadProjection.METADATA, 1, 1024, 64), 1, scope)) {
        assertEquals(1, exists.execute().join().count());
      }
    }
  }

  @Test
  void historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions() throws Exception {
    Fixture fixture = new Fixture(2, List.of(
        candidate("beta", 3, 3, 1, StorageMetadata.State.LIVE, bytes("recreated"))));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "created"));
    fixture.physical.add(tombstone("beta", 2, 2));

    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView current = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore currentStore = fixture.readStore(current, scope);
      StoredRecord recreated = currentStore.get(key("beta"), 3, ReadProjection.FULL).join().orElseThrow();
      assertEquals(3, recreated.metadata().createRevision());
      assertEquals(1, recreated.metadata().version());
      assertEquals("recreated", new String(recreated.value().orElseThrow()));

      PinnedReadView historical = fixture.views.open(OptionalLong.of(2), digest(9), scope).join();
      ReadViewRecordStore historicalStore = fixture.readStore(historical, scope);
      StoredRecord deleted = historicalStore.get(key("beta"), 2, ReadProjection.METADATA).join().orElseThrow();
      assertEquals(StorageMetadata.State.TOMBSTONE, deleted.metadata().state());
      assertFalse(deleted.hasValue());
      assertTrue(historical.view().selectedRoot().isEmpty());
      assertEquals(2, historical.view().stableRevision());
    }
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void selectedRootIsPinnedAndRecheckedBeforeAnyDataRead() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      assertEquals(2, fixture.control.reads.get(), "control is read before and after pin registration");
      assertEquals(2, fixture.repository.rootReads.get(), "the selected root is rechecked after pinning");
      assertEquals(1, fixture.pinRegistrations.get());
      assertEquals(17, pinned.view().pinGeneration());
    }
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void cancellingAReadKeepsItsPinUntilThePhysicalProducerStops() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
    fixture.physical.blockNextGet(key("zeta"));
    ReadViewRecordStore view = fixture.readStore(pinned, scope);
    var pending = view.get(key("zeta"), 2, ReadProjection.FULL);

    scope.close();
    assertEquals(0, fixture.pinCloses.get(), "an unfinished backend read still protects its view");
    fixture.physical.pendingGet.complete(Optional.empty());

    assertTrue(pending.isCompletedExceptionally());
    scope.cleanupCompletion().toCompletableFuture().join();
    assertEquals(1, fixture.pinCloses.get(), "the view pin releases after producer completion");
  }

  @Test
  void byteTruncatedPhysicalPageDoesNotSkipKeysDuringOverlayMerge() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("z", 2, 2, 1, StorageMetadata.State.LIVE, new byte[100])));
    fixture.physical.add(liveBytes("a", 1, 1, 100));
    fixture.physical.add(liveBytes("b", 1, 1, 700));

    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 1024, 3));
    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 4096, 1));
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void byteTruncatedCandidatePageDoesNotSkipKeysDuringOverlayMerge() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[400]),
        candidate("b", 2, 2, 1, StorageMetadata.State.LIVE, new byte[700])));
    fixture.physical.add(liveBytes("z", 1, 1, 100));

    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 1024, 3));
    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 4096, 1));
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void cancellingReadResultDoesNotCancelPinAccounting() throws Exception {
    Fixture beforeProducerCompletion = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope firstScope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView firstPinned = beforeProducerCompletion.views
        .open(OptionalLong.empty(), digest(9), firstScope).join();
    beforeProducerCompletion.physical.blockNextGet(key("zeta"));
    var firstResult = beforeProducerCompletion.readStore(firstPinned, firstScope)
        .get(key("zeta"), 2, ReadProjection.FULL);

    assertTrue(firstResult.cancel(false));
    firstScope.close();
    assertEquals(0, beforeProducerCompletion.pinCloses.get());
    beforeProducerCompletion.physical.pendingGet.complete(Optional.empty());
    firstScope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, beforeProducerCompletion.pinCloses.get(),
        "scope cleanup waits for the physical producer even when the caller cancels its result");

    Fixture afterProducerCompletion = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope secondScope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView secondPinned = afterProducerCompletion.views
        .open(OptionalLong.empty(), digest(9), secondScope).join();
    afterProducerCompletion.physical.blockNextGet(key("zeta"));
    var secondResult = afterProducerCompletion.readStore(secondPinned, secondScope)
        .get(key("zeta"), 2, ReadProjection.FULL);

    assertTrue(secondResult.cancel(false));
    afterProducerCompletion.physical.pendingGet.complete(Optional.empty());
    assertEquals(0, afterProducerCompletion.pinCloses.get());
    secondScope.close();
    secondScope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, afterProducerCompletion.pinCloses.get(),
        "scope cleanup releases the pin when the producer already completed");
  }

  private static List<String> scanKeys(Fixture fixture, long maximumBytes, int expectedPages) {
    List<String> keys = new ArrayList<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
          ReadProjection.FULL, 8, maximumBytes, 8);
      try (BucketCursor cursor = new BucketCursor(view, request, scope)) {
        int pages = 0;
        boolean exhausted = false;
        while (!exhausted && pages < 10) {
          try (BucketCursor.Page page = cursor.fetch().join()) {
            pages++;
            Optional<StoredRecord> next;
            while ((next = page.next()).isPresent()) keys.add(text(next.orElseThrow().key()));
            exhausted = page.exhausted();
          }
        }
        assertTrue(exhausted, "read view scan must finish without losing progress");
        assertEquals(expectedPages, pages);
      }
    }
    return List.copyOf(keys);
  }

  private static PreparedCandidate candidate(String key, long createRevision,
      long modificationRevision, long version, StorageMetadata.State state, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(createRevision, modificationRevision, version,
        0, state, value.length);
    StoredRecord record = state == StorageMetadata.State.LIVE
        ? StoredRecord.live(key(key), metadata, value) : StoredRecord.metadataOnly(key(key), metadata);
    return new PreparedCandidate(TransactionId.create(), 1, 0, modificationRevision - 1, record);
  }

  private static StoredRecord live(String key, long createRevision, long modificationRevision,
      long version, long lease, String value) {
    byte[] bytes = bytes(value);
    return StoredRecord.live(key(key), new StorageMetadata(createRevision, modificationRevision,
        version, lease, StorageMetadata.State.LIVE, bytes.length), bytes);
  }

  private static StoredRecord liveBytes(String key, long createRevision,
      long modificationRevision, int valueLength) {
    byte[] value = new byte[valueLength];
    return StoredRecord.live(key(key), new StorageMetadata(createRevision, modificationRevision,
        1, 0, StorageMetadata.State.LIVE, value.length), value);
  }

  private static StoredRecord tombstone(String key, long createRevision, long modificationRevision) {
    return StoredRecord.metadataOnly(key(key), new StorageMetadata(createRevision,
        modificationRevision, 2, 0, StorageMetadata.State.TOMBSTONE, 0));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
  private static byte[] bytes(String value) { return value.getBytes(java.nio.charset.StandardCharsets.UTF_8); }
  private static String text(ByteKey key) { return new String(key.bytes(), java.nio.charset.StandardCharsets.UTF_8); }

  private static final class Fixture {
    private final ByteKey profile = digest(3);
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final List<PreparedCandidate> selectedCandidates;
    private final ManifestRoot root;
    private final MemoryManifestRepository repository;
    private final MemoryControl control;
    private final AtomicInteger pinRegistrations = new AtomicInteger();
    private final AtomicInteger pinCloses = new AtomicInteger();
    private final ReadViewService views;
    private final MemoryRecordStore physical = new MemoryRecordStore();

    private Fixture(long baseRevision, List<PreparedCandidate> candidates) {
      selectedCandidates = candidates.stream().map(candidate -> new PreparedCandidate(transactionId,
          1, baseRevision - 1, baseRevision, candidate.record())).toList();
      var references = selectedCandidates.stream()
          .map(candidate -> ManifestCandidateReference.from(candidate, 0)).toList();
      ManifestPage page = ManifestPage.create(transactionId, 0, references);
      long payloadBytes = selectedCandidates.stream()
          .mapToLong(candidate -> candidate.record().metadata().valueLength()).sum();
      root = ManifestRoot.create(transactionId, 1, baseRevision - 1, baseRevision,
          selectedCandidates.size(), payloadBytes, Optional.of(page.reference()));
      repository = new MemoryManifestRepository(root, page, selectedCandidates);
      long baseSequence = baseRevision - 1;
      long revision = baseRevision + 1;
      PublicationSlot slot = new PublicationSlot(transactionId, invocationId, 1, baseSequence,
          baseRevision, baseSequence + 1, revision, true, root.systemIdentity(), root.digest());
      CommitState state = CommitState.restore(profile, Optional.of("fixture-owner"), 1, 10_000,
          2, slot.sequence(), slot.revision(), baseSequence, baseRevision, Optional.of(slot), 0);
      control = new MemoryControl(state);
      ManifestCatalog catalog = new ManifestCatalog(repository);
      ReadViewPinRegistry pinRegistry = (view, scope) -> {
        pinRegistrations.incrementAndGet();
        return CompletableFuture.completedFuture(new ReadViewPinRegistry.Pin() {
          @Override public long generation() { return 17; }
          @Override public void close() { pinCloses.incrementAndGet(); }
        });
      };
      views = new ReadViewService(control, catalog, pinRegistry, profile);
    }

    private ReadViewRecordStore readStore(PinnedReadView pinned, RequestScope scope) {
      return new ReadViewRecordStore(pinned, physical, new ManifestCatalog(repository), ignored -> 0,
          1, scope);
    }

    private void unselectedCandidate(PreparedCandidate candidate) {
      repository.candidates.put(candidate.systemIdentity(), candidate);
    }
  }

  private static final class MemoryControl implements CommitControlStore {
    private final CommitState state;
    private final AtomicInteger reads = new AtomicInteger();

    private MemoryControl(CommitState state) { this.state = state; }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      reads.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }
  }

  private static final class MemoryManifestRepository implements ManifestRepository {
    private final ManifestRoot root;
    private final ManifestPage page;
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
    private final AtomicInteger rootReads = new AtomicInteger();

    private MemoryManifestRepository(ManifestRoot root, ManifestPage page,
        List<PreparedCandidate> candidates) {
      this.root = root;
      this.page = page;
      candidates.forEach(candidate -> this.candidates.put(candidate.systemIdentity(), candidate));
    }
    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
      rootReads.incrementAndGet();
      return CompletableFuture.completedFuture(identity.equals(root.systemIdentity())
          ? Optional.of(root) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(candidates.get(identity)));
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(identity.equals(page.systemIdentity())
          ? Optional.of(page) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }

  private static final class MemoryRecordStore implements RecordStore {
    private final Map<ByteKey, List<StoredRecord>> versions = new HashMap<>();
    private ByteKey blockedKey;
    private CompletableFuture<Optional<StoredRecord>> pendingGet;
    private void blockNextGet(ByteKey key) {
      blockedKey = key;
      pendingGet = new CompletableFuture<>();
    }
    private void add(StoredRecord record) {
      versions.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record);
      versions.get(record.key()).sort(Comparator.comparingLong(
          (StoredRecord value) -> value.metadata().modificationRevision()).reversed());
    }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection) {
      if (key.equals(blockedKey)) return pendingGet;
      return CompletableFuture.completedFuture(versions.getOrDefault(key, List.of()).stream()
          .filter(record -> record.metadata().modificationRevision() <= upper)
          .findFirst().map(record -> projection == ReadProjection.METADATA && record.hasValue()
              ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record));
    }
    @Override public ScanOperation startScan(ScanRequest request) {
      Map<ByteKey, StoredRecord> latest = new HashMap<>();
      for (List<StoredRecord> history : versions.values()) {
        for (StoredRecord record : history) {
          if (request.range().contains(record.key())
              && request.after().map(after -> record.key().compareTo(after) > 0).orElse(true)
              && record.metadata().modificationRevision() <= request.revisionUpperBound()) {
            latest.merge(record.key(), record,
                (left, right) -> left.metadata().modificationRevision()
                    > right.metadata().modificationRevision() ? left : right);
          }
        }
      }
      List<StoredRecord> matching = latest.values().stream()
          .sorted(Comparator.comparing(StoredRecord::key)).toList();
      List<StoredRecord> projected = new ArrayList<>();
      long bytes = 0;
      boolean exhausted = true;
      for (StoredRecord record : matching) {
        StoredRecord value = request.projection() == ReadProjection.METADATA && record.hasValue()
            ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record;
        if (projected.size() == request.maximumRows()
            || value.retainedBytes() > request.maximumBytes() - bytes) {
          exhausted = false;
          break;
        }
        projected.add(value);
        bytes += value.retainedBytes();
      }
      StoragePage page = new StoragePage(projected, exhausted,
          new StoragePage.Cost(projected.size(), bytes, 1));
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() {
          return CompletableFuture.completedFuture(page);
        }
        @Override public void close() {}
      };
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
  }
}
