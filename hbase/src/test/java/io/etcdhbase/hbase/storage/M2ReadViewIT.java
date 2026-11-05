package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventDirectoryPage;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCandidateReference;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestDirectoryPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewRecordStore;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;

/** Public MiniCluster evidence for the J25 pinned view over both physical history layouts. */
class M2ReadViewIT {
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));
  private static final byte[] FAMILY = {'d'};
  private static final ByteKey PROFILE = digest(3);
  private static final ByteKey AUTHORIZATION = digest(9);
  private static final AtomicLong PIN_GENERATIONS = new AtomicLong();

  @Test
  void readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts() throws Exception {
    for (StorageTableBinding.Layout layout :
        List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      verifyLayout(layout);
    }
  }

  private static void verifyLayout(StorageTableBinding.Layout layout) throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      ReadFixture fixture = prepareLayout(layout, environment, scope);
      try (BudgetManager.Account account = budget(scope)) {
        assertCurrentPointReads(fixture, account);
        assertCurrentRange(fixture, account);
      }
      assertHistoricalReads(fixture);
      System.out.printf("j25-readview layout=%s selectedPending=true historical=true"
          + " deleteVisible=true unselectedIsolated=true%n", layout);
    }
  }

  private static ReadFixture prepareLayout(StorageTableBinding.Layout layout,
      M1TestEnvironment environment, RequestScope scope) throws Exception {
    StorageTableBinding binding = table(layout);
    environment.create(binding, SALT.splitKeys().toArray(byte[][]::new));
    HBaseRecordStore physical = environment.store(binding, scope);
    ReadKeys keys = readKeys();
    seedPhysicalHistory(physical, keys);

    TransactionId transactionId = TransactionId.create();
    List<PreparedCandidate> candidates = selectedCandidates(transactionId, keys);
    MemoryManifestRepository repository = new MemoryManifestRepository();
    ManifestRoot root = repository.add(transactionId, 1, 0, 2, candidates,
        key -> binding.codec().bucket(key));
    repository.addPrivateCandidate(candidate(TransactionId.create(),
        live(keys.unselected(), 3, 3, 1, "unselected-value")));
    repository.addPrivateCandidate(candidate(TransactionId.create(),
        live(keys.inserted(), 3, 3, 1, "failed-attempt-value")));

    PublicationSlot slot = new PublicationSlot(transactionId, InvocationId.create(), 1,
        0, 2, 1, 3, true, root.systemIdentity(), root.digest());
    CommitState state = CommitState.initial(PROFILE, 2)
        .claimOwner("j25-minicluster", 1, 10_000)
        .publish(slot);
    MemoryControl control = new MemoryControl(state);
    ManifestCatalog catalog = new ManifestCatalog(repository);
    ReadViewService views = new ReadViewService(control, catalog, pinRegistry(), PROFILE);
    ReadViewRecordStore current = open(views, binding, physical, catalog, scope,
        OptionalLong.empty());
    return new ReadFixture(keys, binding, physical, catalog, views, scope, current);
  }

  private static ReadKeys readKeys() {
    return new ReadKeys(keyInBucket("updated", 0), keyInBucket("deleted", 0),
        keyInBucket("inserted", 0), keyInBucket("recreated", 0),
        keyInBucket("unselected", 0));
  }

  private static void seedPhysicalHistory(HBaseRecordStore physical, ReadKeys keys)
      throws Exception {
    await(physical.put(live(keys.updated(), 1, 1, 1, "old-value")));
    await(physical.put(live(keys.updated(), 1, 2, 2, "stable-value")));
    await(physical.put(live(keys.deleted(), 1, 1, 1, "old-before-delete")));
    await(physical.put(live(keys.recreated(), 1, 1, 1, "before-delete")));
    await(physical.put(tombstone(keys.recreated(), 1, 2)));
  }

  private static List<PreparedCandidate> selectedCandidates(TransactionId transactionId,
      ReadKeys keys) {
    return List.of(
        candidate(transactionId, live(keys.updated(), 1, 3, 3, "selected-value")),
        candidate(transactionId, tombstone(keys.deleted(), 1, 3)),
        candidate(transactionId, live(keys.inserted(), 3, 3, 1, "selected-insert")),
        candidate(transactionId, live(keys.recreated(), 3, 3, 1, "after-recreate")));
  }

  private static void assertCurrentPointReads(ReadFixture fixture, BudgetManager.Account account) {
    ReadViewRecordStore current = fixture.current();
    ReadKeys keys = fixture.keys();
    StoredRecord visibleUpdate = current.get(keys.updated(), 3, ReadProjection.FULL, account).join().orElseThrow();
    assertEquals(1, visibleUpdate.metadata().createRevision());
    assertEquals(3, visibleUpdate.metadata().modificationRevision());
    assertEquals(3, visibleUpdate.metadata().version());
    assertEquals("selected-value", text(visibleUpdate.value().orElseThrow()));
    StoredRecord visibleDelete = current.get(keys.deleted(), 3, ReadProjection.METADATA, account).join().orElseThrow();
    assertEquals(StorageMetadata.State.TOMBSTONE, visibleDelete.metadata().state());
    assertFalse(visibleDelete.hasValue());
    assertTrue(current.get(keys.unselected(), 3, ReadProjection.FULL, account).join().isEmpty());
    StoredRecord visibleRecreate = current.get(keys.recreated(), 3, ReadProjection.FULL, account)
        .join().orElseThrow();
    assertEquals(3, visibleRecreate.metadata().createRevision());
    assertEquals(1, visibleRecreate.metadata().version());
    assertEquals("after-recreate", text(visibleRecreate.value().orElseThrow()));
    assertEquals("selected-insert", text(current.get(keys.inserted(), 3, ReadProjection.FULL, account)
        .join().orElseThrow().value().orElseThrow()));
  }

  private static void assertCurrentRange(ReadFixture fixture, BudgetManager.Account account) {
    ReadKeys keys = fixture.keys();
    StoragePage page = fixture.current().startScan(new ScanRequest(0,
        KeyRange.all(), Optional.empty(), 3, ReadProjection.FULL, 16, 64 * 1024, 64), account)
        .completion().toCompletableFuture().join();
    assertEquals(List.of(keys.updated(), keys.deleted(), keys.inserted(), keys.recreated()).stream()
        .sorted().toList(), page.records().stream().map(StoredRecord::key).sorted().toList());
    assertEquals("selected-value", text(page.records().stream()
        .filter(record -> record.key().equals(keys.updated())).findFirst().orElseThrow()
        .value().orElseThrow()));
    assertEquals(StorageMetadata.State.TOMBSTONE, page.records().stream()
        .filter(record -> record.key().equals(keys.deleted())).findFirst().orElseThrow()
        .metadata().state());
    assertTrue(page.exhausted());
  }

  private static BudgetManager.Account budget(RequestScope scope) throws Exception {
    var limits = new BudgetManager.Limits(4, 16, 2, 2, 2, 1_000_000, 16L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    return manager.admit("public", scope,
        new BudgetManager.Reservation(128 * 1024, 128 * 1024, 1L << 20, 1L << 20, 1L << 20));
  }

  private static void assertHistoricalReads(ReadFixture fixture) {
    ReadKeys keys = fixture.keys();
    ReadViewRecordStore historical = fixture.open(OptionalLong.of(1));
    StoredRecord priorDeleteValue = historical.get(keys.deleted(), 1, ReadProjection.FULL)
        .join().orElseThrow();
    assertEquals(StorageMetadata.State.LIVE, priorDeleteValue.metadata().state());
    assertArrayEquals(bytes("old-before-delete"), priorDeleteValue.value().orElseThrow());

    ReadViewRecordStore beforeRecreate = fixture.open(OptionalLong.of(2));
    StoredRecord deletedBeforeRecreate = beforeRecreate.get(
        keys.recreated(), 2, ReadProjection.METADATA).join().orElseThrow();
    assertEquals(StorageMetadata.State.TOMBSTONE, deletedBeforeRecreate.metadata().state());
    StoragePage historicalRange = beforeRecreate.startScan(new ScanRequest(0,
        KeyRange.all(), Optional.empty(), 2, ReadProjection.METADATA, 16, 64 * 1024, 64))
        .completion().toCompletableFuture().join();
    assertEquals(List.of(keys.updated(), keys.deleted(), keys.recreated()).stream().sorted().toList(),
        historicalRange.records().stream().map(StoredRecord::key).sorted().toList());
    assertEquals(StorageMetadata.State.TOMBSTONE, historicalRange.records().stream()
        .filter(record -> record.key().equals(keys.recreated())).findFirst().orElseThrow()
        .metadata().state());
    assertTrue(historical.get(keys.inserted(), 2, ReadProjection.FULL).join().isEmpty());
  }

  private static PreparedCandidate candidate(TransactionId transactionId, StoredRecord record) {
    return new PreparedCandidate(transactionId, 1, 0, 2, record);
  }

  private record ReadKeys(ByteKey updated, ByteKey deleted, ByteKey inserted,
      ByteKey recreated, ByteKey unselected) {}

  private record ReadFixture(ReadKeys keys, StorageTableBinding binding,
      HBaseRecordStore physical, ManifestCatalog catalog, ReadViewService views, RequestScope scope,
      ReadViewRecordStore current) {
    private ReadViewRecordStore open(OptionalLong revision) {
      return M2ReadViewIT.open(views, binding, physical, catalog, scope, revision);
    }
  }

  private static ReadViewRecordStore open(ReadViewService views, StorageTableBinding binding,
      HBaseRecordStore physical, ManifestCatalog catalog, RequestScope scope, OptionalLong revision) {
    return new ReadViewRecordStore(views.open(revision, AUTHORIZATION, scope).join(), physical,
        catalog, binding.codec()::bucket, SALT.profile().bucketCount(), scope);
  }

  private static StoredRecord live(ByteKey key, long createRevision, long revision,
      long version, String value) {
    byte[] bytes = bytes(value);
    return StoredRecord.live(key, new StorageMetadata(createRevision, revision, version,
        0, StorageMetadata.State.LIVE, bytes.length), bytes);
  }

  private static StoredRecord tombstone(ByteKey key, long createRevision, long revision) {
    return StoredRecord.metadataOnly(key, new StorageMetadata(createRevision, revision, 2,
        0, StorageMetadata.State.TOMBSTONE, 0));
  }

  private static ByteKey keyInBucket(String prefix, int bucket) {
    for (int suffix = 0; suffix < 100_000; suffix++) {
      ByteKey candidate = ByteKey.copyOf(bytes(prefix + "-" + suffix));
      if (bindingCodecBucket(candidate) == bucket) return candidate;
    }
    throw new IllegalStateException("could not find a fixture key for the requested bucket");
  }

  private static int bindingCodecBucket(ByteKey key) {
    byte[] prefix = SALT.salt(key.bytes());
    return ((prefix[0] & 0xff) << 8) | (prefix[1] & 0xff);
  }

  private static StorageTableBinding table(StorageTableBinding.Layout layout) {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    return new StorageTableBinding(TableName.valueOf("m2_j25_" + suffix), FAMILY, layout, SALT);
  }

  private static ReadViewPinRegistry pinRegistry() {
    return (view, scope) -> {
      long generation = PIN_GENERATIONS.incrementAndGet();
      return CompletableFuture.completedFuture(new ReadViewPinRegistry.Pin() {
        @Override public long generation() { return generation; }
        @Override public void close() {}
      });
    };
  }

  private static ByteKey digest(int value) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, (byte) value);
    return ByteKey.copyOf(digest);
  }

  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
  private static String text(byte[] value) { return new String(value, StandardCharsets.UTF_8); }
  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(90, java.util.concurrent.TimeUnit.SECONDS);
  }

  private static final class MemoryControl implements CommitControlStore {
    private final CommitState state;
    private MemoryControl(CommitState state) { this.state = state; }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState,
        RequestScope scope) { return CompletableFuture.completedFuture(false); }
    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed,
        CommitState replacement, RequestScope scope) { return CompletableFuture.completedFuture(false); }
  }

  private static final class MemoryManifestRepository implements ManifestRepository {
    private final Map<ByteKey, ManifestRoot> roots = new HashMap<>();
    private final Map<ByteKey, ManifestPage> pages = new HashMap<>();
    private final Map<ByteKey, ManifestDirectoryPage> directories = new HashMap<>();
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();

    private ManifestRoot add(TransactionId transactionId, long ownerEpoch, long baseSequence,
        long baseRevision, List<PreparedCandidate> entries,
        java.util.function.ToIntFunction<ByteKey> bucketSelector) {
      List<ManifestCandidateReference> references = entries.stream()
          .map(candidate -> ManifestCandidateReference.from(candidate,
              bucketSelector.applyAsInt(candidate.record().key())))
          .sorted(Comparator.comparingInt(ManifestCandidateReference::bucket)
              .thenComparing(ManifestCandidateReference::logicalKey)).toList();
      if (references.stream().map(ManifestCandidateReference::bucket).distinct().count() != 1) {
        throw new IllegalArgumentException("fixture candidates must share one salt bucket");
      }
      ManifestPage page = ManifestPage.create(transactionId, 0, references);
      entries.forEach(candidate -> candidates.put(candidate.systemIdentity(), candidate));
      pages.put(page.systemIdentity(), page);
      long valueBytes = entries.stream()
          .mapToLong(candidate -> candidate.record().metadata().valueLength()).sum();
      ManifestRoot root = ManifestRoot.create(transactionId, ownerEpoch, baseSequence, baseRevision,
          entries.size(), valueBytes, Optional.of(page.reference()));
      roots.put(root.systemIdentity(), root);
      return root;
    }

    private void addPrivateCandidate(PreparedCandidate candidate) {
      candidates.put(candidate.systemIdentity(), candidate);
    }

    @Override public int candidateBucket(PreparedCandidate candidate) {
      return bindingCodecBucket(candidate.record().key());
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot root, RequestScope scope) { return created(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity,
        RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(roots.get(identity))); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity,
        RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(candidates.get(identity))); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity,
        RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(pages.get(identity))); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(directories.get(identity)));
    }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity,
        RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity,
        RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }
}
