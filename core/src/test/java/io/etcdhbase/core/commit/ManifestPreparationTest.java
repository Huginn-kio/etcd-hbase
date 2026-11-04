package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ManifestPreparationTest {
  private static final long OWNER_EPOCH = 1;
  private static final long BASE_SEQUENCE = 0;
  private static final long BASE_REVISION = 1;

  @Test
  void preparesMultipleCandidateAndEventPagesAndStreamsThemInOrder() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    List<PreparedCandidate> candidates = new ArrayList<>();
    List<PreparedEvent> events = new ArrayList<>();
    for (int index = 0; index < 130; index++) {
      byte[] key = numberedKey(index);
      candidates.add(candidate(transactionId, key, new byte[] {(byte) index}));
      events.add(event(transactionId, index, key, new byte[] {(byte) index}));
    }

    try (RequestScope scope = scope()) {
      ManifestRoot root = prepare(repository, transactionId, candidates, events,
          new ManifestPreparation.Limits(130, 130, 130, 1_000_000), scope);

      List<ByteKey> candidateKeys = new ArrayList<>();
      ManifestCatalog.Summary candidateSummary = join(
          new ManifestCatalog(repository).forEachCandidate(root, scope, value -> {
            candidateKeys.add(value.record().key());
            return CompletableFuture.completedFuture(null);
          }));
      List<Long> ordinals = new ArrayList<>();
      ManifestCatalog.Summary eventSummary = join(
          new ManifestCatalog(repository).forEachEvent(root, scope, value -> {
            ordinals.add(value.ordinal());
            return CompletableFuture.completedFuture(null);
          }));

      assertEquals(130, candidateSummary.count());
      assertEquals(130, eventSummary.count());
      assertEquals(2, repository.pages.size());
      assertEquals(3, repository.eventPages.size());
      assertEquals(1, repository.directories.size());
      assertEquals(1, repository.eventDirectories.size());
      assertEquals(candidates.stream().map(value -> value.record().key()).toList(), candidateKeys);
      assertEquals(java.util.stream.LongStream.range(0, 130).boxed().toList(), ordinals);
    }
  }

  @Test
  void catalogTraversesNestedCandidateAndEventDirectories() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedCandidate first = candidate(transactionId, new byte[] {'a'}, new byte[] {1});
    PreparedCandidate second = candidate(transactionId, new byte[] {'b'}, new byte[] {2});
    repository.put(first);
    repository.put(second);
    ManifestPage firstPage = ManifestPage.create(transactionId, 0,
        List.of(ManifestCandidateReference.from(first, 0)));
    ManifestPage secondPage = ManifestPage.create(transactionId, 1,
        List.of(ManifestCandidateReference.from(second, 0)));
    repository.put(firstPage);
    repository.put(secondPage);
    ManifestDirectoryPage firstDirectory = ManifestDirectoryPage.create(transactionId, 0, 1,
        List.of(firstPage.reference()));
    ManifestDirectoryPage secondDirectory = ManifestDirectoryPage.create(transactionId, 1, 1,
        List.of(secondPage.reference()));
    ManifestDirectoryPage candidateRoot = ManifestDirectoryPage.create(transactionId, 2, 2,
        List.of(firstDirectory.reference(), secondDirectory.reference()));
    repository.put(firstDirectory);
    repository.put(secondDirectory);
    repository.put(candidateRoot);

    PreparedEvent firstEvent = event(transactionId, 0, new byte[] {'a'}, new byte[] {1});
    PreparedEvent secondEvent = event(transactionId, 1, new byte[] {'b'}, new byte[] {2});
    repository.put(firstEvent);
    repository.put(secondEvent);
    EventPage firstEventPage = EventPage.create(transactionId, 0,
        List.of(PreparedEventReference.from(firstEvent)));
    EventPage secondEventPage = EventPage.create(transactionId, 1,
        List.of(PreparedEventReference.from(secondEvent)));
    EventDirectoryPage firstEventDirectory = EventDirectoryPage.create(transactionId, 0, 1,
        List.of(firstEventPage.reference()));
    EventDirectoryPage secondEventDirectory = EventDirectoryPage.create(transactionId, 1, 1,
        List.of(secondEventPage.reference()));
    EventDirectoryPage eventRoot = EventDirectoryPage.create(transactionId, 2, 2,
        List.of(firstEventDirectory.reference(), secondEventDirectory.reference()));
    repository.put(firstEventPage);
    repository.put(secondEventPage);
    repository.put(firstEventDirectory);
    repository.put(secondEventDirectory);
    repository.put(eventRoot);
    ManifestRoot root = ManifestRoot.create(transactionId, OWNER_EPOCH, BASE_SEQUENCE,
        BASE_REVISION, 2, 2, Optional.of(candidateRoot.reference()), 2,
        firstEvent.retainedBytes() + secondEvent.retainedBytes(), Optional.of(eventRoot.reference()));

    try (RequestScope scope = scope()) {
      List<ByteKey> keys = new ArrayList<>();
      List<Long> ordinals = new ArrayList<>();
      ManifestCatalog catalog = new ManifestCatalog(repository);
      assertEquals(2, join(catalog.forEachCandidate(root, scope, value -> {
        keys.add(value.record().key());
        return CompletableFuture.completedFuture(null);
      })).count());
      assertEquals(2, join(catalog.forEachEvent(root, scope, value -> {
        ordinals.add(value.ordinal());
        return CompletableFuture.completedFuture(null);
      })).count());
      assertEquals(List.of(ByteKey.copyOf(new byte[] {'a'}), ByteKey.copyOf(new byte[] {'b'})), keys);
      assertEquals(List.of(0L, 1L), ordinals);
    }
  }

  @Test
  void candidateIdentitiesIsolateTransactionsWithTheSameLogicalKey() {
    MemoryRepository repository = new MemoryRepository();
    TransactionId firstTransaction = TransactionId.create();
    TransactionId secondTransaction = TransactionId.create();
    PreparedCandidate first = candidate(firstTransaction, new byte[] {'k'}, new byte[] {1});
    PreparedCandidate second = candidate(secondTransaction, new byte[] {'k'}, new byte[] {2});

    try (RequestScope scope = scope()) {
      ManifestRoot firstRoot = prepare(repository, firstTransaction, List.of(first), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      ManifestRoot secondRoot = prepare(repository, secondTransaction, List.of(second), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      assertFalse(first.systemIdentity().equals(second.systemIdentity()));
      assertEquals(first, repository.candidates.get(first.systemIdentity()));
      assertEquals(second, repository.candidates.get(second.systemIdentity()));
      assertFalse(firstRoot.systemIdentity().equals(secondRoot.systemIdentity()));
    }
  }

  @Test
  void rejectsOrderAndBudgetsBeforeWritingTheRejectedRecord() {
    TransactionId transactionId = TransactionId.create();
    try (RequestScope scope = scope()) {
      MemoryRepository unordered = new MemoryRepository();
      var orderFailure = assertThrows(CompletionException.class, () -> prepare(unordered,
          transactionId,
          List.of(candidate(transactionId, new byte[] {'b'}, new byte[] {1}),
              candidate(transactionId, new byte[] {'a'}, new byte[] {2})), List.of(),
          new ManifestPreparation.Limits(2, 2), scope));
      assertTrue(orderFailure.getCause() instanceof IllegalArgumentException);
      assertEquals(1, unordered.candidates.size());
      assertTrue(unordered.roots.isEmpty());

      MemoryRepository candidateBudget = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(candidateBudget, transactionId,
          List.of(candidate(transactionId, new byte[] {'a'}, new byte[] {1})), List.of(),
          new ManifestPreparation.Limits(0, 0), scope));
      assertEquals(0, candidateBudget.writeCount);
      assertTrue(candidateBudget.roots.isEmpty());

      MemoryRepository eventBudget = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(eventBudget, transactionId,
          List.of(), List.of(event(transactionId, 0, new byte[] {'a'}, new byte[] {1})),
          new ManifestPreparation.Limits(0, 0, 0, 0), scope));
      assertEquals(0, eventBudget.writeCount);
      assertTrue(eventBudget.roots.isEmpty());

      MemoryRepository rowLimit = new MemoryRepository();
      rowLimit.maximumLogicalKeyLength = 1;
      assertThrows(CompletionException.class, () -> prepare(rowLimit, transactionId,
          List.of(candidate(transactionId, new byte[] {'a', 'b'}, new byte[] {1})), List.of(),
          new ManifestPreparation.Limits(1, 1), scope));
      assertEquals(0, rowLimit.writeCount);
      assertTrue(rowLimit.roots.isEmpty());

      MemoryRepository unorderedEvents = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(unorderedEvents, transactionId,
          List.of(), List.of(event(transactionId, 1, new byte[] {'a'}, new byte[] {1})),
          new ManifestPreparation.Limits(0, 0, 2, 1_000), scope));
      assertEquals(0, unorderedEvents.writeCount);
      assertTrue(unorderedEvents.roots.isEmpty());
    }
  }

  @Test
  void partialCandidateWriteFailureNeverSealsTheManifest() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    repository.failCandidateWriteNumber = 2;
    try (RequestScope scope = scope()) {
      assertThrows(CompletionException.class, () -> prepare(repository, transactionId,
          List.of(candidate(transactionId, new byte[] {'a'}, new byte[] {1}),
              candidate(transactionId, new byte[] {'b'}, new byte[] {2})), List.of(),
          new ManifestPreparation.Limits(2, 2), scope));
      assertEquals(1, repository.candidates.size());
      assertTrue(repository.roots.isEmpty());
      assertEquals(0, repository.sealCalls);
    }
  }

  @Test
  void catalogRejectsMissingOrMismatchedPagesAndRestoreRejectsBadDigest() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedCandidate candidate = candidate(transactionId, new byte[] {'a'}, new byte[] {1});
    try (RequestScope scope = scope()) {
      ManifestRoot root = prepare(repository, transactionId, List.of(candidate), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      ManifestPage page = repository.pages.values().iterator().next();
      ManifestCatalog catalog = new ManifestCatalog(repository);

      repository.pages.remove(page.systemIdentity());
      assertThrows(CompletionException.class, () -> catalog.forEachCandidate(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      PreparedCandidate other = candidate(transactionId, new byte[] {'b'}, new byte[] {2});
      repository.put(ManifestPage.create(transactionId, page.pageIndex(),
          List.of(ManifestCandidateReference.from(other, 0))));
      assertThrows(CompletionException.class, () -> catalog.forEachCandidate(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      byte[] wrongDigest = page.digest().bytes();
      wrongDigest[0] ^= 1;
      assertThrows(IllegalArgumentException.class, () -> ManifestPage.restore(transactionId,
          page.pageIndex(), page.candidates(), ByteKey.copyOf(wrongDigest)));
    }
  }

  @Test
  void catalogRejectsMissingOrMismatchedEventPages() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedEvent event = event(transactionId, 0, new byte[] {'a'}, new byte[] {1});
    repository.put(event);
    EventPage page = EventPage.create(transactionId, 0,
        List.of(PreparedEventReference.from(event)));
    repository.put(page);
    ManifestRoot root = ManifestRoot.create(transactionId, OWNER_EPOCH, BASE_SEQUENCE,
        BASE_REVISION, 0, 0, Optional.empty(), 1, event.retainedBytes(), Optional.of(page.reference()));
    ManifestCatalog catalog = new ManifestCatalog(repository);

    try (RequestScope scope = scope()) {
      repository.eventPages.remove(page.systemIdentity());
      assertThrows(CompletionException.class, () -> catalog.forEachEvent(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      PreparedEvent changed = event(transactionId, 0, new byte[] {'b'}, new byte[] {2});
      repository.put(EventPage.create(transactionId, 0,
          List.of(PreparedEventReference.from(changed))));
      assertThrows(CompletionException.class, () -> catalog.forEachEvent(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());
    }
  }

  private static ManifestRoot prepare(MemoryRepository repository, TransactionId transactionId,
      List<PreparedCandidate> candidates, List<PreparedEvent> events,
      ManifestPreparation.Limits limits, RequestScope scope) {
    return join(new ManifestPreparation(repository, limits).prepare(transactionId, OWNER_EPOCH,
        BASE_SEQUENCE, BASE_REVISION, candidates.iterator(), events.iterator(), scope));
  }

  private static PreparedCandidate candidate(TransactionId transactionId, byte[] key, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length);
    return new PreparedCandidate(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION,
        StoredRecord.live(ByteKey.copyOf(key), metadata, value));
  }

  private static PreparedEvent event(TransactionId transactionId, long ordinal, byte[] key, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length);
    StoredRecord record = StoredRecord.live(ByteKey.copyOf(key), metadata, value);
    return new PreparedEvent(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, ordinal,
        PreparedEvent.Kind.PUT, record, Optional.empty());
  }

  private static byte[] numberedKey(int index) {
    return String.format("k%03d", index).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  }

  private static RequestScope scope() {
    return RequestScope.withTimeout(Duration.ofMinutes(1));
  }

  private static <T> T join(CompletableFuture<T> future) {
    return future.join();
  }

  private static final class MemoryRepository implements ManifestRepository {
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
    private final Map<ByteKey, ManifestPage> pages = new HashMap<>();
    private final Map<ByteKey, ManifestDirectoryPage> directories = new HashMap<>();
    private final Map<ByteKey, PreparedEvent> events = new HashMap<>();
    private final Map<ByteKey, EventPage> eventPages = new HashMap<>();
    private final Map<ByteKey, EventDirectoryPage> eventDirectories = new HashMap<>();
    private final Map<ByteKey, ManifestRoot> roots = new HashMap<>();
    private int maximumLogicalKeyLength = Integer.MAX_VALUE;
    private int candidateWriteCount;
    private int failCandidateWriteNumber = -1;
    private int writeCount;
    private int sealCalls;

    @Override
    public int candidateBucket(PreparedCandidate candidate) {
      if (candidate.record().key().length() > maximumLogicalKeyLength) {
        throw new IllegalArgumentException("candidate key exceeds test row limit");
      }
      return 0;
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) {
      candidateWriteCount++;
      if (candidateWriteCount == failCandidateWriteNumber) {
        return CompletableFuture.failedFuture(new IllegalStateException("injected candidate write failure"));
      }
      writeCount++;
      return create(candidates, candidate.systemIdentity(), candidate, PreparedCandidate::contentDigest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) {
      writeCount++;
      return create(pages, page.systemIdentity(), page, ManifestPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) {
      writeCount++;
      return create(directories, page.systemIdentity(), page, ManifestDirectoryPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) {
      writeCount++;
      return create(events, event.systemIdentity(), event, PreparedEvent::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) {
      writeCount++;
      return create(eventPages, page.systemIdentity(), page, EventPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) {
      writeCount++;
      return create(eventDirectories, page.systemIdentity(), page, EventDirectoryPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot root, RequestScope scope) {
      writeCount++;
      sealCalls++;
      return create(roots, root.systemIdentity(), root, ManifestRoot::digest);
    }

    @Override
    public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
      return completed(roots.get(identity));
    }

    @Override
    public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity, RequestScope scope) {
      return completed(candidates.get(identity));
    }

    @Override
    public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope) {
      return completed(pages.get(identity));
    }

    @Override
    public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) {
      return completed(directories.get(identity));
    }

    @Override
    public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity, RequestScope scope) {
      return completed(events.get(identity));
    }

    @Override
    public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity, RequestScope scope) {
      return completed(eventPages.get(identity));
    }

    @Override
    public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) {
      return completed(eventDirectories.get(identity));
    }

    private void put(PreparedCandidate value) { candidates.put(value.systemIdentity(), value); }
    private void put(ManifestPage value) { pages.put(value.systemIdentity(), value); }
    private void put(ManifestDirectoryPage value) { directories.put(value.systemIdentity(), value); }
    private void put(PreparedEvent value) { events.put(value.systemIdentity(), value); }
    private void put(EventPage value) { eventPages.put(value.systemIdentity(), value); }
    private void put(EventDirectoryPage value) { eventDirectories.put(value.systemIdentity(), value); }

    private static <T> CompletableFuture<SystemRecordStore.CreateResult> create(
        Map<ByteKey, T> records, ByteKey identity, T value,
        java.util.function.Function<T, ByteKey> digest) {
      T previous = records.putIfAbsent(identity, value);
      if (previous == null) return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
      return CompletableFuture.completedFuture(digest.apply(previous).equals(digest.apply(value))
          ? SystemRecordStore.CreateResult.ALREADY_PRESENT : SystemRecordStore.CreateResult.CONFLICT);
    }

    private static <T> CompletableFuture<Optional<T>> completed(T value) {
      return CompletableFuture.completedFuture(Optional.ofNullable(value));
    }
  }
}
