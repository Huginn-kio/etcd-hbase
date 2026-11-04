package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.RowKeyCodec;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class J22ManifestRepositoryIT {
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "j22-manifest";
  private static final String SALT_PROFILE = "j22-manifest-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void persistsPagesEventsAndSealedRootOnDisposableHBase() throws Exception {
    TableName table = TableName.valueOf("j22_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, FAMILY, TABLE_GROUP, SALT_PROFILE, SALT);
    var descriptor = tableDescriptor(table);

    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, descriptor, SALT.splitKeys().toArray(byte[][]::new));
      var records = new CountingSystemRecordStore(HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join());
      var repository = new HBaseManifestRepository(records);
      TransactionId transactionId = TransactionId.create();
      List<PreparedCandidate> candidates = candidatesInTwoBuckets(repository, transactionId);
      List<PreparedEvent> events = preparedEvents(transactionId, 65);

      verifyCandidatePersistence(repository, candidates, scope);
      verifyEventPersistence(repository, events, transactionId, scope);
      ManifestRoot restored = prepareAndReadManifest(repository, candidates, events, transactionId, scope);
      verifyCatalogTraversal(repository, restored, candidates, events, scope);
      verifyOversizedSystemKeyRejectedBeforePersistence(repository, records, scope);
    }
  }

  private static TableDescriptor tableDescriptor(TableName table) {
    return TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", TABLE_GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count",
            Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes",
            Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", "CONTROL,INVOCATION,CANDIDATE,MANIFEST")
        .build();
  }

  private static void verifyCandidatePersistence(HBaseManifestRepository repository,
      List<PreparedCandidate> candidates, RequestScope scope) {
    PreparedCandidate first = candidates.getFirst();
    assertEquals(SystemRecordStore.CreateResult.CREATED, repository.createCandidate(first, scope).join());
    assertEquals(SystemRecordStore.CreateResult.ALREADY_PRESENT,
        repository.createCandidate(first, scope).join());
    assertEquals(SystemRecordStore.CreateResult.CONFLICT,
        repository.createCandidate(candidate(first.transactionId(), first.record().key().bytes(),
            new byte[] {9}), scope).join());

    TransactionId otherTransaction = TransactionId.create();
    PreparedCandidate sameKeyOtherTransaction = candidate(otherTransaction,
        first.record().key().bytes(), new byte[] {9});
    assertEquals(SystemRecordStore.CreateResult.CREATED,
        repository.createCandidate(sameKeyOtherTransaction, scope).join());
    assertEquals(first.record().key(), repository.readCandidate(first.systemIdentity(), scope)
        .join().orElseThrow().record().key());
    assertEquals(first.record().key(), repository.readCandidate(
        sameKeyOtherTransaction.systemIdentity(), scope).join().orElseThrow().record().key());
  }

  private static void verifyEventPersistence(HBaseManifestRepository repository,
      List<PreparedEvent> events, TransactionId transactionId, RequestScope scope) {
    PreparedEvent first = events.getFirst();
    assertEquals(SystemRecordStore.CreateResult.CREATED, repository.createEvent(first, scope).join());
    PreparedEvent restored = repository.readEvent(first.systemIdentity(), scope).join().orElseThrow();
    assertTrue(restored.previous().orElseThrow().hasValue());
    assertArrayEquals(first.previous().orElseThrow().value().orElseThrow(),
        restored.previous().orElseThrow().value().orElseThrow());
    assertEquals(SystemRecordStore.CreateResult.ALREADY_PRESENT,
        repository.createEvent(first, scope).join());
    assertEquals(SystemRecordStore.CreateResult.CONFLICT,
        repository.createEvent(event(transactionId, 0, new byte[] {'x'}), scope).join());
  }

  private static ManifestRoot prepareAndReadManifest(HBaseManifestRepository repository,
      List<PreparedCandidate> candidates, List<PreparedEvent> events,
      TransactionId transactionId, RequestScope scope) {
    ManifestRoot prepared = new ManifestPreparation(repository,
        new ManifestPreparation.Limits(candidates.size(), candidates.size(), events.size(), 1_000_000))
        .prepare(transactionId, 1, 0, 1, candidates.iterator(), events.iterator(), scope).join();
    ManifestRoot restored = repository.readRoot(prepared.systemIdentity(), scope).join().orElseThrow();
    assertEquals(prepared.digest(), restored.digest());
    assertTrue(restored.candidateDirectory().orElseThrow().leafPageCount() >= 2);
    assertTrue(restored.eventDirectory().orElseThrow().leafPageCount() >= 2);
    return restored;
  }

  private static void verifyCatalogTraversal(HBaseManifestRepository repository,
      ManifestRoot restored, List<PreparedCandidate> candidates, List<PreparedEvent> events,
      RequestScope scope) {
    List<ByteKey> visitedCandidates = new ArrayList<>();
    List<Long> visitedEvents = new ArrayList<>();
    ManifestCatalog catalog = new ManifestCatalog(repository);
    ManifestCatalog.Summary candidatesRead = catalog.forEachCandidate(restored, scope, value -> {
      visitedCandidates.add(value.record().key());
      return CompletableFuture.completedFuture(null);
    }).join();
    ManifestCatalog.Summary eventsRead = catalog.forEachEvent(restored, scope, value -> {
      visitedEvents.add(value.ordinal());
      return CompletableFuture.completedFuture(null);
    }).join();
    assertEquals(candidates.size(), candidatesRead.count());
    assertEquals(events.size(), eventsRead.count());
    assertEquals(candidates.stream().map(value -> value.record().key()).toList(), visitedCandidates);
    assertEquals(events.stream().map(PreparedEvent::ordinal).toList(), visitedEvents);
  }

  private static void verifyOversizedSystemKeyRejectedBeforePersistence(
      HBaseManifestRepository repository, CountingSystemRecordStore records, RequestScope scope) {
    byte[] largestKvKey = new byte[HConstants.MAX_ROW_LENGTH - SALT.profile().saltWidthBytes() - 2];
    java.util.Arrays.fill(largestKvKey, (byte) 'x');
    ByteKey largestLogicalKey = ByteKey.copyOf(largestKvKey);
    assertEquals(HConstants.MAX_ROW_LENGTH,
        new RowKeyCodec(SALT, RowKeyCodec.Shape.CURRENT).encode(largestLogicalKey, 1).length);
    PreparedCandidate oversizedSystemKey = candidate(TransactionId.create(), largestKvKey, new byte[] {1});
    int writesBefore = records.writeCalls;
    assertThrows(CompletionException.class, () -> new ManifestPreparation(repository,
        new ManifestPreparation.Limits(1, 1)).prepare(oversizedSystemKey.transactionId(), 1, 0, 1,
            List.of(oversizedSystemKey).iterator(), scope).join());
    assertEquals(writesBefore, records.writeCalls,
        "a system row too long for HBase must be rejected before persistence");
  }

  private static List<PreparedEvent> preparedEvents(TransactionId transactionId, int count) {
    List<PreparedEvent> result = new ArrayList<>(count);
    for (int ordinal = 0; ordinal < count; ordinal++) {
      byte[] key = new byte[] {'e', (byte) (ordinal >>> 8), (byte) ordinal};
      Optional<StoredRecord> previous = Optional.empty();
      if (ordinal == 0) {
        byte[] previousValue = new byte[] {9};
        previous = Optional.of(StoredRecord.live(ByteKey.copyOf(key),
            new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, previousValue.length),
            previousValue));
      }
      result.add(event(transactionId, ordinal, key, previous));
    }
    return List.copyOf(result);
  }

  private static List<PreparedCandidate> candidatesInTwoBuckets(
      HBaseManifestRepository repository, TransactionId transactionId) {
    Map<Integer, List<PreparedCandidate>> byBucket = new TreeMap<>();
    for (int index = 0; index < 10_000; index++) {
      byte[] key = String.format("candidate-%05d", index)
          .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
      PreparedCandidate candidate = candidate(transactionId, key, new byte[] {(byte) index});
      byBucket.computeIfAbsent(repository.candidateBucket(candidate), ignored -> new ArrayList<>())
          .add(candidate);
      long populated = byBucket.values().stream().filter(values -> values.size() >= 2).count();
      if (populated >= 2) break;
    }
    List<PreparedCandidate> result = byBucket.values().stream()
        .filter(values -> values.size() >= 2)
        .limit(2)
        .flatMap(List::stream)
        .sorted(Comparator.comparingInt(repository::candidateBucket)
            .thenComparing(value -> value.record().key()))
        .toList();
    if (result.size() != 4) throw new IllegalStateException("could not construct two bucket fixtures");
    return result;
  }

  private static PreparedCandidate candidate(TransactionId transactionId, byte[] key, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.LIVE, value.length);
    return new PreparedCandidate(transactionId, 1, 0, 1,
        StoredRecord.live(ByteKey.copyOf(key), metadata, value));
  }

  private static PreparedEvent event(TransactionId transactionId, long ordinal, byte[] key) {
    return event(transactionId, ordinal, key, Optional.empty());
  }

  private static PreparedEvent event(TransactionId transactionId, long ordinal, byte[] key,
      Optional<StoredRecord> previous) {
    byte[] value = {(byte) ordinal};
    StorageMetadata metadata = new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.LIVE, value.length);
    return new PreparedEvent(transactionId, 1, 0, 1, ordinal, PreparedEvent.Kind.PUT,
        StoredRecord.live(ByteKey.copyOf(key), metadata, value), previous);
  }

  private static final class CountingSystemRecordStore implements SystemRecordStore {
    private final SystemRecordStore delegate;
    private int writeCalls;

    private CountingSystemRecordStore(SystemRecordStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return delegate.read(type, identity, scope);
    }

    @Override
    public int bucket(SystemRecordType type, ByteKey identity) {
      return delegate.bucket(type, identity);
    }

    @Override
    public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      writeCalls++;
      return delegate.createIfAbsent(type, identity, value, scope);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      return delegate.compareAndSet(type, identity, expected, replacement, scope);
    }
  }
}
