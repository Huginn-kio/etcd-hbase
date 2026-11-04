package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Public MiniCluster evidence for fenced M2 version materialization in both layouts. */
class M2CommitMaterializationIT {
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));
  private static final byte[] FAMILY = {'d'};
  private static final byte[] CURRENT_FENCE = {'f'};

  private static TemporaryHBaseClusterProcess cluster;
  private static HBaseBackendIdentityProvider provider;
  private static AsyncConnection connection;
  private static AuthorizedBackendIdentity identity;
  private static ThreadPoolExecutor worker;
  private static final List<TableName> TABLES = new ArrayList<>();

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start();
    worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32));
    AuthenticationScope authentication = new AuthenticationScope("m2", "public");
    provider = new HBaseBackendIdentityProvider("m2-simple", authentication,
        cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
        (client, scope) -> client.getAdmin().listNamespaces().thenApply(ignored -> null),
        2, 2, Duration.ofSeconds(30));
    try (var credentials = new BackendCredentials("m2-fixture", new char[0])) {
      identity = await(provider.authenticate(credentials, authentication).toCompletableFuture());
    }
    connection = await(provider.connectionFor(identity).toCompletableFuture());
  }

  @AfterAll
  static void stopCluster() throws Exception {
    Throwable failure = null;
    failure = attemptCleanup(failure, M2CommitMaterializationIT::deleteTables);
    failure = attemptCleanup(failure, M2CommitMaterializationIT::stopWorker);
    failure = attemptCleanup(failure, M2CommitMaterializationIT::closeProvider);
    failure = attemptCleanup(failure, M2CommitMaterializationIT::closeCluster);
    rethrowCleanupFailure(failure);
  }

  private static void deleteTables() throws Exception {
    if (connection != null) {
      for (TableName table : TABLES) {
        await(connection.getAdmin().disableTable(table));
        await(connection.getAdmin().deleteTable(table));
      }
    }
  }

  private static void stopWorker() throws Exception {
    if (worker != null) {
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  private static void closeProvider() throws Exception {
    if (provider != null) provider.close();
  }

  private static void closeCluster() throws Exception {
    if (cluster != null) cluster.close();
  }

  private static Throwable attemptCleanup(Throwable previous, CleanupAction cleanup) {
    try {
      cleanup.run();
    } catch (Throwable failure) {
      // Include fatal Errors so one failed close does not skip the remaining cleanup; rethrow
      // the first failure after all resources are attempted and retain later failures as suppressed.
      if (previous == null) return failure;
      if (previous != failure) previous.addSuppressed(failure);
    }
    return previous;
  }

  private static void rethrowCleanupFailure(Throwable failure) throws Exception {
    if (failure instanceof Exception exception) throw exception;
    if (failure instanceof Error error) throw error;
  }

  @FunctionalInterface
  private interface CleanupAction {
    void run() throws Exception;
  }

  @Test
  void materializesHistoryAndNativeVersionsAndFencesLateWriters() throws Exception {
    for (StorageTableBinding.Layout layout :
        List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      verifyLayout(layout);
    }
  }

  private void verifyLayout(StorageTableBinding.Layout layout) throws Exception {
    StorageTableBinding primary = createTable(layout);
    Optional<StorageTableBinding> current = layout == StorageTableBinding.Layout.HISTORY
        ? Optional.of(createTable(StorageTableBinding.Layout.CURRENT)) : Optional.empty();
    ByteKey key = ByteKey.copyOf(new byte[] {0, 0x4a, (byte) 0xff});
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      HBaseMaterializationStore materializer = await(HBaseMaterializationStore.open(
          provider, identity, primary, current, scope));
      StorageTableBinding currentBinding = current.orElse(primary);
      HBaseRecordStore historyReader = await(HBaseRecordStore.open(
          provider, identity, primary, scope, worker));
      HBaseRecordStore currentReader = currentBinding == primary
          ? historyReader
          : await(HBaseRecordStore.open(provider, identity, currentBinding, scope, worker));

      PreparedCandidate first = liveCandidate(key, 1, 0, 1, 2, 2, 1, new byte[] {2});
      assertEquals(MaterializationStore.ApplyResult.APPLIED,
          await(apply(materializer, first, scope)));
      assertEquals(MaterializationStore.ApplyResult.ALREADY_APPLIED,
          await(apply(materializer, first, scope)));

      PreparedCandidate second = liveCandidate(key, 2, 1, 2, 3, 2, 2, new byte[] {3});
      assertEquals(MaterializationStore.ApplyResult.APPLIED,
          await(apply(materializer, second, scope)));
      assertEquals(MaterializationStore.ApplyResult.CURRENT_SUPERSEDED,
          await(apply(materializer, first, scope)));
      assertValue(historyReader, key, 2, new byte[] {2});
      assertValue(historyReader, key, 3, new byte[] {3});
      assertValue(currentReader, key, Long.MAX_VALUE, new byte[] {3});

      PreparedCandidate deleted = tombstoneCandidate(key, 2, 2, 3, 4, 3);
      assertEquals(MaterializationStore.ApplyResult.APPLIED,
          await(apply(materializer, deleted, scope)));
      var tombstone = await(historyReader.get(key, 4, ReadProjection.FULL)).orElseThrow();
      assertEquals(StorageMetadata.State.TOMBSTONE, tombstone.metadata().state());
      assertFalse(tombstone.hasValue());

      PreparedCandidate recreated = liveCandidate(key, 3, 3, 4, 5, 5, 1, new byte[0]);
      assertEquals(MaterializationStore.ApplyResult.APPLIED,
          await(apply(materializer, recreated, scope)));
      var live = await(historyReader.get(key, 5, ReadProjection.FULL)).orElseThrow();
      assertEquals(StorageMetadata.State.LIVE, live.metadata().state());
      assertTrue(live.hasValue());
      assertArrayEquals(new byte[0], live.value().orElseThrow());
      assertEquals(MaterializationStore.ApplyResult.CURRENT_SUPERSEDED,
          await(apply(materializer, second, scope)));
      assertValue(currentReader, key, Long.MAX_VALUE, new byte[0]);

      if (layout == StorageTableBinding.Layout.NATIVE) {
        verifyNativeFenceVersions(primary, key);
      }
      System.out.printf("j24-materialization layout=%s idempotent=true lateWriterFenced=true"
          + " tombstoneRecreate=true%n", layout);
    }
  }

  private static CompletableFuture<MaterializationStore.ApplyResult> apply(
      HBaseMaterializationStore materializer, PreparedCandidate candidate, RequestScope scope)
      throws Exception {
    await(materializer.preflight(candidate, scope));
    return materializer.applySelected(decision(candidate), candidate, scope);
  }

  private static PreparedCandidate liveCandidate(ByteKey key, long ownerEpoch, long baseSequence,
      long baseRevision, long revision, long createRevision, long version, byte[] value) {
    TransactionId transactionId = TransactionId.create();
    StoredRecord record = StoredRecord.live(key,
        new StorageMetadata(createRevision, revision, version, 0,
            StorageMetadata.State.LIVE, value.length), value);
    return new PreparedCandidate(transactionId, ownerEpoch, baseSequence, baseRevision, record);
  }

  private static PreparedCandidate tombstoneCandidate(ByteKey key, long ownerEpoch,
      long baseSequence, long baseRevision, long revision, long createRevision) {
    TransactionId transactionId = TransactionId.create();
    StoredRecord record = StoredRecord.metadataOnly(key,
        new StorageMetadata(createRevision, revision, 3, 0, StorageMetadata.State.TOMBSTONE, 0));
    return new PreparedCandidate(transactionId, ownerEpoch, baseSequence, baseRevision, record);
  }

  private static CommitDecision decision(PreparedCandidate candidate) {
    TransactionId transactionId = candidate.transactionId();
    byte[] root = ByteBuffer.allocate(17).put(transactionId.bytes()).put((byte) 3).array();
    return new CommitDecision(transactionId, InvocationId.create(), candidate.baseSequence() + 1,
        candidate.baseRevision(), candidate.record().metadata().modificationRevision(),
        ByteKey.copyOf(root), candidate.contentDigest(), true);
  }

  private static void assertValue(HBaseRecordStore reader, ByteKey key, long revision, byte[] expected)
      throws Exception {
    var observed = await(reader.get(key, revision, ReadProjection.FULL)).orElseThrow();
    assertEquals(StorageMetadata.State.LIVE, observed.metadata().state());
    assertArrayEquals(expected, observed.value().orElseThrow());
  }

  private static void verifyNativeFenceVersions(StorageTableBinding binding, ByteKey key)
      throws Exception {
    byte[] row = binding.codec().encode(key, 1);
    Result result = await(connection.getTable(binding.table()).get(
        new Get(row).readVersions(64).addFamily(binding.family())));
    Set<Long> timestamps = Arrays.stream(result.rawCells())
        .filter(cell -> Arrays.equals(CellUtil.cloneQualifier(cell), CURRENT_FENCE))
        .map(cell -> cell.getTimestamp()).collect(java.util.stream.Collectors.toSet());
    assertEquals(Set.of(1L, 2L, 3L, 4L), timestamps);
    Set<Long> metadataTimestamps = Arrays.stream(result.rawCells())
        .filter(cell -> Arrays.equals(CellUtil.cloneQualifier(cell), HBaseMaterializationStore.METADATA))
        .map(cell -> cell.getTimestamp()).collect(java.util.stream.Collectors.toSet());
    assertEquals(timestamps, metadataTimestamps);
  }

  private static StorageTableBinding createTable(StorageTableBinding.Layout layout) throws Exception {
    TableName name = TableName.valueOf("m2_" + UUID.randomUUID().toString().replace("-", ""));
    StorageTableBinding binding = new StorageTableBinding(name, FAMILY, layout, SALT);
    await(connection.getAdmin().createTable(binding.descriptor(), SALT.splitKeys().toArray(byte[][]::new)));
    TABLES.add(name);
    return binding;
  }

  private static <T> T await(CompletableFuture<T> operation) throws Exception {
    return operation.get(90, TimeUnit.SECONDS);
  }
}
