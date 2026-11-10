package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.TxnWorkingSet;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Real HBase base scans and durable private pages across independent buckets. */
class M3WorksetIT {
  @Test
  void orderedPrivateOverlayMergesWithRealScansAcrossBucketsAndLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      verifyLayout(layout);
    }
  }

  private void verifyLayout(StorageTableBinding.Layout layout) throws Exception {
    try (var environment = M3TestEnvironment.start(layout, 4)) {
      RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3));
      var limits = new BudgetManager.Limits(2, 0, 4, 4, 4, 0, 32L << 20);
      var manager = new BudgetManager(limits, Map.of("public", limits), limits);
      var account = manager.admit("public", scope,
          new BudgetManager.Reservation(0, 1L << 20, 1L << 20, 1L << 20, 64 * 1024));
      try (var lease = account.tryRetain(BudgetManager.Memory.CANDIDATE, 64 * 1024).orElseThrow()) {
        verifyOverlay(environment, scope, account);
      } finally {
        account.close();
        scope.close();
        scope.cleanupCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
        scope.physicalProducersCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
      }
      assertEquals(0, manager.snapshot().allocatedBytes());
      assertEquals(0, manager.snapshot().reservedBytes());
    }
  }

  private void verifyOverlay(M3TestEnvironment environment, RequestScope scope,
      BudgetManager.Account account) throws Exception {
    var primary = environment.openPrimary(scope);
    var grouped = findBucketKeys(environment);
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    var invocation = InvocationBinding.begin(InvocationId.create(), digest, digest, TransactionId.create(),
        1, 0, 1);
    var workset = new TxnWorkingSet(invocation, 64 * 1024, environment.records(),
        new HBaseManifestRepository(environment.records()), scope, account,
        environment.primary().codec()::bucket);
    workset.begin().join();
    for (int bucket = 0; bucket < 4; bucket++) {
      List<ByteKey> keys = grouped.get(bucket).stream().sorted().toList();
      assertEquals(3, keys.size());
      StoredRecord first = record(keys.get(0), "old-first");
      StoredRecord second = record(keys.get(1), "old-second");
      primary.put(first).join();
      primary.put(second).join();
      workset.put(new PutCommand(keys.get(0), bytes("new-first"), 0, false, false, false),
          Optional.of(first)).join();
      workset.put(new PutCommand(keys.get(2), bytes("new-third"), 0, false, false, false),
          Optional.empty()).join();
    }
    for (int bucket = 0; bucket < 4; bucket++) {
      verifyBucket(primary, workset, grouped.get(bucket).stream().sorted().toList(), bucket);
    }
    assertEquals(8, workset.candidateCount());
    assertEquals(8, workset.eventCount());
  }

  private Map<Integer, List<ByteKey>> findBucketKeys(M3TestEnvironment environment) {
    var grouped = new HashMap<Integer, List<ByteKey>>();
    for (int number = 0; number < 512 && grouped.values().stream().mapToInt(List::size).sum() < 12; number++) {
      ByteKey key = key(String.format("bucket-key-%04d", number));
      int bucket = environment.primary().codec().bucket(key);
      var keys = grouped.computeIfAbsent(bucket, ignored -> new ArrayList<>());
      if (keys.size() < 3) keys.add(key);
    }
    assertEquals(4, grouped.size());
    return grouped;
  }

  private void verifyBucket(io.etcdhbase.core.storage.RecordStore primary, TxnWorkingSet workset,
      List<ByteKey> keys, int bucket) {
    var physical = primary.startScan(new ScanRequest(bucket, KeyRange.all(), Optional.empty(),
        1, ReadProjection.FULL, 16, 16 * 1024, 64)).completion().toCompletableFuture().join();
    assertTrue(physical.exhausted());
    assertEquals(List.of(keys.get(0), keys.get(1)),
        physical.records().stream().map(StoredRecord::key).sorted().toList());
    var base = physical.records().stream().sorted(Comparator.comparing(StoredRecord::key)).toList();
    var merged = workset.merge(bucket, KeyRange.all(), AsyncCursor.fromIterator(base.iterator()));
    for (String expected : List.of("new-first", "old-second", "new-third")) {
      assertArrayEquals(bytes(expected), merged.next().join().orElseThrow().value().orElseThrow());
    }
    assertTrue(merged.next().join().isEmpty());
    assertArrayEquals(bytes("old-first"), primary.get(keys.get(0), 1, ReadProjection.FULL)
        .join().orElseThrow().value().orElseThrow(), "unselected private pages must not alter the base");
  }

  private static StoredRecord record(ByteKey key, String value) {
    byte[] bytes = bytes(value);
    return StoredRecord.live(key, new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, bytes.length), bytes);
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
