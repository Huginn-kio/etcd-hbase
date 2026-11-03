package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SnapshotPinManagerTest {
  private static final ByteKey PROFILE = digest(3);
  private static final ByteKey AUTHORIZATION = digest(9);

  @Test
  void durableFloorsProtectExistingViewsAndRejectNewOlderViews() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(3, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    ReadView oldView = stableView(5);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(oldView, scope).join();
      SnapshotGcBoundary advanced = manager.advanceRegistrationFloors(8, 4, scope).join();
      assertEquals(5, advanced.safeRevision());
      assertEquals(4, advanced.safeSequence());
      assertEquals(1, advanced.protections().size());
      assertEquals(5, advanced.protections().getFirst().requiredRevision());

      Throwable rejected = failure(() -> manager.register(stableView(7), scope).join());
      assertInstanceOf(IllegalStateException.class, rejected);

      pin.complete(scope).join();
      SnapshotGcBoundary afterCompletion = manager.readBoundary(scope).join();
      assertEquals(8, afterCompletion.safeRevision());
      assertEquals(4, afterCompletion.safeSequence());
      assertTrue(afterCompletion.protections().isEmpty());
    }
  }

  @Test
  void selectedMetadataDecisionUsesItsOwnSequenceBoundaryAndReference() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    ReadView selected = selectedMetadataView(5);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(selected, scope).join();
      SnapshotGcBoundary boundary = manager.advanceRegistrationFloors(8, 3, scope).join();
      assertEquals(5, boundary.safeRevision());
      assertEquals(1, boundary.safeSequence());
      assertEquals(1, boundary.protections().size());
      SnapshotGcBoundary.Protection protection = boundary.protections().getFirst();
      assertEquals(1, protection.requiredSequence());
      assertTrue(protection.transactionId().isPresent());
      assertTrue(protection.rootIdentity().isPresent());
      assertTrue(protection.rootDigest().isPresent());

      Throwable rejected = failure(() -> manager.register(selectedMetadataView(5), scope).join());
      assertInstanceOf(IllegalStateException.class, rejected);
      pin.close();
      pin.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void expiryAloneDoesNotReleaseProtectionAndRevocationFencesTheOldLease() {
    var records = new MemoryRecords();
    var clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(1));
    SnapshotPinManager manager = open(records, configuration, clock);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      manager.advanceRegistrationFloors(9, 4, scope).join();
      clock.advance(Duration.ofSeconds(2));
      SnapshotGcBoundary expiredButNotRevoked = manager.readBoundary(scope).join();
      assertEquals(5, expiredButNotRevoked.safeRevision());
      assertEquals(4, expiredButNotRevoked.safeSequence());
      assertEquals(1, expiredButNotRevoked.protections().size());

      SnapshotGcBoundary revoked = manager.revokeExpiredPins(scope).join();
      assertTrue(revoked.protections().isEmpty());
      assertEquals(9, revoked.safeRevision());
      assertEquals(4, revoked.safeSequence());
      assertInstanceOf(BackendFailure.class, failure(() -> pin.renew(scope).join()));
    }
  }

  @Test
  void terminalSlotCanBeReusedOnlyWithANewerFencingGeneration() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin first = manager.register(stableView(5), scope).join();
      Throwable capacity = failure(() -> manager.register(stableView(6), scope).join());
      assertInstanceOf(BackendFailure.class, capacity);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) capacity).category());

      first.complete(scope).join();
      ReadViewPinRegistry.Pin second = manager.register(stableView(6), scope).join();
      assertTrue(second.generation() > first.generation());
      assertInstanceOf(BackendFailure.class, failure(() -> first.renew(scope).join()));
      second.complete(scope).join();
    }
  }

  @Test
  void unknownCasRepliesAreResolvedFromTheDurableSlot() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      records.loseNextCasReplyAfterWrite();
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      assertEquals(1, manager.readBoundary(scope).join().protections().size());

      records.loseNextCasReplyAfterWrite();
      pin.complete(scope).join();
      assertTrue(manager.readBoundary(scope).join().protections().isEmpty());
      pin.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void registrationRacesFloorAdvanceOnTheSameShardCas() throws Exception {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    records.pauseNextShardReads(2);

    CompletableFuture<Optional<ReadViewPinRegistry.Pin>> registration = CompletableFuture.supplyAsync(() -> {
      try (RequestScope scope = scope()) {
        try {
          return Optional.of(manager.register(stableView(5), scope).join());
        } catch (CompletionException rejected) {
          return Optional.empty();
        }
      }
    });
    CompletableFuture<SnapshotGcBoundary> floorAdvance = CompletableFuture.supplyAsync(() -> {
      try (RequestScope scope = scope()) {
        return manager.advanceRegistrationFloors(6, 0, scope).join();
      }
    });

    SnapshotGcBoundary boundary = floorAdvance.get(5, TimeUnit.SECONDS);
    Optional<ReadViewPinRegistry.Pin> pin = registration.get(5, TimeUnit.SECONDS);
    if (pin.isPresent()) {
      assertEquals(5, boundary.safeRevision());
      assertEquals(1, boundary.protections().size());
      pin.orElseThrow().close();
      pin.orElseThrow().releaseCompletion().toCompletableFuture().join();
    } else {
      assertEquals(6, boundary.safeRevision());
      assertTrue(boundary.protections().isEmpty());
    }
  }

  @Test
  void storedConfigurationIsImmutableAndShardRowsMustBeComplete() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      Throwable mismatch = failure(() -> SnapshotPinManager.open(records,
          new SnapshotPinConfiguration(2, 3, Duration.ofSeconds(2)), scope).join());
      assertInstanceOf(IllegalStateException.class, mismatch);
      SnapshotPinManager manager = SnapshotPinManager.open(records, configuration, scope).join();
      assertEquals(0, manager.readBoundary(scope).join().safeRevision());
      records.remove(SystemRecordType.SNAPSHOT_PIN, shardIdentity(1));
      assertInstanceOf(IllegalStateException.class,
          failure(() -> manager.readBoundary(scope).join()));
    }
  }

  private static SnapshotPinManager open(MemoryRecords records,
      SnapshotPinConfiguration configuration, Clock clock) {
    try (RequestScope scope = scope()) {
      return SnapshotPinManager.open(records, configuration, clock, scope).join();
    }
  }

  private static SnapshotPinManager open(MemoryRecords records,
      SnapshotPinConfiguration configuration, RequestScope scope) {
    return SnapshotPinManager.open(records, configuration, scope).join();
  }

  private static ReadView stableView(long revision) {
    return ReadView.capture(CommitState.initial(PROFILE, revision), revision, AUTHORIZATION,
        Optional.empty(), Optional.empty());
  }

  private static ReadView selectedMetadataView(long revision) {
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = ManifestRoot.create(transaction, 1, 0, revision, 0, 0, Optional.empty());
    PublicationSlot slot = new PublicationSlot(transaction, InvocationId.create(), 1,
        0, revision, 1, revision, false, root.systemIdentity(), root.digest());
    CommitState selected = CommitState.initial(PROFILE, revision)
        .claimOwner("pin-test-owner", 1, 10)
        .publish(slot);
    return ReadView.capture(selected, revision, AUTHORIZATION,
        Optional.of(CommitDecision.from(slot)), Optional.of(root));
  }

  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }

  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static Throwable failure(Runnable action) {
    try {
      action.run();
    } catch (CompletionException failure) {
      Throwable cause = failure.getCause();
      return cause == null ? failure : cause;
    }
    throw new AssertionError("expected operation to fail");
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<RecordKey, ByteKey> rows = new HashMap<>();
    private volatile CyclicBarrier readBarrier;
    private final AtomicInteger pausedReads = new AtomicInteger();
    private final AtomicBoolean loseCasReplyAfterWrite = new AtomicBoolean();

    @Override
    public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      Optional<ByteKey> value;
      synchronized (this) {
        value = Optional.ofNullable(rows.get(new RecordKey(type, identity)));
      }
      if (type == SystemRecordType.SNAPSHOT_PIN && pausedReads.getAndUpdate(count ->
          Math.max(0, count - 1)) > 0) {
        try {
          readBarrier.await(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
          return CompletableFuture.failedFuture(failure);
        }
      }
      return CompletableFuture.completedFuture(value);
    }

    @Override
    public int bucket(SystemRecordType type, ByteKey identity) { return 0; }

    @Override
    public synchronized CompletableFuture<CreateResult> createIfAbsent(
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

    @Override
    public synchronized CompletableFuture<Boolean> compareAndSet(SystemRecordType type,
        ByteKey identity, Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      RecordKey key = new RecordKey(type, identity);
      ByteKey existing = rows.get(key);
      if (!expected.equals(Optional.ofNullable(existing))) {
        return CompletableFuture.completedFuture(false);
      }
      rows.put(key, replacement);
      if (loseCasReplyAfterWrite.compareAndSet(true, false)) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }

    private void pauseNextShardReads(int count) {
      readBarrier = new CyclicBarrier(count);
      pausedReads.set(count);
    }

    private void loseNextCasReplyAfterWrite() { loseCasReplyAfterWrite.set(true); }

    private synchronized void remove(SystemRecordType type, ByteKey identity) {
      rows.remove(new RecordKey(type, identity));
    }
  }

  private record RecordKey(SystemRecordType type, ByteKey identity) {}

  private static ByteKey shardIdentity(int shard) {
    return ByteKey.copyOf(java.nio.ByteBuffer.allocate(Integer.BYTES * 2)
        .putInt(1).putInt(shard).array());
  }

  private static final class MutableClock extends Clock {
    private Instant current;

    private MutableClock(Instant current) { this.current = current; }
    private void advance(Duration duration) { current = current.plus(duration); }
    @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
    @Override public Clock withZone(ZoneId zone) { return this; }
    @Override public Instant instant() { return current; }
  }
}
