package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TableGroupIdentityServiceTest {
  @Test
  void concurrentInitializersShareThePersistedWinnerAndPreserveHighBits() throws Exception {
    MemoryStore store = new MemoryStore();
    AtomicLong proposed = new AtomicLong(Long.MIN_VALUE);
    TableGroupIdentityService service = new TableGroupIdentityService(store, proposed::getAndIncrement);
    int contenders = 8;
    CyclicBarrier barrier = new CyclicBarrier(contenders);
    try (var executor = Executors.newFixedThreadPool(contenders)) {
      List<CompletableFuture<Long>> runs = new ArrayList<>();
      for (int index = 0; index < contenders; index++) {
        runs.add(CompletableFuture.supplyAsync(() -> {
          try { barrier.await(5, TimeUnit.SECONDS); }
          catch (Exception failure) { throw new CompletionException(failure); }
          return service.initialize(RequestScope.withoutDeadline()).join();
        }, executor));
      }
      long winner = runs.getFirst().join();
      assertTrue(winner < 0);
      for (var run : runs) assertEquals(winner, run.join());
      assertEquals(winner, service.initialize(RequestScope.withoutDeadline()).join());
    }
  }

  @Test
  void lostAcknowledgementResolvesTheDurableValueButPermissionFailuresStayFailures() {
    MemoryStore store = new MemoryStore();
    store.loseAcknowledgement = true;
    var service = new TableGroupIdentityService(store, () -> Long.MIN_VALUE);
    assertEquals(Long.MIN_VALUE, service.initialize(RequestScope.withoutDeadline()).join());
    int reads = store.reads.get();
    store.failPermission = true;
    var failure = assertThrows(CompletionException.class,
        () -> service.initialize(RequestScope.withoutDeadline()).join());
    assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
    assertEquals(reads, store.reads.get());
  }

  @Test
  void missingOrCorruptDurableIdentityIsNotInvented() {
    MemoryStore store = new MemoryStore();
    store.failUnknownBeforeWrite = true;
    var service = new TableGroupIdentityService(store, () -> 5);
    assertThrows(CompletionException.class, () -> service.initialize(RequestScope.withoutDeadline()).join());
    store.failUnknownBeforeWrite = false;
    store.values.put(ByteKey.copyOf(new byte[] {'c'}), ByteKey.copyOf(new byte[] {1, 2}));
    assertThrows(CompletionException.class, () -> service.initialize(RequestScope.withoutDeadline()).join());
    assertThrows(CompletionException.class, () -> new TableGroupIdentityService(new MemoryStore(), () -> 0)
        .initialize(RequestScope.withoutDeadline()).join());
  }

  @Test
  void openingAnEstablishedGroupNeverCreatesOrReplacesItsIdentity() {
    MemoryStore store = new MemoryStore();
    var service = new TableGroupIdentityService(store, () -> 5);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class, () -> service.readExisting(scope).join());
      assertTrue(store.values.isEmpty(), "runtime open must not invent a missing cluster ID");
      long created = service.initialize(scope).join();
      assertEquals(created, service.readExisting(scope).join());
      store.values.clear();
      assertThrows(CompletionException.class, () -> service.readExisting(scope).join());
      assertTrue(store.values.isEmpty(), "a lost identity must not be silently replaced");
    }
  }

  private static final class MemoryStore implements SystemRecordStore {
    private final ConcurrentHashMap<ByteKey, ByteKey> values = new ConcurrentHashMap<>();
    private boolean loseAcknowledgement;
    private boolean failPermission;
    private boolean failUnknownBeforeWrite;
    private final AtomicInteger reads = new AtomicInteger();

    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      assertEquals(SystemRecordType.TABLE_GROUP_IDENTITY, type);
      reads.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(identity)));
    }
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      assertEquals(SystemRecordType.TABLE_GROUP_IDENTITY, type);
      if (failPermission) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      if (failUnknownBeforeWrite) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      ByteKey existing = values.putIfAbsent(identity, value);
      if (loseAcknowledgement) {
        loseAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(existing == null ? CreateResult.CREATED
          : existing.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) { throw new UnsupportedOperationException(); }
  }
}
