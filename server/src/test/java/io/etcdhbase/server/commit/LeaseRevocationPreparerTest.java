package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseBindingReadView;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Exercises the actual preparer, readers, fixed view, codecs and budget; only storage is controlled. */
class LeaseRevocationPreparerTest {
  private static final LeaseIdentity IDENTITY = new LeaseIdentity(-7, 2);
  private static final ByteKey KEY_A = ByteKey.copyOf(new byte[] {'a'});
  private static final ByteKey KEY_B = ByteKey.copyOf(new byte[] {'b'});
  private static final long POINT_BYTES = LeaseCostAdmission.MAXIMUM_REVOCATION_POINT_BYTES;
  private static final LeaseCostAdmission COSTS = new LeaseCostAdmission(LeaseCostAdmission.DEFAULT_LIMITS);

  @Test
  void preparesConsistentDeletesPrevKvUnbindsAndTerminalStateUntilClosed() throws Exception {
    try (Fixture f = new Fixture()) {
      try (var prepared = f.prepare().get(5, TimeUnit.SECONDS)) {
        assertEquals(IDENTITY, prepared.identity());
        assertEquals(2, prepared.deletedCount());
        assertEquals(List.of(KEY_A, KEY_B), f.readKeys);
        assertEquals(1, f.finishes);
        assertEquals(InvocationBinding.State.PREPARING, f.invocation.state());
        long allocated = f.budgets.snapshot().allocatedBytes();
        assertTrue(allocated >= f.retainedBytes());
        assertPreparedRows(prepared, f.rows);
        assertTerminalRecords(prepared, f.metadata.state);
        assertThrows(UnsupportedOperationException.class, () -> prepared.candidates().clear());
        prepared.close();
        assertEquals(allocated - f.retainedBytes(), f.budgets.snapshot().allocatedBytes());
        prepared.close();
        assertEquals(allocated - f.retainedBytes(), f.budgets.snapshot().allocatedBytes());
      }
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void secondKvReadFailureRejectsTheWholeResultAndReleasesReservations() throws Exception {
    try (Fixture f = new Fixture()) {
      var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      f.read = key -> key.equals(KEY_B) ? CompletableFuture.failedFuture(failure)
          : CompletableFuture.completedFuture(Optional.of(f.rows.get(key)));
      assertSame(failure, failure(f.prepare()));
      assertEquals(List.of(KEY_A, KEY_B), f.readKeys);
      assertEquals(0, f.finishes);
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void mismatchedKvLeaseOrCreateRevisionRejectsTheWholeResult() throws Exception {
    for (boolean wrongLease : List.of(false, true)) {
      try (Fixture f = new Fixture()) {
        StoredRecord bad = StoredRecord.live(KEY_B, new StorageMetadata(wrongLease ? 1 : 2,
            4, 2, wrongLease ? 9 : IDENTITY.leaseId(), StorageMetadata.State.LIVE, 4), new byte[4]);
        f.read = key -> CompletableFuture.completedFuture(Optional.of(key.equals(KEY_B) ? bad : f.rows.get(key)));
        assertInstanceOf(IllegalStateException.class, failure(f.prepare()));
        assertEquals(0, f.finishes);
        f.close();
        assertEquals(0, f.budgets.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void readerConstructionFailureReleasesTheAlreadyRetainedCandidateMemory() throws Exception {
    try (Fixture f = new Fixture(1L << 20, 1L << 20)) {
      assertInstanceOf(IllegalArgumentException.class, failure(f.prepare()));
      assertTrue(f.budgets.snapshot().peakAllocatedBytes() >= f.retainedBytes());
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
      assertTrue(f.readKeys.isEmpty());
      assertEquals(0, f.finishes);
    }
  }

  @Test
  void insufficientCandidateCapacityFailsBeforeReadingKv() throws Exception {
    try (Fixture f = new Fixture(POINT_BYTES, 32L << 10)) {
      var failure = assertInstanceOf(BackendFailure.class, failure(f.prepare()));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertTrue(f.readKeys.isEmpty());
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void finishFailureReleasesAllPreparedMemoryWithoutReturningAPartialPlan() throws Exception {
    try (Fixture f = new Fixture()) {
      var failure = new IllegalStateException("pin finish failed");
      f.pin.finished = CompletableFuture.failedFuture(failure);
      assertSame(failure, failure(f.prepare()));
      assertEquals(List.of(KEY_A, KEY_B), f.readKeys);
      assertEquals(1, f.finishes);
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void throwingFinishSupplierReleasesAllPreparedMemory() throws Exception {
    try (Fixture f = new Fixture()) {
      f.throwOnFinish = true;
      assertInstanceOf(IllegalStateException.class, failure(f.prepare()));
      assertEquals(1, f.finishes);
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void cancellationKeepsPinAndBudgetUntilThePhysicalKvReadStops() throws Exception {
    try (Fixture f = new Fixture()) {
      f.blockFirstRead();
      var waiting = f.prepare();
      f.readStarted.get(5, TimeUnit.SECONDS);
      long retained = f.budgets.snapshot().allocatedBytes();
      assertTrue(waiting.cancel(false));
      f.scope.cancel();
      assertFalse(f.scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertFalse(f.account.termination().toCompletableFuture().isDone());
      assertEquals(retained, f.budgets.snapshot().allocatedBytes());
      assertEquals(0, f.pin.closes);
      f.blocked.complete(Optional.of(f.rows.get(KEY_A)));
      f.scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
      assertEquals(1, f.pin.closes);
      assertEquals(List.of(KEY_A), f.readKeys);
      assertEquals(0, f.finishes);
      assertTrue(waiting.isCancelled());
    }
  }

  @Test
  void cancelledWaiterReclaimsALateSuccessfulPreparedResult() throws Exception {
    long auxiliaryMemory = auxiliaryReadFootprint();
    try (Fixture f = new Fixture()) {
      f.blockFirstRead();
      var waiting = f.prepare();
      f.readStarted.get(5, TimeUnit.SECONDS);
      assertTrue(waiting.cancel(false));
      assertFalse(f.scope.isClosed());
      f.blocked.complete(Optional.of(f.rows.get(KEY_A)));
      assertEquals(List.of(KEY_A, KEY_B), f.readKeys);
      assertEquals(1, f.finishes);
      assertFalse(f.scope.isClosed());
      assertEquals(auxiliaryMemory, f.budgets.snapshot().allocatedBytes(),
          "only the auxiliary reader's scoped buffers may remain after an undelivered Prepared");
      f.close();
      assertTrue(waiting.isCancelled());
    }
  }

  @Test
  void cancellationDuringPinFinishCannotReturnASuccessfulPlan() throws Exception {
    try (Fixture f = new Fixture()) {
      f.pin.finished = new CompletableFuture<>();
      var waiting = f.prepare();
      assertEquals(1, f.finishes);
      assertTrue(f.budgets.snapshot().allocatedBytes() >= f.retainedBytes());
      f.scope.cancel();
      assertFalse(waiting.isDone());
      f.pin.finished.complete(null);
      try {
        var inactive = assertInstanceOf(BackendFailure.class, failure(waiting));
        assertEquals(BackendFailure.Category.CANCELLED, inactive.category());
      } finally {
        if (!waiting.isCompletedExceptionally()) waiting.join().close();
      }
      f.close();
      assertEquals(0, f.budgets.snapshot().allocatedBytes());
    }
  }

  /** Positive control separates request-owned reader buffers from the Prepared's own reservation. */
  private static long auxiliaryReadFootprint() throws Exception {
    try (Fixture control = new Fixture(); var prepared = control.prepare().get(5, TimeUnit.SECONDS)) {
      long allocated = control.budgets.snapshot().allocatedBytes();
      prepared.close();
      long auxiliary = control.budgets.snapshot().allocatedBytes();
      assertEquals(allocated - control.retainedBytes(), auxiliary);
      return auxiliary;
    }
  }

  private static Throwable failure(CompletableFuture<?> result) {
    var failure = assertThrows(CompletionException.class, result::join);
    Throwable cause = failure.getCause();
    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
    return cause;
  }

  private static void assertPreparedRows(LeaseRevocationPreparer.Prepared prepared,
      Map<ByteKey, StoredRecord> rows) {
    assertEquals(2, prepared.events().size());
    assertEquals(2, prepared.bindingChanges().size());
    for (int i = 0; i < 2; i++) {
      var candidate = prepared.candidates().get(i);
      var event = prepared.events().get(i);
      StoredRecord previous = rows.get(candidate.record().key());
      assertEquals(PreparedEvent.Kind.DELETE, event.kind());
      assertEquals(i, event.ordinal());
      assertEquals(candidate.record().metadata(), event.current().metadata());
      assertEquals(StorageMetadata.State.TOMBSTONE, event.current().metadata().state());
      assertEquals(6, event.current().metadata().modificationRevision());
      assertEquals(0, event.current().metadata().leaseId());
      assertArrayEquals(previous.value().orElseThrow(), event.previous().orElseThrow().value().orElseThrow());
      assertEquals(previous.metadata(), event.previous().orElseThrow().metadata());
      var change = prepared.bindingChanges().get(i);
      assertEquals(previous.key(), change.key());
      assertEquals(IDENTITY.leaseId(), change.previousLeaseId());
      assertEquals(0, change.currentLeaseId());
    }
  }

  private static void assertTerminalRecords(LeaseRevocationPreparer.Prepared prepared, LeaseState previous) {
    var records = prepared.auxiliaryRecords();
    assertEquals(List.of(SystemRecordType.LEASE_STATE, SystemRecordType.LEASE_ENUMERATION,
        SystemRecordType.LEASE_BINDING, SystemRecordType.LEASE_BINDING),
        records.stream().map(record -> record.targetType()).toList());
    LeaseState revoked = LeaseStateMessages.decode(records.getFirst().payload());
    assertEquals(LeaseState.Status.REVOKED, revoked.status());
    assertEquals(11, revoked.sequence());
    assertEquals(previous.expiryCandidateSequence(), revoked.expiryCandidateSequence());
    assertEquals(0, revoked.boundKeyCount());
    assertEquals(0, revoked.boundKeyBytes());
    assertEquals(0, revoked.revokeCostBytes());
    assertEquals(previous.bindingHistoryBytes() + 2 * COSTS.bindingHistoryRecordBytes(KEY_A),
        revoked.bindingHistoryBytes());
    for (int i = 0; i < 2; i++) {
      var binding = LeaseBindingMessages.decode(records.get(i + 2).payload());
      assertFalse(binding.bound());
      assertEquals(IDENTITY, binding.identity());
      assertEquals(prepared.candidates().get(i).record().key(), binding.key());
      assertEquals(11, binding.sequence());
      assertEquals(6, binding.keyModRevision());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30));
    private final Map<ByteKey, StoredRecord> rows = Map.of(KEY_A, row(KEY_A, 3), KEY_B, row(KEY_B, 4));
    private final List<ByteKey> readKeys = new ArrayList<>();
    private final CompletableFuture<Void> readStarted = new CompletableFuture<>();
    private final TestPin pin = new TestPin();
    private final Metadata metadata = new Metadata(rows);
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final PinnedReadView pinned;
    private final LeaseReadView leases;
    private final LeaseRevocationPreparer preparer;
    private final InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        digest(), digest(), TransactionId.create(), 2, 10, 5);
    private CompletableFuture<Optional<StoredRecord>> blocked;
    private Function<ByteKey, CompletableFuture<Optional<StoredRecord>>> read =
        key -> CompletableFuture.completedFuture(Optional.ofNullable(rows.get(key)));
    private int finishes;
    private boolean throwOnFinish;

    private Fixture() throws Exception { this(2 * POINT_BYTES, 1L << 20); }

    private Fixture(long output, long candidates) throws Exception {
      var limits = new BudgetManager.Limits(2, 2, 4, 2, 2, 1L << 20, 128L << 20);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope,
          new BudgetManager.Reservation(0, 64L << 10, 64L << 20, output, candidates));
      var manifests = new ManifestCatalog(unused(ManifestRepository.class));
      pinned = openPinnedView(manifests);
      leases = new LeaseReadView(metadata.store, manifests, LeaseStateMessages::decode, 10, 1L << 20);
      var bindings = new LeaseBindingReader(metadata.store, manifests, LeaseBindingMessages::decode,
          4, 4096, 10, 1L << 20, 10, 4096, 10, 1L << 20);
      var points = new LeaseBindingReadView(metadata.store, manifests, LeaseBindingMessages::decode, 10, 1L << 20);
      var planner = new LeaseBindingMutationPlanner(leases, points,
          () -> new LeaseTimeObservation(1_000, 1_000, true), COSTS);
      preparer = new LeaseRevocationPreparer(leases, bindings, planner, COSTS,
          metadata.store);
    }

    private PinnedReadView openPinnedView(ManifestCatalog manifests) {
      var state = CommitState.restore(CommitState.LEASE_BINDING_FORMAT_VERSION, digest(),
          Optional.of("fixture"), 2, 60_000, 1, 10, 5, 10, 5, Optional.empty(), 0);
      CommitControlStore control = port(CommitControlStore.class, (name, args) -> {
        if (!name.equals("read")) throw new AssertionError("unexpected control write");
        return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
      });
      ReadViewPinRegistry pins = new ReadViewPinRegistry() {
        @Override public CompletableFuture<Pin> register(ReadView view, RequestScope request) {
          throw new AssertionError("auxiliary protection is required");
        }
        @Override public CompletableFuture<Pin> registerAuxiliary(ReadView view, RequestScope request) {
          assertEquals(10, view.barrierSequence());
          return CompletableFuture.completedFuture(pin);
        }
      };
      return new ReadViewService(control, manifests, pins, digest()).openAuxiliary(digest(), scope).join();
    }

    private CompletableFuture<LeaseRevocationPreparer.Prepared> prepare() {
      RecordStore records = port(RecordStore.class, (name, args) -> {
        if (name.equals("pointReadUsesScanner")) return false;
        if (!name.equals("get")) throw new AssertionError("unexpected KV operation " + name);
        assertEquals(5L, args[1]);
        assertEquals(ReadProjection.FULL, args[2]);
        ByteKey key = (ByteKey) args[0];
        readKeys.add(key);
        CompletableFuture<Optional<StoredRecord>> operation = read.apply(key);
        // Data failure is distinct from physical cleanup; this controlled producer stops at completion.
        scope.trackPhysicalProducer(operation.handle((ignored, failure) -> null));
        readStarted.complete(null);
        return operation;
      });
      var proof = new ManifestCatalog.ProofReadBudget(scope, account, 100, 128L << 20);
      return preparer.prepare(IDENTITY, invocation, false, pinned, records, scope, account, proof, () -> {
        finishes++;
        if (throwOnFinish) throw new IllegalStateException("finish supplier failed");
        return leases.finish(pinned, scope);
      });
    }

    private void blockFirstRead() {
      blocked = new CompletableFuture<>();
      read = key -> key.equals(KEY_A) ? blocked
          : CompletableFuture.completedFuture(Optional.of(rows.get(key)));
    }

    private long retainedBytes() {
      return metadata.state.revokeCostBytes() + 2 * COSTS.bindingHistoryRecordBytes(KEY_A);
    }

    @Override public void close() throws Exception {
      if (blocked != null && !blocked.isDone()) blocked.complete(Optional.empty());
      if (!pin.finished.isDone()) pin.finished.complete(null);
      scope.close();
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
      assertEquals(0, budgets.snapshot().requests());
    }
  }

  private static final class TestPin implements ReadViewPinRegistry.Pin {
    private CompletableFuture<Void> finished = CompletableFuture.completedFuture(null);
    private int closes;
    @Override public long generation() { return 1; }
    @Override public void close() { closes++; }
    @Override public CompletableFuture<Void> complete(RequestScope scope) { return finished.thenRun(this::close); }
  }

  /** Fixed authoritative auxiliary rows; no selected overlay, writes or unbounded scan fallback. */
  private static final class Metadata {
    private final LeaseState state;
    private final List<LeaseBindingVersion> bindings;
    private final SystemRecordStore store = port(SystemRecordStore.class, this::invoke);

    private Metadata(Map<ByteKey, StoredRecord> rows) {
      long cost = rows.values().stream().mapToLong(row -> COSTS.revokeCostBytes(row.key(),
          row.value().orElseThrow().length)).sum();
      state = new LeaseState(IDENTITY, 60, 60_000, 0, LeaseState.Status.ACTIVE,
          10, 1, 2, 2, cost, 2 * COSTS.bindingHistoryRecordBytes(KEY_A));
      bindings = List.of(new LeaseBindingVersion(IDENTITY, KEY_A, true, 10, 1, 4),
          new LeaseBindingVersion(IDENTITY, KEY_B, true, 10, 1, 4));
    }

    private Object invoke(String name, Object[] args) {
      return switch (name) {
        case "bucket" -> 0;
        case "latestAtOrBefore" -> latest((SystemRecordType) args[0], (ByteKey) args[1], (long) args[2]);
        case "scanLeaseBindings" -> scan((long) args[0], (long) args[1], (Optional<?>) args[2],
            (int) args[3], (long) args[4]);
        default -> throw new AssertionError("unexpected metadata operation " + name);
      };
    }

    private CompletableFuture<Optional<SystemRecordEntry>> latest(SystemRecordType type, ByteKey route, long sequence) {
      assertEquals(10, sequence);
      if (type == SystemRecordType.LEASE_STATE) {
        assertEquals(LeaseRecordIdentity.routePrefix(IDENTITY.leaseId()), route);
        return CompletableFuture.completedFuture(Optional.of(new SystemRecordEntry(
            LeaseRecordIdentity.stateVersion(IDENTITY.leaseId(), 10), LeaseStateMessages.encode(state))));
      }
      assertEquals(SystemRecordType.LEASE_BINDING, type);
      return CompletableFuture.completedFuture(bindings.stream().filter(binding ->
          LeaseBindingIdentity.keyRoutePrefix(IDENTITY.leaseId(), IDENTITY.incarnation(), binding.key()).equals(route))
          .map(binding -> new SystemRecordEntry(
              binding.recordIdentity().encode(), LeaseBindingMessages.encode(binding)))
          .findFirst());
    }

    private CompletableFuture<SystemRecordPage> scan(long leaseId, long incarnation,
        Optional<?> after, int limit, long maximumBytes) {
      assertEquals(IDENTITY.leaseId(), leaseId);
      assertEquals(IDENTITY.incarnation(), incarnation);
      var matching = bindings.stream().map(binding -> new SystemRecordEntry(
          binding.recordIdentity().encode(), LeaseBindingMessages.encode(binding)))
          .filter(entry -> after.isEmpty() || entry.identity().compareTo((ByteKey) after.orElseThrow()) > 0).toList();
      var page = matching.stream().limit(limit).toList();
      long bytes = page.stream().mapToLong(SystemRecordEntry::encodedBytes).sum();
      assertTrue(bytes <= maximumBytes);
      return CompletableFuture.completedFuture(new SystemRecordPage(page,
          page.isEmpty() ? Optional.empty() : Optional.of(page.getLast().identity()),
          matching.size() <= limit, bytes));
    }
  }

  private static StoredRecord row(ByteKey key, int length) {
    return StoredRecord.live(key, new StorageMetadata(1, 4, 2, IDENTITY.leaseId(),
        StorageMetadata.State.LIVE, length), new byte[length]);
  }

  private static ByteKey digest() { return ByteKey.copyOf(new byte[32]); }

  private static <T> T port(Class<T> type, BiFunction<String, Object[], Object> operation) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
        (proxy, method, args) -> operation.apply(method.getName(), args)));
  }

  private static <T> T unused(Class<T> type) {
    return port(type, (name, args) -> { throw new AssertionError("unexpected " + type.getSimpleName() + "." + name); });
  }
}
