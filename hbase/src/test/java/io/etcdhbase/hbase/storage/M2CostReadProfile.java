package io.etcdhbase.hbase.storage;

import static io.etcdhbase.hbase.storage.M1TestEnvironment.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitDecisionRepository;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.ReadViewRecordStore;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** Exercises the pinned read path while a selected transaction is only partly materialized. */
final class M2CostReadProfile {
  private static final ByteKey REQUEST_DIGEST = digest("j29-pending-read-request");
  private static final SnapshotPinConfiguration PIN_CONFIGURATION =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));

  record Context(M1TestEnvironment environment, StorageTableBinding.Layout layout,
      StorageTableBinding primary, SystemTableBinding systemBinding, int bucketCount,
      ByteKey authorizationDigest, Duration operationTimeout, SystemRecordStore records,
      M2CostMetrics.Control control, InvocationRepository invocations,
      InvocationService invocationService,
      ManifestRepository manifests, ManifestPreparation preparation, ManifestCatalog catalog,
      CommitOutcomeResolver outcomes, CommitDecisionRepository decisions,
      MaterializationStore materialization, CommitOwnerService.OwnerToken owner,
      M2CostRuntimeSampler runtime, LongSupplier maximumCandidatePayloadBytes) {}

  private record Pending(PublicationSlot slot,
      List<io.etcdhbase.core.commit.PreparedCandidate> candidates,
      long recoveryNowEpochMillis) {}

  private record PreparedRead(ReadViewRecordStore view, M2CostPinObservations pins, long openNanos) {}

  private record ReadMeasurements(long openNanos, long pointReadNanos, long returnedKeys,
      ScanTotals totals, M2CostPinObservations pins) {}

  private static final class RecoveryRun implements AutoCloseable {
    private final RequestScope scope;
    private final CompletableFuture<RecoveryManager.Result> completion;
    private final long startedNanos;

    RecoveryRun(RequestScope scope, CompletableFuture<RecoveryManager.Result> completion,
        long startedNanos) {
      this.scope = scope;
      this.completion = completion;
      this.startedNanos = startedNanos;
    }

    CompletableFuture<RecoveryManager.Result> completion() { return completion; }
    long elapsedNanos() { return System.nanoTime() - startedNanos; }

    @Override public void close() { scope.close(); }
  }

  private M2CostReadProfile() {}

  static void initializeFreshPins(SystemRecordStore records, RequestScope scope) throws Exception {
    await(SnapshotPinManager.initializeFresh(records, PIN_CONFIGURATION, scope));
  }

  static void run(Context context, long seed, String workloadDigest) throws Exception {
    Pending pending = createPendingPublication(context);
    M2CostRecoveryInterlock interlock = new M2CostRecoveryInterlock(
        context.materialization(), pending.candidates().size());
    try (RecoveryRun recovery = startRecovery(context, pending, interlock)) {
      await(interlock.firstRecoveryApplication());
      ReadMeasurements read = readSelectedPublication(context, pending, interlock);
      RecoveryManager.Result recovered = await(recovery.completion());
      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      assertTrue(recovered.owner().orElseThrow().epoch() > pending.slot().ownerEpoch());
      assertEquals(pending.candidates().size(), interlock.recoveryApplications());
      assertEquals(context.bucketCount() * 3, read.totals().pageRequests);
      assertEquals(context.bucketCount() * 2, read.totals().pageReopens);
      assertTrue(interlock.readerPagesDuringRecovery() >= interlock.recoveryApplications());
      assertTrue(interlock.readerPagesDuringRecovery() <= read.totals().pageRequests);
      if (read.pins().activePins() != 0) {
        throw new IllegalStateException("J29 read profile leaked a durable pin");
      }
      context.runtime().sample();
      printRead(context, seed, workloadDigest, read, recovery.elapsedNanos(), interlock);
      printResources(context, seed, workloadDigest, read.pins());
    }
  }

  private static Pending createPendingPublication(Context context) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(context.operationTimeout())) {
      ControlSnapshot observed = await(context.control().read(scope)).orElseThrow();
      long sequence = observed.state().publishedSequence();
      long revision = observed.state().publishedRevision();
      long recoveryNow = Math.addExact(observed.state().ownerLeaseDeadlineEpochMillis(), 1);
      TransactionId transaction = TransactionId.create();
      InvocationId invocationId = InvocationId.create();
      InvocationBinding proposed = InvocationBinding.begin(invocationId, REQUEST_DIGEST,
          context.authorizationDigest(), transaction, context.owner().epoch(), sequence, revision);
      InvocationBinding preparing = await(context.invocationService().bindOrJoin(proposed, scope));
      List<io.etcdhbase.core.commit.PreparedCandidate> candidates = candidates(
          context, transaction, sequence, revision, 8, 128);
      ManifestRoot root = await(context.preparation().prepare(transaction, context.owner().epoch(),
          sequence, revision, candidates.iterator(), scope));
      InvocationBinding sealed = await(context.invocationService().transition(preparing,
          preparing.seal(root.systemIdentity()), scope));
      ControlSnapshot current = await(context.control().read(scope)).orElseThrow();
      PublicationSlot slot = new PublicationSlot(transaction, invocationId, context.owner().epoch(),
          sequence, revision, sequence + 1, revision + 1, true, root.systemIdentity(), root.digest());
      assertTrue(await(context.control().compareAndSet(current, current.state().publish(slot), scope)));
      CommitOutcomeResolver.Resolution resolution = await(context.outcomes().resolve(sealed, scope));
      assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, resolution.state());
      assertEquals(MaterializationStore.ApplyResult.APPLIED, await(context.materialization()
          .applySelected(CommitDecision.from(slot), candidates.getFirst(), scope)));
      return new Pending(slot, candidates, recoveryNow);
    }
  }

  private static RecoveryRun startRecovery(Context context, Pending pending,
      M2CostRecoveryInterlock interlock) {
    RequestScope scope = RequestScope.withTimeout(context.operationTimeout());
    CommitFinalizationService recoveryFinalizer = new CommitFinalizationService(
        context.control(), context.outcomes(), context.decisions(), context.catalog(), interlock);
    RecoveryManager recovery = new RecoveryManager(context.systemBinding().profileFingerprint(),
        context.control(), new CommitOwnerService(context.control()), context.invocations(),
        context.outcomes(), recoveryFinalizer);
    long startedNanos = System.nanoTime();
    interlock.recoveryStarted();
    CompletableFuture<RecoveryManager.Result> completion;
    try {
      long leaseDeadline = Math.addExact(pending.recoveryNowEpochMillis(), Duration.ofHours(1).toMillis());
      completion = recovery.recover("j29-recovery-" + context.layout().name().toLowerCase(Locale.ROOT),
          pending.recoveryNowEpochMillis(), leaseDeadline, scope)
          .whenComplete((ignored, failure) -> interlock.recoveryCompleted());
    } catch (RuntimeException | Error failure) {
      // A fatal setup failure must also release the recovery scope before it propagates.
      interlock.recoveryCompleted();
      scope.close();
      throw failure;
    }
    return new RecoveryRun(scope, completion, startedNanos);
  }

  private static ReadMeasurements readSelectedPublication(Context context, Pending pending,
      M2CostRecoveryInterlock interlock) throws Exception {
    assertTrue(interlock.recoveryInFlight());
    try (RequestScope scope = RequestScope.withTimeout(context.operationTimeout());
        BudgetManager.Account account = readBudget(scope)) {
      PreparedRead read = openSelectedRead(context, scope);
      ScanTotals totals = new ScanTotals();
      ReadMeasurements measurements;
      try {
        long pointNanos = measureSelectedPoint(read.view(), pending, account);
        List<StoredRecord> records = scanBuckets(context, pending, read.view(), account, interlock, totals);
        verifySelectedKeys(pending, records);
        measurements = new ReadMeasurements(read.openNanos(), pointNanos, records.size(), totals, read.pins());
      } finally {
        await(read.view().finishRead());
      }
      assertTrue(read.pins().completionCount() > 0);
      return measurements;
    }
  }

  private static PreparedRead openSelectedRead(Context context, RequestScope scope) throws Exception {
    context.runtime().sample();
    SnapshotPinManager manager = await(SnapshotPinManager.open(context.records(), PIN_CONFIGURATION, scope));
    M2CostPinObservations pins = new M2CostPinObservations(manager);
    ReadViewService views = new ReadViewService(context.control(), context.catalog(), pins,
        context.systemBinding().profileFingerprint());
    HBaseRecordStore physical = context.environment().store(context.primary(), scope);
    long started = System.nanoTime();
    PinnedReadView pinned = await(views.open(OptionalLong.empty(), context.authorizationDigest(), scope));
    long openNanos = System.nanoTime() - started;
    context.runtime().sample();
    ReadViewRecordStore view = new ReadViewRecordStore(pinned, physical, context.catalog(),
        context.primary().codec()::bucket, context.bucketCount(), scope);
    return new PreparedRead(view, pins, openNanos);
  }

  private static long measureSelectedPoint(ReadViewRecordStore view, Pending pending,
      BudgetManager.Account account) throws Exception {
    long started = System.nanoTime();
    StoredRecord selected = await(view.get(pending.candidates().getFirst().record().key(),
        pending.slot().revision(), ReadProjection.FULL, account)).orElseThrow();
    long elapsed = System.nanoTime() - started;
    assertArrayEquals(pending.candidates().getFirst().record().value().orElseThrow(),
        selected.value().orElseThrow());
    return elapsed;
  }

  private static void verifySelectedKeys(Pending pending, List<StoredRecord> records) {
    records.sort(Comparator.comparing(StoredRecord::key));
    assertEquals(pending.candidates().stream().map(candidate -> candidate.record().key()).sorted().toList(),
        records.stream().map(StoredRecord::key).toList());
  }

  private static BudgetManager.Account readBudget(RequestScope scope) throws Exception {
    var limits = new BudgetManager.Limits(4, 16, 2, 2, 2, 1_000_000, 16L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    return manager.admit("public", scope,
        new BudgetManager.Reservation(128 * 1024, 128 * 1024, 1L << 20, 1L << 20, 1L << 20));
  }

  private static final class ScanTotals {
    private long elapsedNanos;
    private long physicalRows;
    private long returnedCellBytes;
    private long scanRpcCalls;
    private long directoryPages;
    private long leafPages;
    private long candidateBytes;
    private int pageRequests;
    private int pageReopens;
  }

  private static List<StoredRecord> scanBuckets(Context context, Pending pending,
      ReadViewRecordStore view, BudgetManager.Account account, M2CostRecoveryInterlock interlock,
      ScanTotals totals) throws Exception {
    List<StoredRecord> records = new ArrayList<>();
    KeyRange range = KeyRange.prefix(ByteKey.copyOf(("pending/"
        + context.layout().name().toLowerCase(Locale.ROOT) + "/").getBytes(StandardCharsets.UTF_8)));
    for (int bucket = 0; bucket < context.bucketCount(); bucket++) {
      Optional<ByteKey> after = Optional.empty();
      boolean exhausted = false;
      while (!exhausted) {
        ScanRequest request = new ScanRequest(bucket, range, after,
            pending.slot().revision(), ReadProjection.FULL, 1, 1L << 20, 512);
        if (after.isPresent()) totals.pageReopens++;
        long started = System.nanoTime();
        ScanOperation scan = view.startScan(request, account);
        StoragePage page;
        try {
          page = await(scan.completion().toCompletableFuture());
        } finally {
          scan.close();
        }
        totals.elapsedNanos += System.nanoTime() - started;
        totals.pageRequests++;
        totals.physicalRows += page.cost().physicalRows();
        totals.returnedCellBytes += page.cost().returnedCellBytes();
        totals.scanRpcCalls += page.cost().scanRpcCalls();
        totals.directoryPages += page.cost().observations()
            .getOrDefault(StoragePage.Observation.MANIFEST_DIRECTORY_PAGES, 0L);
        totals.leafPages += page.cost().observations()
            .getOrDefault(StoragePage.Observation.MANIFEST_LEAF_PAGES, 0L);
        totals.candidateBytes += page.cost().observations()
            .getOrDefault(StoragePage.Observation.MANIFEST_CANDIDATE_BYTES, 0L);
        context.runtime().sample();
        records.addAll(page.records());
        if (!page.records().isEmpty()) {
          after = Optional.of(page.records().getLast().key());
        } else if (!page.exhausted()) {
          throw new IllegalStateException("J29 page reopen did not advance the logical key");
        }
        exhausted = page.exhausted();
        interlock.readerPageCompleted();
      }
    }
    return records;
  }

  private static void printRead(Context context, long seed, String workloadDigest,
      ReadMeasurements read, long recoveryNanos, M2CostRecoveryInterlock interlock) {
    System.out.printf(Locale.ROOT,
        "j29-read {\"layout\":\"%s\",\"seed\":%d,\"workloadSha256\":\"%s\","
            + "\"readBarrierAndPinOpenMicros\":%d,\"pinRegisterCalls\":%d,"
            + "\"pinRegisterMicros\":%d,\"pinRenewCalls\":%d,\"pinRenewMicros\":%d,"
            + "\"pinCompleteCalls\":%d,\"pinCompleteMicros\":%d,\"peakPinOccupancy\":%d,"
            + "\"selectedCandidatePointReadMicros\":%d,"
            + "\"crossBucketFanoutMicros\":%d,\"bucketCount\":%d,\"pageRequests\":%d,"
            + "\"pageReopens\":%d,\"pagesWhileRecoveryActive\":%d,"
            + "\"recoveryMaterializationCalls\":%d,\"recoveryElapsedMicros\":%d,"
            + "\"returnedKeys\":%d,"
            + "\"recoveryPending\":true,\"partiallyMaterialized\":true,\"physicalRows\":%d,"
            + "\"returnedCellBytes\":%d,\"scanRpcCalls\":%d,\"manifestDirectoryPages\":%d,"
            + "\"manifestLeafPages\":%d,\"manifestCandidateBytes\":%d}%n",
        context.layout(), seed, workloadDigest, read.openNanos() / 1000,
        read.pins().registrationCount(), read.pins().registrationTimeNanos() / 1000,
        read.pins().renewalCount(), read.pins().renewalTimeNanos() / 1000,
        read.pins().completionCount(), read.pins().completionTimeNanos() / 1000,
        read.pins().peakPins(), read.pointReadNanos() / 1000, read.totals().elapsedNanos / 1000,
        context.bucketCount(), read.totals().pageRequests, read.totals().pageReopens,
        interlock.readerPagesDuringRecovery(), interlock.recoveryApplications(), recoveryNanos / 1000,
        read.returnedKeys(), read.totals().physicalRows, read.totals().returnedCellBytes,
        read.totals().scanRpcCalls, read.totals().directoryPages, read.totals().leafPages,
        read.totals().candidateBytes);
  }

  private static void printResources(Context context, long seed, String workloadDigest,
      M2CostPinObservations pins) {
    System.out.printf(Locale.ROOT,
        "j29-resource {\"layout\":\"%s\",\"seed\":%d,\"workloadSha256\":\"%s\","
            + "\"sampledPeakHeapBytes\":%d,\"sampledPeakDirectBufferBytes\":%d,\"sampledPeakThreadCount\":%d,"
            + "\"peakLogicalCandidatePayloadBytes\":%d,\"peakActiveScanners\":1,"
            + "\"activePinsAfterRelease\":%d,\"pinCapacity\":%d,"
            + "\"recoveryReserveMeasured\":false,\"crossProcessTakeoverMeasured\":false}%n",
        context.layout(), seed, workloadDigest, context.runtime().peakHeapBytes(),
        context.runtime().peakDirectBytes(), context.runtime().peakThreadCount(),
        context.maximumCandidatePayloadBytes().getAsLong(), pins.activePins(),
        PIN_CONFIGURATION.shardCount() * PIN_CONFIGURATION.slotsPerShard());
  }

  private static List<io.etcdhbase.core.commit.PreparedCandidate> candidates(Context context,
      TransactionId transaction, long sequence, long revision, int count, int valueBytes) {
    List<io.etcdhbase.core.commit.PreparedCandidate> result = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      int bucket = index % context.bucketCount();
      ByteKey key = keyInBucket(context, "pending/" + context.layout().name().toLowerCase(Locale.ROOT)
          + "/" + index, bucket);
      byte[] value = new byte[valueBytes];
      Arrays.fill(value, (byte) (0x20 + index));
      long modRevision = Math.addExact(revision, 1);
      StoredRecord record = StoredRecord.live(key,
          new StorageMetadata(modRevision, modRevision, 1, 0, StorageMetadata.State.LIVE, value.length),
          value);
      result.add(new io.etcdhbase.core.commit.PreparedCandidate(transaction,
          context.owner().epoch(), sequence, revision, record));
    }
    result.sort(Comparator.comparingInt((io.etcdhbase.core.commit.PreparedCandidate candidate) ->
        context.manifests().candidateBucket(candidate)).thenComparing(candidate -> candidate.record().key()));
    return List.copyOf(result);
  }

  private static ByteKey keyInBucket(Context context, String prefix, int bucket) {
    for (int suffix = 0; suffix < 100_000; suffix++) {
      ByteKey key = ByteKey.copyOf((prefix + "/" + suffix).getBytes(StandardCharsets.UTF_8));
      if (context.primary().codec().bucket(key) == bucket) return key;
    }
    throw new IllegalStateException("could not generate a key for the requested bucket");
  }

  private static ByteKey digest(String value) {
    try {
      return ByteKey.copyOf(java.security.MessageDigest.getInstance("SHA-256")
          .digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }
}
