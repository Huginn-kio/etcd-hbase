package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PublicationHandoff;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class M3ResultRepositoryIT {
  private static final byte[] FAMILY = {'s'};
  private static final String GROUP = "m3-result";
  private static final String PROFILE = "m3-result-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void persistsVerifiesAndRejectsDamagedResultOnDisposableHBase() throws Exception {
    TableName table = TableName.valueOf("m3_result_" + UUID.randomUUID().toString().replace("-", ""));
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, GROUP, PROFILE, SALT);
    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, tableDescriptor(table, GROUP, true), SALT.splitKeys().toArray(byte[][]::new));
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      HBaseResultRepository first = new HBaseResultRepository(records);
      HBaseResultRepository reopened = new HBaseResultRepository(records);
      long clusterId = new TableGroupIdentityService(records).initialize(scope).join();
      assertNotEquals(0, clusterId);
      assertEquals(clusterId, new TableGroupIdentityService(records).readExisting(scope).join());
      TableName otherTable = TableName.valueOf("m3_other_" + UUID.randomUUID().toString().replace("-", ""));
      String otherGroup = GROUP + "-other";
      environment.create(otherTable, tableDescriptor(otherTable, otherGroup, true),
          SALT.splitKeys().toArray(byte[][]::new));
      var otherBinding = new SystemTableBinding(otherTable, FAMILY, otherGroup, PROFILE, SALT);
      var otherRecords = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), otherBinding, scope).join();
      assertNotEquals(clusterId, new TableGroupIdentityService(otherRecords).initialize(scope).join());
      InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
          ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
      ResultIdentity identity = ResultIdentity.from(invocation);
      ByteKey logicalKey = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
      ResultPage page = new ResultPage(identity, 0,
          ResultMessages.encode(new PutResult(logicalKey, 1, Optional.empty())));
      TableName legacyTable = TableName.valueOf("m3_legacy_" + UUID.randomUUID().toString().replace("-", ""));
      environment.create(legacyTable, tableDescriptor(legacyTable, GROUP, false), SALT.splitKeys().toArray(byte[][]::new));
      var legacyBinding = new SystemTableBinding(legacyTable, FAMILY, GROUP, PROFILE, SALT);
      var legacyRecords = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), legacyBinding, scope).join();
      assertTrue(legacyRecords.read(SystemRecordType.CONTROL, ByteKey.copyOf(new byte[] {1}), scope).join().isEmpty());
      assertThrows(IllegalStateException.class, () -> new HBaseResultRepository(legacyRecords).createPage(page, scope));
      var reference = new ResultPreparation(first).prepare(identity, List.of(page), scope).join();
      var manifestStore = new HBaseManifestRepository(records);
      var manifests = new ManifestCatalog(manifestStore);
      var resultCatalog = new ResultCatalog(reopened);
      var root = new ManifestPreparation(manifestStore, new ManifestPreparation.Limits(0, 0, 0, 0))
          .prepareWithResult(invocation, List.<PreparedCandidate>of().iterator(),
              List.<PreparedEvent>of().iterator(), reference, resultCatalog,
              () -> CompletableFuture.completedFuture(null), scope).join();
      var sealed = invocation.seal(root.systemIdentity());
      MaterializationStore materialization = new MaterializationStore() {
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }
        @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(null);
        }
      };
      var verifier = new CommitPlanVerifier(manifests, materialization, resultCatalog);

      assertEquals(reference, new ResultCatalog(reopened).verify(reference, scope).join().reference());
      assertEquals(reference, new HBaseManifestRepository(records).readRoot(root.systemIdentity(), scope)
          .join().orElseThrow().resultReference().orElseThrow());
      assertEquals(root.digest(), verifier.verify(sealed, root, scope).join().digest());
      verifyRecoveryThroughRealControlAndInvocationRows(
          environment, binding, records, invocation, sealed, root, manifests, resultCatalog,
          materialization, verifier, logicalKey, scope);
      assertEquals(SystemRecordStore.CreateResult.CONFLICT,
          reopened.createPage(new ResultPage(identity, 0, new byte[] {7}), scope).join());
      assertEquals(identity, reopened.readPage(identity, 0, scope).join().orElseThrow().identity());

      // Fault injection after successful verification: a later reader must not trust the directory alone.
      ByteKey oldBytes = ByteKey.copyOf(ResultMessages.encode(page));
      ByteKey replacement = ByteKey.copyOf(new byte[] {1});
      assertTrue(records.compareAndSet(SystemRecordType.RESULT, page.systemIdentity(),
          Optional.of(oldBytes), replacement, scope).join());
      assertThrows(CompletionException.class, () -> new ResultCatalog(reopened).verify(reference, scope).join());
      assertThrows(CompletionException.class, () -> verifier.verify(sealed, root, scope).join());
    }
  }

  private static void verifyRecoveryThroughRealControlAndInvocationRows(
      M1TestEnvironment environment, SystemTableBinding binding, SystemRecordStore records,
      InvocationBinding preparing, InvocationBinding sealed, io.etcdhbase.core.commit.ManifestRoot root,
      ManifestCatalog manifests, ResultCatalog results, MaterializationStore materialization,
      CommitPlanVerifier verifier, ByteKey logicalKey, RequestScope scope) {
    var control = HBaseCommitControlStore.open(environment.factory(), environment.identity(), binding, scope).join();
    assertTrue(control.initializeIfAbsent(CommitState.initial(binding.profileFingerprint(), 1), scope).join());
    var initial = control.read(scope).join().orElseThrow();
    assertTrue(control.compareAndSet(initial, initial.state().claimOwner("fixture-owner", 0, Long.MAX_VALUE), scope).join());
    var invocationRepository = new HBaseInvocationRepository(records);
    var invocations = new InvocationService(invocationRepository);
    invocations.bindOrJoin(preparing, scope).join();
    invocations.transition(preparing, sealed, scope).join();
    var decisions = new HBaseCommitDecisionRepository(records);
    var outcomes = new CommitOutcomeResolver(control, decisions);
    var finalizer = new CommitFinalizationService(control, outcomes, decisions, manifests, materialization, results);
    PublicationHandoff handoff = new PublicationHandoff() {
      @Override public <T> CompletableFuture<T> transfer(InvocationBinding invocation,
          RequestScope requestScope, java.util.function.Function<RequestScope, CompletableFuture<T>> work) {
        return work.apply(requestScope);
      }
    };
    var published = new CommitPublisher(control, outcomes, verifier, handoff, finalizer)
        .publish(sealed, new CommitOwnerService.OwnerToken("fixture-owner", 1), root, scope).join();
    assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, published.state());

    var reopenedResults = new ResultCatalog(new HBaseResultRepository(records));
    var reopened = new InvocationResultResolver(new HBaseInvocationRepository(records),
        new CommitOutcomeResolver(control, new HBaseCommitDecisionRepository(records)),
        new ManifestCatalog(new HBaseManifestRepository(records)), reopenedResults);
    var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
    BudgetManager.Account account;
    try {
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
    } catch (io.etcdhbase.core.error.BackendFailure failure) {
      throw new AssertionError(failure);
    }
    try (var recovered = reopened.resolve(preparing.invocationId(), preparing.requestDigest(),
        preparing.authorizationScopeDigest(), scope, account).join()) {
      PutResult decoded = ResultMessages.decodePut(recovered.payload().buffer());
      assertEquals(logicalKey, decoded.key());
      assertTrue(decoded.previous().isEmpty());
      assertEquals(1, recovered.decision().revision());
    }
    assertEquals(InvocationBinding.State.SELECTED,
        invocationRepository.read(preparing.invocationId(), scope).join().orElseThrow().state());
    assertEquals(0, budgets.snapshot().allocatedBytes());
    account.close();
  }

  private static TableDescriptor tableDescriptor(TableName table, String group, boolean results) {
    return TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN" + (results ? ",RESULT" : ""))
        .build();
  }
}
