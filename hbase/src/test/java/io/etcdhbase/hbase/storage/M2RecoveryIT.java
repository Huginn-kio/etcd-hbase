package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitDecisionRepository;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** MiniCluster evidence that a fresh recovery manager resumes a durable selected publication. */
class M2RecoveryIT {
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));
  private static final byte[] FAMILY = {'s'};

  @Test
  void freshManagerArchivesAndFinalizesSelectedMetadataOnlyPublication() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      RecoveryFixture fixture = prepareSelectedPublication(environment, scope);
      RecoveryComponents reopened = reopenRecovery(environment, fixture.binding(), scope);
      assertRecoveredPublication(fixture, reopened, scope);
    }
  }

  private static RecoveryFixture prepareSelectedPublication(
      M1TestEnvironment environment, RequestScope scope) throws Exception {
    SystemTableBinding binding = createRecoveryTable(environment);
    HBaseSystemRecordStore records = openRecords(environment, binding, scope);
    HBaseCommitControlStore control = openControl(environment, binding, scope);
    InvocationRepository invocations = new HBaseInvocationRepository(records);
    ManifestRepository manifests = new HBaseManifestRepository(records);
    assertTrue(await(control.initializeIfAbsent(
        CommitState.initial(binding.profileFingerprint(), 7), scope)));

    long recoveryNow = System.currentTimeMillis();
    long oldClaimTime = recoveryNow - 2_000;
    CommitState expiredOwner = CommitState.initial(binding.profileFingerprint(), 7)
        .claimOwner("stopped-process", oldClaimTime, oldClaimTime + 1_000);
    ControlSnapshot initialized = await(control.read(scope)).orElseThrow();
    assertTrue(await(control.compareAndSet(initialized, expiredOwner, scope)));

    var transaction = TransactionId.create();
    InvocationId invocationId = InvocationId.create();
    ManifestRoot root = ManifestRoot.create(transaction, expiredOwner.ownerEpoch(), 0, 7,
        0, 0, Optional.empty());
    InvocationBinding invocation = InvocationBinding.begin(invocationId, digest(4), digest(5),
        transaction, expiredOwner.ownerEpoch(), 0, 7).seal(root.systemIdentity());
    assertEquals(SystemRecordStore.CreateResult.CREATED,
        await(invocations.createIfAbsent(invocation, scope)));
    assertEquals(SystemRecordStore.CreateResult.CREATED, await(manifests.seal(root, scope)));

    ControlSnapshot beforePublish = await(control.read(scope)).orElseThrow();
    PublicationSlot slot = new PublicationSlot(transaction, invocationId, expiredOwner.ownerEpoch(),
        0, 7, 1, 7, false, root.systemIdentity(), root.digest());
    assertTrue(await(control.compareAndSet(beforePublish, beforePublish.state().publish(slot), scope)));
    return new RecoveryFixture(binding, slot, recoveryNow);
  }

  private static SystemTableBinding createRecoveryTable(M1TestEnvironment environment) throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    TableName table = TableName.valueOf("m2_recovery_" + suffix);
    String tableGroup = "m2-recovery-" + suffix;
    String saltProfile = "m2-recovery-salt-" + suffix;
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, tableGroup, saltProfile, SALT);
    environment.create(table, descriptor(binding, saltProfile), SALT.splitKeys().toArray(byte[][]::new));
    return binding;
  }

  private static RecoveryComponents reopenRecovery(M1TestEnvironment environment,
      SystemTableBinding binding, RequestScope scope) throws Exception {
    HBaseSystemRecordStore records = openRecords(environment, binding, scope);
    HBaseCommitControlStore control = openControl(environment, binding, scope);
    InvocationRepository invocations = new HBaseInvocationRepository(records);
    ManifestRepository manifests = new HBaseManifestRepository(records);
    CommitDecisionRepository decisions = new HBaseCommitDecisionRepository(records);
    CommitOutcomeResolver outcomes = new CommitOutcomeResolver(control, decisions);
    CommitFinalizationService finalizer = new CommitFinalizationService(
        control, outcomes, decisions, new ManifestCatalog(manifests), noCandidatesMaterialization());
    RecoveryManager recovery = new RecoveryManager(binding.profileFingerprint(), control,
        new CommitOwnerService(control), invocations, outcomes, finalizer);
    return new RecoveryComponents(control, decisions, recovery);
  }

  private static void assertRecoveredPublication(RecoveryFixture fixture,
      RecoveryComponents reopened, RequestScope scope) throws Exception {
    RecoveryManager.Result result = await(reopened.manager().recover(
        "recovery-process", fixture.recoveryNow(), fixture.recoveryNow() + 60_000, scope));
    assertEquals(RecoveryManager.State.FINALIZED, result.state());
    assertEquals(2, result.owner().orElseThrow().epoch());
    CommitState recovered = await(reopened.control().read(scope)).orElseThrow().state();
    assertEquals(1, recovered.publishedSequence());
    assertEquals(1, recovered.finalizedSequence());
    assertEquals(7, recovered.publishedRevision());
    assertEquals(7, recovered.finalizedRevision());
    assertTrue(recovered.publication().isEmpty());
    assertEquals(Optional.of(CommitDecision.from(fixture.slot())),
        await(reopened.decisions().read(1, scope)));
    System.out.println("j27-recovery durableSelectedSlot=true archiveAndFinalize=true revisionStable=true");
  }

  private record RecoveryFixture(SystemTableBinding binding, PublicationSlot slot, long recoveryNow) {}

  private record RecoveryComponents(HBaseCommitControlStore control,
      CommitDecisionRepository decisions, RecoveryManager manager) {}

  private static HBaseSystemRecordStore openRecords(M1TestEnvironment environment,
      SystemTableBinding binding, RequestScope scope) throws Exception {
    return await(HBaseSystemRecordStore.open(environment.factory(), environment.identity(), binding, scope));
  }

  private static HBaseCommitControlStore openControl(M1TestEnvironment environment,
      SystemTableBinding binding, RequestScope scope) throws Exception {
    return await(HBaseCommitControlStore.open(environment.factory(), environment.identity(), binding, scope));
  }

  private static MaterializationStore noCandidatesMaterialization() {
    return new MaterializationStore() {
      @Override
      public CompletableFuture<ApplyResult> applySelected(
          io.etcdhbase.core.commit.CommitDecision decision,
          io.etcdhbase.core.commit.PreparedCandidate candidate, RequestScope scope) {
        return CompletableFuture.failedFuture(new AssertionError("metadata-only commit has no candidate"));
      }

      @Override
      public CompletableFuture<Void> preflight(
          io.etcdhbase.core.commit.PreparedCandidate candidate, RequestScope scope) {
        return CompletableFuture.completedFuture(null);
      }
    };
  }

  private static org.apache.hadoop.hbase.client.TableDescriptor descriptor(
      SystemTableBinding binding, String saltProfile) {
    return TableDescriptorBuilder.newBuilder(binding.table())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", binding.tableGroupId())
        .setValue("etcdhbase.m0.salt-profile-id", saltProfile)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN")
        .build();
  }

  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(90, java.util.concurrent.TimeUnit.SECONDS);
  }
}
