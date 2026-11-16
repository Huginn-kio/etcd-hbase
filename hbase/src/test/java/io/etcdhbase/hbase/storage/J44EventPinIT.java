package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventHistoryRetention;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** Exercises real durable pin rows and a controlled physical event-record collector. */
class J44EventPinIT {
  @Test
  void eventPinSurvivesReopenAndStopsPhysicalCollectionUntilReleased() throws Exception {
    byte[] family = {'s'};
    var salt = new HashSaltStrategy();
    var table = TableName.valueOf("j44_pin_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, family, "j44-pin", "j44-salt", salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", "j44-pin")
        .setValue("etcdhbase.m0.salt-profile-id", "j44-salt")
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
    descriptor = binding.withEventRevisionIndexCapability(binding.withLeaseExpiryCheckpointCapability(
        binding.withLeaseBindingCapability(binding.withAuxiliaryCapability(descriptor))));
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      var config = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
      var pins = EventHistoryPinManager.initializeFresh(records, config, Clock.systemUTC(), scope).join();
      var snapshotPins = SnapshotPinManager.initializeFresh(records, config, scope).join();
      var state = new AtomicReference<>(controlState(binding, 5));
      var manifests = new ManifestCatalog((ManifestRepository) Proxy.newProxyInstance(
          ManifestRepository.class.getClassLoader(), new Class<?>[] {ManifestRepository.class},
          (proxy, method, arguments) -> { throw new UnsupportedOperationException(method.getName()); }));
      var control = (CommitControlStore) Proxy.newProxyInstance(
          CommitControlStore.class.getClassLoader(), new Class<?>[] {CommitControlStore.class},
          (proxy, method, arguments) -> {
            if (method.getName().equals("read")) {
              return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state.get(), new byte[] {1})));
            }
            throw new UnsupportedOperationException(method.getName());
          });
      var views = new ReadViewService(control, manifests, snapshotPins, binding.profileFingerprint());
      var retention = new EventHistoryRetention(pins, views, new EventIndexBackfillReader(records, manifests));
      ByteKey rootIdentity = ByteKey.copyOf(new byte[] {1, 2, 3});
      ByteKey rootDigest = ByteKey.copyOf(new byte[32]);
      records.createIfAbsent(SystemRecordType.EVENT, rootIdentity, ByteKey.copyOf(new byte[] {4}), scope).join();
      var eventRoot = new EventReference(rootIdentity, rootDigest, 0, 0, 1, 1, 1, 0);
      var requirement = new EventHistoryPinManager.Requirement(5, 3, ByteKey.copyOf(new byte[32]),
          Optional.of(TransactionId.create()), Optional.of(eventRoot));
      var pin = pins.register(requirement, scope).join();
      assertFalse(collectIfSafe(environment, binding, retention, rootIdentity, 3));
      var checkpoint = new EventIndexBackfillCheckpoint(1, 5, 3, 4, 3, 4, 4, 5, 0,
          pins.registryIdentity());
      records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
          EventIndexBackfillCheckpoint.versionIdentity(5), checkpoint.encode(), scope).join();
      var reopened = EventHistoryPinManager.open(records, config, Clock.systemUTC(), scope).join();
      retention = new EventHistoryRetention(reopened, views, new EventIndexBackfillReader(records, manifests));

      assertFalse(collectIfSafe(environment, binding, retention, rootIdentity, 3));
      assertTrue(records.read(SystemRecordType.EVENT, rootIdentity, scope).join().isPresent());
      pin.complete(scope).join();
      assertFalse(collectIfSafe(environment, binding, retention, rootIdentity, 3));
      var complete = new EventIndexBackfillCheckpoint(1, 6, 3, 4, 5, 4, 5, 5, 0,
          pins.registryIdentity());
      records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
          EventIndexBackfillCheckpoint.versionIdentity(6), complete.encode(), scope).join();
      state.set(controlState(binding, 6));
      assertTrue(collectIfSafe(environment, binding, retention, rootIdentity, 3));
      assertTrue(records.read(SystemRecordType.EVENT, rootIdentity, scope).join().isEmpty());
    }
  }

  private static CommitState controlState(SystemTableBinding binding, long sequence) {
    return CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, binding.profileFingerprint(),
        Optional.of("owner"), 1, 100, sequence, sequence, 5, sequence, 5, Optional.empty(), 0);
  }

  private static boolean collectIfSafe(M1TestEnvironment environment, SystemTableBinding binding,
      EventHistoryRetention retention, ByteKey identity, long sequence) throws Exception {
    var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
    var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(20));
    try (scope;
         BudgetManager.Account account = budgets.admit("public", scope,
             new BudgetManager.Reservation(8192, 32768, 128L << 20, 2L << 20, 4L << 20),
             new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000))) {
      var boundary = retention.advance(8, 6, ByteKey.copyOf(new byte[32]), scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 100, 8L << 30)).join();
      if (boundary.safeSequence() <= sequence || boundary.protections().stream()
          .anyMatch(protection -> protection.rootIdentity().filter(identity::equals).isPresent())) {
        return false;
      }
      environment.connection().getTable(binding.table())
          .delete(new Delete(binding.row(SystemRecordType.EVENT, identity))).join();
      return true;
    } finally {
      scope.cleanupCompletion().toCompletableFuture().join();
    }
  }
}
