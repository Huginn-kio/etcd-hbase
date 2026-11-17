package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventHistoryRegistryUpgrade;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class J44EventIndexControlStoreIT {
  @Test
  void requiresDescriptorAndReopenedStoreForFencedControlUpgrade() throws Exception {
    byte[] family = {'s'};
    var salt = new HashSaltStrategy();
    var table = TableName.valueOf("j44_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, family, "j44-control", "j44-salt", salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", "j44-control")
        .setValue("etcdhbase.m0.salt-profile-id", "j44-salt")
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
    descriptor = binding.withLeaseExpiryCheckpointCapability(
        binding.withLeaseBindingCapability(binding.withAuxiliaryCapability(descriptor)));

    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
      var oldStore = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      assertFalse(oldStore.supportsEventRevisionIndex());
      assertTrue(M1TestEnvironment.await(oldStore.initializeIfAbsent(
          CommitState.initial(binding.profileFingerprint(), 1), scope)));
      var initial = M1TestEnvironment.await(oldStore.read(scope)).orElseThrow();
      var sizedInitial = M1TestEnvironment.await(oldStore.readWithSize(scope));
      var measuredInitial = sizedInitial.value().orElseThrow();
      assertArrayEquals(initial.comparisonToken(), measuredInitial.comparisonToken());
      assertEquals((long) binding.controlRow().length + initial.comparisonToken().length,
          sizedInitial.encodedBytes());
      var claimed = initial.state().claimOwner("owner", 10, 100);
      assertTrue(M1TestEnvironment.await(oldStore.compareAndSet(initial, claimed, scope)));
      var snapshot = M1TestEnvironment.await(oldStore.read(scope)).orElseThrow();
      var auxiliary = snapshot.state().upgradeAuxiliaryFormat("owner", snapshot.state().ownerEpoch(), 11);
      assertTrue(M1TestEnvironment.await(oldStore.compareAndSet(snapshot, auxiliary, scope)));
      snapshot = M1TestEnvironment.await(oldStore.read(scope)).orElseThrow();
      var bindings = snapshot.state().upgradeLeaseBindingFormat("owner", snapshot.state().ownerEpoch(), 12);
      assertTrue(M1TestEnvironment.await(oldStore.compareAndSet(snapshot, bindings, scope)));
      snapshot = M1TestEnvironment.await(oldStore.read(scope)).orElseThrow();
      var expiry = snapshot.state().upgradeLeaseExpiryFormat("owner", snapshot.state().ownerEpoch(), 13);
      assertTrue(M1TestEnvironment.await(oldStore.compareAndSet(snapshot, expiry, scope)));

      var beforeUpgrade = M1TestEnvironment.await(oldStore.read(scope)).orElseThrow();
      var eventIndex = beforeUpgrade.state().upgradeEventIndexFormat("owner",
          beforeUpgrade.state().ownerEpoch(), 14);
      assertThrows(ExecutionException.class,
          () -> M1TestEnvironment.await(oldStore.compareAndSet(beforeUpgrade, eventIndex, scope)));
      environment.connection().getAdmin().modifyTable(binding.withEventRevisionIndexCapability(descriptor)).join();
      assertThrows(ExecutionException.class,
          () -> M1TestEnvironment.await(oldStore.compareAndSet(beforeUpgrade, eventIndex, scope)));
      var newStore = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      assertTrue(newStore.supportsEventRevisionIndex());
      assertTrue(M1TestEnvironment.await(newStore.compareAndSet(beforeUpgrade, eventIndex, scope)));
      assertThrows(ExecutionException.class, () -> M1TestEnvironment.await(oldStore.read(scope)));
      assertTrue(M1TestEnvironment.await(newStore.read(scope)).orElseThrow()
          .state().supportsEventRevisionIndex());
      assertFalse(M1TestEnvironment.await(oldStore.compareAndSet(beforeUpgrade,
          beforeUpgrade.state().heartbeat("owner", beforeUpgrade.state().ownerEpoch(), 200), scope)));

      var records = M1TestEnvironment.await(HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope));
      var pinConfiguration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
      var clock = Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC);
      var selected = M1TestEnvironment.await(new EventHistoryRegistryUpgrade(
          records, newStore, pinConfiguration, clock)
          .upgrade(new CommitOwnerService.OwnerToken("owner", eventIndex.ownerEpoch()), scope));
      var selectedState = M1TestEnvironment.await(newStore.read(scope)).orElseThrow().state();
      assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, selectedState.formatVersion());
      assertEquals(selected.owner().epoch(), selectedState.ownerEpoch());
      var reopened = M1TestEnvironment.await(EventHistoryPinManager.openSelected(
          records, newStore, pinConfiguration, clock, scope));
      assertEquals(selected.registry().registryIdentity(), reopened.registryIdentity());

      var first = new EventIndexBackfillCheckpoint(1, 8, 2, 7, 2, 10, 10, 11, 4, ByteKey.copyOf(new byte[16]));
      var second = new EventIndexBackfillCheckpoint(1, 9, 2, 7, 3, 10, 10, 11, 4, ByteKey.copyOf(new byte[16]));
      var type = SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT;
      records.createIfAbsent(type, EventIndexBackfillCheckpoint.versionIdentity(8), first.encode(), scope).join();
      records.createIfAbsent(type, EventIndexBackfillCheckpoint.versionIdentity(9), second.encode(), scope).join();
      var prefix = EventIndexBackfillCheckpoint.routePrefix();
      assertTrue(records.latestAtOrBefore(type, prefix, 7, scope).join().isEmpty());
      assertEquals(first, EventIndexBackfillCheckpoint.decode(
          records.latestAtOrBefore(type, prefix, 8, scope).join().orElseThrow().value()));
      assertEquals(second, EventIndexBackfillCheckpoint.decode(
          records.latestAtOrBefore(type, prefix, 12, scope).join().orElseThrow().value()));
    }
  }
}
