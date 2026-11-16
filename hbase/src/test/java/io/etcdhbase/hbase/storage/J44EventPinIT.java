package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
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
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      var config = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
      var pins = EventHistoryPinManager.initializeFresh(records, config, Clock.systemUTC(), scope).join();
      ByteKey rootIdentity = ByteKey.copyOf(new byte[] {1, 2, 3});
      ByteKey rootDigest = ByteKey.copyOf(new byte[32]);
      records.createIfAbsent(SystemRecordType.EVENT, rootIdentity, ByteKey.copyOf(new byte[] {4}), scope).join();
      var eventRoot = new EventReference(rootIdentity, rootDigest, 0, 0, 1, 1, 1, 0);
      var requirement = new EventHistoryPinManager.Requirement(5, 3, ByteKey.copyOf(new byte[32]),
          Optional.of(TransactionId.create()), Optional.of(eventRoot));
      var pin = pins.register(requirement, scope).join();
      pins.advanceRegistrationFloors(8, 6, scope).join();
      var reopened = EventHistoryPinManager.open(records, config, Clock.systemUTC(), scope).join();

      assertFalse(collectIfSafe(environment, binding, reopened, rootIdentity, 3, scope));
      assertTrue(records.read(SystemRecordType.EVENT, rootIdentity, scope).join().isPresent());
      pin.complete(scope).join();
      assertTrue(collectIfSafe(environment, binding, reopened, rootIdentity, 3, scope));
      assertTrue(records.read(SystemRecordType.EVENT, rootIdentity, scope).join().isEmpty());
    }
  }

  private static boolean collectIfSafe(M1TestEnvironment environment, SystemTableBinding binding,
      EventHistoryPinManager pins, ByteKey identity, long sequence, RequestScope scope) {
    var boundary = pins.readBoundary(scope).join();
    if (boundary.safeSequence() <= sequence || boundary.protections().stream()
        .anyMatch(protection -> protection.rootIdentity().filter(identity::equals).isPresent())) {
      return false;
    }
    environment.connection().getTable(binding.table())
        .delete(new Delete(binding.row(SystemRecordType.EVENT, identity))).join();
    return true;
  }
}
