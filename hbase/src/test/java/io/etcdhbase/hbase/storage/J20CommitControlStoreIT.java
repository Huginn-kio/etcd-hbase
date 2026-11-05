package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class J20CommitControlStoreIT {
  private static final byte[] FAMILY = {'s'};
  private static final byte[] STATE_QUALIFIER = {'s'};
  private static final String TABLE_GROUP = "j20-control";
  private static final String SALT_PROFILE = "j20-control-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void initializesAndComparesTheFullControlValueAtomicallyOnDisposableHBase() throws Exception {
    TableName table = TableName.valueOf("j20_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, FAMILY, TABLE_GROUP, SALT_PROFILE, SALT);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", TABLE_GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count",
            Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes",
            Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN")
        .build();

    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      environment.create(table, descriptor, SALT.splitKeys().toArray(byte[][]::new));
      var store = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      CommitState initial = CommitState.initial(binding.profileFingerprint(), 1);

      byte[] wrongInitialFingerprint = new byte[32];
      wrongInitialFingerprint[0] = 1;
      ExecutionException wrongInitial = assertThrows(ExecutionException.class,
          () -> M1TestEnvironment.await(store.initializeIfAbsent(
              CommitState.initial(ByteKey.copyOf(wrongInitialFingerprint), 1), scope)));
      assertTrue(wrongInitial.getCause() instanceof IllegalArgumentException);

      assertTrue(M1TestEnvironment.await(store.initializeIfAbsent(initial, scope)));
      assertFalse(M1TestEnvironment.await(store.initializeIfAbsent(initial, scope)));

      var differentSalt = new HashSaltStrategy(
          new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 32, 2));
      var differentBinding = new SystemTableBinding(
          table, FAMILY, TABLE_GROUP, SALT_PROFILE, differentSalt);
      assertFalse(java.util.Arrays.equals(binding.controlRow(), differentBinding.controlRow()));
      assertThrows(ExecutionException.class, () -> M1TestEnvironment.await(
          HBaseCommitControlStore.open(
              environment.factory(), environment.identity(), differentBinding, scope)));
      var mismatchedTableClient = environment.connection().getTableBuilder(table).build();
      assertTrue(M1TestEnvironment.await(mismatchedTableClient.get(
          new Get(differentBinding.controlRow()).addFamily(FAMILY))).isEmpty());

      var firstSnapshot = M1TestEnvironment.await(store.read(scope)).orElseThrow();
      CommitState claimed = firstSnapshot.state().claimOwner("owner-a", 10, 20);
      assertTrue(M1TestEnvironment.await(store.compareAndSet(firstSnapshot, claimed, scope)));
      assertFalse(M1TestEnvironment.await(store.compareAndSet(firstSnapshot, claimed, scope)),
          "a token captured before the successful write must no longer match");

      var raceSnapshot = M1TestEnvironment.await(store.read(scope)).orElseThrow();
      CommitState renewalA = raceSnapshot.state().heartbeat("owner-a", 1, 30);
      CommitState renewalB = raceSnapshot.state().heartbeat("owner-a", 1, 40);
      var raceA = store.compareAndSet(raceSnapshot, renewalA, scope);
      var raceB = store.compareAndSet(raceSnapshot, renewalB, scope);
      boolean wonA = M1TestEnvironment.await(raceA);
      boolean wonB = M1TestEnvironment.await(raceB);
      assertNotEquals(wonA, wonB, "exactly one concurrent update may match the observed bytes");

      var current = M1TestEnvironment.await(store.read(scope)).orElseThrow().state();
      assertEquals(raceSnapshot.state().stateVersion() + 1, current.stateVersion());
      assertTrue(current.ownerLeaseDeadlineEpochMillis() == 30
          || current.ownerLeaseDeadlineEpochMillis() == 40);

      byte[] wrongFingerprint = new byte[32];
      wrongFingerprint[0] = 1;
      byte[] encodedWrongProfile = CommitControlMessages.encode(
          CommitState.initial(ByteKey.copyOf(wrongFingerprint), 1));
      var tableClient = environment.connection().getTableBuilder(table).build();
      M1TestEnvironment.await(tableClient.put(new Put(binding.controlRow())
          .setDurability(Durability.SYNC_WAL)
          .addColumn(FAMILY, STATE_QUALIFIER, encodedWrongProfile)));

      ExecutionException failure = assertThrows(ExecutionException.class,
          () -> M1TestEnvironment.await(store.read(scope)));
      Throwable root = failure;
      while (root.getCause() != null && root.getCause() != root) root = root.getCause();
      assertTrue(root instanceof IllegalArgumentException);
      assertTrue(root.getMessage().contains("profile differs"));
    }
  }
}
