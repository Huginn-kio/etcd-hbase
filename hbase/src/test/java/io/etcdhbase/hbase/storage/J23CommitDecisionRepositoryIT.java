package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class J23CommitDecisionRepositoryIT {
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "j23-decisions";
  private static final String SALT_PROFILE = "j23-decisions-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void persistsDecisionRoundTripAndRejectsDifferentDecisionAtSameSequence() throws Exception {
    TableName table = TableName.valueOf("j23_" + UUID.randomUUID().toString().replace("-", ""));
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
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, descriptor, SALT.splitKeys().toArray(byte[][]::new));
      var records = M1TestEnvironment.await(HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope));
      var repository = new HBaseCommitDecisionRepository(records);
      CommitDecision decision = decision(1, 1);

      assertEquals(SystemRecordStore.CreateResult.CREATED,
          M1TestEnvironment.await(repository.createIfAbsent(decision, scope)));
      assertEquals(SystemRecordStore.CreateResult.ALREADY_PRESENT,
          M1TestEnvironment.await(repository.createIfAbsent(decision, scope)));
      assertEquals(decision,
          M1TestEnvironment.await(repository.read(decision.sequence(), scope)).orElseThrow());

      CommitDecision conflict = new CommitDecision(TransactionId.create(), InvocationId.create(),
          decision.sequence(), decision.baseRevision(), decision.revision(),
          decision.manifestRoot(), decision.manifestDigest(), decision.containsKeyChanges());
      assertThrows(ExecutionException.class,
          () -> M1TestEnvironment.await(repository.createIfAbsent(conflict, scope)));
      assertEquals(decision,
          M1TestEnvironment.await(repository.read(decision.sequence(), scope)).orElseThrow());
    }
  }

  private static CommitDecision decision(long sequence, long baseRevision) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), sequence,
        baseRevision, baseRevision + 1, ByteKey.copyOf(new byte[] {1, 2, 3}), digest(), true);
  }

  private static ByteKey digest() {
    return ByteKey.copyOf(new byte[32]);
  }
}
