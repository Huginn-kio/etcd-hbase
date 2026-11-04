package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class J21OwnerInvocationStoreIT {
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "j21-owner-invocation";
  private static final String SALT_PROFILE = "j21-owner-invocation-salt";

  @Test
  void ownerTakeoverAndInvocationBindingUseDurableHBaseCas() throws Exception {
    var salt = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 32, 2));
    TableName table = TableName.valueOf("j21_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, FAMILY, TABLE_GROUP, SALT_PROFILE, salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", TABLE_GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count",
            Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes",
            Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION")
        .build();

    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
      var control = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      assertTrue(M1TestEnvironment.await(control.initializeIfAbsent(
          CommitState.initial(binding.profileFingerprint(), 1), scope)));

      var owners = new CommitOwnerService(control);
      var firstOwner = M1TestEnvironment.await(owners.claim("owner-a", 10, 20, scope))
          .orElseThrow();
      var replacementOwner = M1TestEnvironment.await(owners.claim("owner-b", 20, 30, scope))
          .orElseThrow();
      assertEquals(firstOwner.epoch() + 1, replacementOwner.epoch());
      assertFalse(M1TestEnvironment.await(owners.heartbeat(firstOwner, 40, scope)),
          "a process from the expired owner epoch must not renew the replacement lease");

      var records = M1TestEnvironment.await(HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope));
      var repository = new HBaseInvocationRepository(records);
      var invocations = new InvocationService(repository);
      InvocationId invocationId = InvocationId.create();
      ByteKey requestDigest = digest(1);
      ByteKey authDigest = digest(2);
      InvocationBinding proposalA = InvocationBinding.begin(invocationId, requestDigest,
          authDigest, TransactionId.create(), replacementOwner.epoch(), 0, 1);
      InvocationBinding proposalB = InvocationBinding.begin(invocationId, requestDigest,
          authDigest, TransactionId.create(), replacementOwner.epoch(), 0, 1);

      var gate = new CyclicBarrier(2);
      var failure = new AtomicReference<Throwable>();
      CompletableFuture<InvocationBinding> resultA = bindAfter(gate, invocations, proposalA,
          scope, failure);
      CompletableFuture<InvocationBinding> resultB = bindAfter(gate, invocations, proposalB,
          scope, failure);
      InvocationBinding winnerA = M1TestEnvironment.await(resultA);
      InvocationBinding winnerB = M1TestEnvironment.await(resultB);
      assertTrue(failure.get() == null, () -> "concurrent bind failed: " + failure.get());
      assertEquals(winnerA.transactionId(), winnerB.transactionId());
      assertSameBinding(winnerA, M1TestEnvironment.await(repository.read(invocationId, scope))
          .orElseThrow());

      InvocationBinding sealed = winnerA.seal(digest(3));
      InvocationBinding persistedSeal = M1TestEnvironment.await(
          invocations.transition(winnerA, sealed, scope));
      assertEquals(InvocationBinding.State.SEALED, persistedSeal.state());
      assertFalse(M1TestEnvironment.await(repository.compareAndSet(
          winnerA, sealed, scope)), "a stale PREPARING byte value must not overwrite SEALED");
      assertSameBinding(sealed, M1TestEnvironment.await(repository.read(invocationId, scope))
          .orElseThrow());
    }
  }

  private static CompletableFuture<InvocationBinding> bindAfter(
      CyclicBarrier gate, InvocationService service, InvocationBinding proposal,
      RequestScope scope, AtomicReference<Throwable> failure) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        gate.await(10, TimeUnit.SECONDS);
        return M1TestEnvironment.await(service.bindOrJoin(proposal, scope));
      } catch (Exception problem) {
        failure.compareAndSet(null, problem);
        throw new RuntimeException(problem);
      }
    });
  }

  private static ByteKey digest(int marker) {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) marker;
    return ByteKey.copyOf(bytes);
  }

  private static void assertSameBinding(InvocationBinding expected, InvocationBinding actual) {
    assertTrue(expected.sameAttempt(actual));
    assertEquals(expected.state(), actual.state());
    assertEquals(expected.manifestRoot(), actual.manifestRoot());
    assertEquals(expected.resultReference(), actual.resultReference());
  }
}
