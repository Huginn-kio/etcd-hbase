package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real gRPC and disposable HBase coverage for Range before the complete Go comparison. */
@Timeout(600)
class M3RangeRpcIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);

  @Test
  void standaloneAndTransactionRangeUseTheSameProtectedViewOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        verifyLayout(cluster, file);
      }
    }
  }

  private static void verifyLayout(TemporaryHBaseClusterProcess cluster, String file) throws Exception {
    var plan = M3TableGroupRuntimeIT.plan(file,
        cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
    var auth = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), auth,
             cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
             (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
             1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-range-fixture", new char[0])) {
      var identity = provider.authenticate(credentials, auth).toCompletableFuture().get(30, TimeUnit.SECONDS);
      verifyAuthenticatedLayout(plan, provider, identity);
    }
  }

  private static void verifyAuthenticatedLayout(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity) throws Exception {
    M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
    var limit = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
    var budgets = new BudgetManager(limit, Map.of(plan.source().tableGroupId(), limit), limit);
    try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
      M3TableGroupRuntime runtime;
      try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        runtime = M3TableGroupRuntime.open(plan, M3TableGroupRuntimeIT.activation("1"),
            RESOURCES, provider, identity, budgets, scans, startup);
      }
      try {
        verifyServer(plan, runtime, provider, identity);
      } finally {
        runtime.close();
        runtime.drained().get(30, TimeUnit.SECONDS);
      }
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
    assertEquals(0, budgets.snapshot().allocatedBytes());
  }

  private static void verifyServer(M3TableGroupPlan plan, M3TableGroupRuntime runtime,
      HBaseBackendIdentityProvider provider, AuthorizedBackendIdentity identity) throws Exception {
    try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
        "m3-range", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
      var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port()).usePlaintext().build();
      try {
        var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(90, TimeUnit.SECONDS);
        verifyRange(stub, runtime, plan.primary().codec()::bucket);
        verifyDescending(stub);
        try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
          var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope)
              .get(30, TimeUnit.SECONDS);
          verifyCompactedRange(stub, control, scope);
        }
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  private static void verifyRange(KVGrpc.KVBlockingStub stub, M3TableGroupRuntime runtime,
      java.util.function.ToIntFunction<ByteKey> bucket) {
    ByteString a = ByteString.copyFromUtf8("j34/a");
    ByteString last = ByteString.copyFrom(new byte[] {'j', '3', '4', '/', (byte) 0xff});
    ByteString prefix = ByteString.copyFromUtf8("j34/");
    ByteString end = ByteString.copyFromUtf8("j340");
    assertTrue(bucket.applyAsInt(ByteKey.copyOf(a.toByteArray()))
        != bucket.applyAsInt(ByteKey.copyOf(last.toByteArray())));
    assertEquals(2, stub.put(put(a, "old")).getHeader().getRevision());
    assertEquals(3, stub.put(put(last, "other")).getHeader().getRevision());
    assertEquals(4, stub.put(put(a, "new")).getHeader().getRevision());
    verifyCurrentHistoricalAndFilteredRanges(stub, runtime, a, prefix, end);
    verifyRangeErrorsAndUnboundedFilter(stub, a, prefix, end);
    verifyTransactionAndBoundaryRanges(stub, a, prefix, last);
  }

  private static void verifyCurrentHistoricalAndFilteredRanges(KVGrpc.KVBlockingStub stub,
      M3TableGroupRuntime runtime, ByteString a, ByteString prefix, ByteString end) {
    var beforeStages = runtime.metrics().txnStages();
    long beforeTxnBytes = runtime.metrics().txnOutputBytes();
    var page = stub.range(Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(end).setLimit(1).build());
    assertEquals(1, page.getCount());
    assertTrue(page.getMore());
    assertEquals(a, page.getKvs(0).getKey());
    assertEquals(runtime.clusterId(), page.getHeader().getClusterId());
    assertEquals(runtime.memberId(), page.getHeader().getMemberId());
    assertEquals(4, page.getHeader().getRevision());
    var historical = stub.range(Rpc.RangeRequest.newBuilder().setKey(a).setRevision(2).build());
    assertEquals(ByteString.copyFromUtf8("old"), historical.getKvs(0).getValue());
    assertEquals(4, historical.getHeader().getRevision());
    var keys = stub.range(Rpc.RangeRequest.newBuilder().setKey(a).setKeysOnly(true).build());
    assertTrue(keys.getKvs(0).getValue().isEmpty());
    assertEquals(4, keys.getKvs(0).getModRevision());
    var exists = stub.range(Rpc.RangeRequest.newBuilder().setKey(a).setCountOnly(true)
        .setMaxModRevision(-1).build());
    assertEquals(1, exists.getCount());
    assertEquals(0, exists.getKvsCount());
    var negative = stub.range(Rpc.RangeRequest.newBuilder().setKey(a).setRevision(-2).build());
    assertEquals(ByteString.copyFromUtf8("new"), negative.getKvs(0).getValue());
    var filtered = stub.range(Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(end)
        .setLimit(1).setMinModRevision(4).build());
    assertEquals(1, filtered.getCount());
    assertFalse(filtered.getMore());
    assertEquals(a, filtered.getKvs(0).getKey());
    assertEquals(beforeStages, runtime.metrics().txnStages());
    assertEquals(beforeTxnBytes, runtime.metrics().txnOutputBytes());
  }

  private static void verifyRangeErrorsAndUnboundedFilter(KVGrpc.KVBlockingStub stub,
      ByteString a, ByteString prefix, ByteString end) {
    var fullFiltered = stub.range(Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(end)
        .setMinModRevision(4).build());
    assertEquals(1, fullFiltered.getCount());
    assertEquals(a, fullFiltered.getKvs(0).getKey());
    assertFalse(fullFiltered.getMore());
    var excludedPoint = stub.range(Rpc.RangeRequest.newBuilder().setKey(a).setLimit(1)
        .setMinModRevision(5).build());
    assertEquals(0, excludedPoint.getCount());
    assertEquals(0, excludedPoint.getKvsCount());
    var unsupportedCount = Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(end).setCountOnly(true).build();
    assertEquals(Status.Code.UNIMPLEMENTED, status(() -> stub.range(unsupportedCount)));
    var future = Rpc.RangeRequest.newBuilder().setKey(a).setRevision(5).build();
    assertEquals(Status.Code.OUT_OF_RANGE, status(() -> stub.range(future)));
    var nestedNegative = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(
        Rpc.RangeRequest.newBuilder().setKey(a).setRevision(-2))).build();
    assertEquals(Status.Code.OUT_OF_RANGE, status(() -> stub.txn(nestedNegative)));
  }

  private static void verifyTransactionAndBoundaryRanges(KVGrpc.KVBlockingStub stub,
      ByteString a, ByteString prefix, ByteString last) {
    ByteString c = ByteString.copyFromUtf8("j34/c");
    var txn = Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(put(c, "txn")))
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder().setKey(c))).build();
    var written = stub.txn(txn);
    assertEquals(ByteString.copyFromUtf8("txn"), written.getResponses(1).getResponseRange().getKvs(0).getValue());
    assertEquals(written.getHeader().getRevision(),
        written.getResponses(1).getResponseRange().getHeader().getRevision());

    ByteString zero = ByteString.copyFrom(new byte[] {0});
    var fromKey = stub.range(Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(zero).build());
    assertEquals(List.of(a, c, last), fromKey.getKvsList().stream().map(kv -> kv.getKey()).toList());
    var all = stub.range(Rpc.RangeRequest.newBuilder().setKey(zero).setRangeEnd(zero).build());
    assertEquals(List.of(a, c, last), all.getKvsList().stream().map(kv -> kv.getKey()).toList());
    var empty = stub.range(Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(prefix).build());
    assertEquals(0, empty.getCount());
    assertEquals(0, empty.getKvsCount());
  }

  private static void verifyDescending(KVGrpc.KVBlockingStub stub) {
    var prefix = ByteString.copyFromUtf8("j34/");
    var end = ByteString.copyFromUtf8("j340");
    var a = ByteString.copyFromUtf8("j34/a");
    var c = ByteString.copyFromUtf8("j34/c");
    var last = ByteString.copyFrom(new byte[] {'j', '3', '4', '/', (byte) 0xff});
    var firstRequest = Rpc.RangeRequest.newBuilder().setKey(prefix).setRangeEnd(end).setRevision(5)
        .setSortOrderValue(2).setLimit(2).build();
    var first = stub.range(firstRequest);
    assertEquals(List.of(last, c), first.getKvsList().stream().map(kv -> kv.getKey()).toList());
    assertEquals(2, first.getCount());
    assertTrue(first.getMore());
    var second = stub.range(firstRequest.toBuilder().setRangeEnd(c).build());
    assertEquals(List.of(a), second.getKvsList().stream().map(kv -> kv.getKey()).toList());
    assertFalse(second.getMore());
    var filtered = stub.range(firstRequest.toBuilder().setMinModRevision(4).setLimit(1).build());
    assertEquals(List.of(c), filtered.getKvsList().stream().map(kv -> kv.getKey()).toList());
    assertTrue(filtered.getMore());
    for (int target = 1; target <= 4; target++) {
      var unsupported = firstRequest.toBuilder().setSortTargetValue(target).build();
      assertEquals(Status.Code.UNIMPLEMENTED, status(() -> stub.range(unsupported)));
    }
  }

  private static void verifyCompactedRange(KVGrpc.KVBlockingStub stub, HBaseCommitControlStore control,
      RequestScope scope) throws Exception {
    // Establish the compact boundary directly in the disposable fixture; Compact RPC is not implemented here.
    boolean advanced = false;
    for (int attempt = 0; attempt < 5 && !advanced; attempt++) {
      var snapshot = control.read(scope).get(30, TimeUnit.SECONDS).orElseThrow();
      advanced = control.compareAndSet(snapshot, snapshot.state().advanceCompactRevision(3), scope)
          .get(30, TimeUnit.SECONDS);
    }
    assertTrue(advanced, "the fixture must install the compact boundary");
    var key = ByteString.copyFromUtf8("j34/a");
    var boundary = stub.range(Rpc.RangeRequest.newBuilder().setKey(key).setRevision(3).build());
    assertEquals(ByteString.copyFromUtf8("old"), boundary.getKvs(0).getValue());
    var request = Rpc.RangeRequest.newBuilder().setKey(key).setRevision(2).build();
    var error = assertThrows(StatusRuntimeException.class, () -> stub.range(request));
    assertEquals(Status.Code.OUT_OF_RANGE, error.getStatus().getCode());
    assertEquals("etcdserver: mvcc: required revision has been compacted", error.getStatus().getDescription());
    var txn = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(request)).build();
    var txnError = assertThrows(StatusRuntimeException.class, () -> stub.txn(txn));
    assertEquals(Status.Code.OUT_OF_RANGE, txnError.getStatus().getCode());
    assertEquals(error.getStatus().getDescription(), txnError.getStatus().getDescription());
  }

  private static Rpc.PutRequest put(ByteString key, String value) {
    return Rpc.PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8(value)).build();
  }

  private static Status.Code status(Runnable call) {
    return Status.fromThrowable(assertThrows(StatusRuntimeException.class, call::run)).getCode();
  }
}
