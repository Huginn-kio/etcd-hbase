package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import etcdserverpb.KVGrpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.CompareCondition;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.grpc.protocol.TxnResponseEmitter;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.BoundedPublicationHandoff;
import io.etcdhbase.server.commit.MutationForwardReceiver;
import io.etcdhbase.server.commit.MutationOwnerLane;
import io.etcdhbase.server.commit.MutationRelay;
import io.etcdhbase.server.commit.SerialMutationQueue;
import io.etcdhbase.server.commit.TableGroupStores;
import io.etcdhbase.server.commit.TxnCommitPreparation;
import io.etcdhbase.server.commit.TxnExecutionLimits;
import io.etcdhbase.server.commit.TxnMutationExecutor;
import io.etcdhbase.server.commit.TxnOwnerProcessor;
import io.etcdhbase.server.commit.TxnReadExecutor;
import io.etcdhbase.server.commit.TxnGateway;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Real HBase evidence for flat transaction publication and the durable no-change result. */
class M3TxnCommitIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(3);
  private static final ByteKey AUTHORIZATION = digest(9);

  @Test
  void publishesMultipleKeysOnceAndReplaysAnEarlierNoChangeResultOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment environment = M3TestEnvironment.start(layout, 1);
           Engine engine = new Engine(environment)) {
        var firstCommand = new TxnCommand(List.of(), List.of(put("a", "one"), put("b", "two")), List.of());
        Receipt first = engine.run(firstCommand, 1);
        assertTrue(first.changed());
        assertEquals(2, first.revision());
        engine.assertValue("a", 2, "one");
        engine.assertValue("b", 2, "two");
        engine.assertResolvedResponse(first, firstCommand, true);
        engine.assertNettyCancellationWaitsForSerialization(first, firstCommand);

        var noChangeCommand = new TxnCommand(List.of(CompareCondition.numeric(
            KeyRange.point(key("a")), CompareCondition.Target.VERSION,
            CompareCondition.Relation.EQUAL, 0)), List.of(put("c", "unused")), List.of());
        Receipt unchanged = engine.run(noChangeCommand, 2);
        assertFalse(unchanged.changed());
        assertEquals(2, unchanged.revision());
        assertEquals(2, engine.currentRevision());
        assertEquals(1, engine.currentSequence());

        Receipt later = engine.run(new TxnCommand(List.of(), List.of(put("a", "new")), List.of()), 3);
        assertTrue(later.changed());
        assertEquals(3, later.revision());
        ResultNode.Txn replayed = engine.replayNoChanges(unchanged);
        assertFalse(replayed.succeeded());
        assertEquals(2, replayed.header().revision());
        assertTrue(replayed.responses().isEmpty());
        engine.assertResolvedResponse(unchanged, noChangeCommand, false);
        assertEquals(3, engine.currentRevision());
        assertEquals(2, engine.currentSequence());

        Receipt ownerWrite = engine.runViaExecutor(new TxnCommand(List.of(), List.of(put("d", "owner")),
            List.of()), 4);
        assertTrue(ownerWrite.changed());
        assertEquals(4, ownerWrite.revision());
        engine.assertValue("d", 4, "owner");
        Receipt ownerNoChange = engine.runViaExecutor(new TxnCommand(List.of(CompareCondition.numeric(
            KeyRange.point(key("d")), CompareCondition.Target.VERSION,
            CompareCondition.Relation.EQUAL, 0)), List.of(put("e", "unused")), List.of()), 5);
        assertFalse(ownerNoChange.changed());
        assertEquals(4, engine.replayNoChanges(ownerNoChange).header().revision());
        engine.assertFinalizationCapacityIsReservedBeforePreparing();
        engine.assertTaggedOwnerRequest();
        engine.assertValue("tagged", 5, "value");
        engine.assertReadOnlyTransaction();
        engine.assertLostOwnerRepliesAndNoChangeTakeover();
      }
    }
  }

  private record Receipt(InvocationId id, ByteKey requestDigest, long revision, boolean changed) {}

  private static final class Engine implements AutoCloseable {
    private final M3TestEnvironment environment;
    private final TableGroupStores stores;
    private final BudgetManager budgets;
    private final ScanScheduler scans;
    private final CommitOwnerService.OwnerToken owner;
    private final InvocationService invocations;
    private final TxnCommitPreparation preparation;
    private final BoundedPublicationHandoff handoff = new BoundedPublicationHandoff(1, 2, TIMEOUT);
    private final CommitPublisher publisher;
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(16), Thread.ofPlatform().daemon().name("m3-txn-test-", 0).factory());
    private final TxnMutationExecutor mutations;
    private final TxnOwnerProcessor processor;
    private final TxnReadExecutor readOnly;

    private Engine(M3TestEnvironment environment) throws Exception {
      this.environment = environment;
      var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 256L << 20);
      budgets = new BudgetManager(limits, Map.of(environment.system().tableGroupId(), limits), limits);
      scans = new ScanScheduler(budgets, Duration.ofSeconds(30));
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        stores = TableGroupStores.open(environment.factory(), environment.identity(), environment.system(),
            environment.primary(), environment.current(),
            new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60)), scope).join();
        long now = System.currentTimeMillis();
        owner = new CommitOwnerService(stores.control()).claim("m3-txn-test-owner", now,
            now + Duration.ofHours(1).toMillis(), scope).join().orElseThrow();
      }
      invocations = new InvocationService(stores.invocations());
      preparation = new TxnCommitPreparation(new ResultPreparation(stores.resultRepository()),
          stores.results(), new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(128, 4L << 20, 128, 8L << 20)), invocations, 8L << 20);
      publisher = new CommitPublisher(stores.control(), stores.outcomes(),
          new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results()),
          stores.invocations(), handoff, stores.finalizer());
      mutations = new TxnMutationExecutor(environment.system().tableGroupId(),
          environment.system().profileFingerprint(), stores.control(), stores.views(), stores.manifests(),
          environment.factory(), environment.primary(), worker, scans, preparation, publisher,
          new TxnExecutionLimits(4096, 1L << 20, 8L << 20, 4096, 64));
      processor = new TxnOwnerProcessor(environment.system().tableGroupId(), owner, budgets,
          reservation(), mutations, stores.outcomes(), 1_572_864);
      readOnly = new TxnReadExecutor(environment.system().tableGroupId(), stores.views(), stores.manifests(),
          environment.factory(), environment.primary(), worker, scans,
          new TxnExecutionLimits(4096, 1L << 20, 8L << 20, 4096, 64));
    }

    private Receipt run(TxnCommand command, int requestNumber) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        ByteKey requestDigest = digest(requestNumber);
        var proposed = InvocationBinding.begin(id, requestDigest, AUTHORIZATION, TransactionId.create(),
            owner.epoch(), control.publishedSequence(), control.publishedRevision());
        var binding = invocations.bindOrJoin(proposed, scope).join();
        var physical = environment.openPrimary(scope);
        var session = TxnReadSession.open(stores.views(), List.of(), AUTHORIZATION, physical,
            stores.manifests(), environment.primary().codec()::bucket, 1, scope).join();
        var comparisons = new CompareReadExecutor(session, scans, account, scope,
            new CompareReadExecutor.Limits(1, 4096, 64, 4096));
        var evaluator = new TxnEvaluator(session, comparisons, account, scope,
            new TxnEvaluator.Limits(4096, 1L << 20, 8L << 20), environment.primary().codec()::bucket);
        var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
        var ready = preparation.prepare(evaluated, binding, scope, account).join();
        if (ready instanceof TxnCommitPreparation.Sealed sealed) {
          var outcome = publisher.publish(sealed.invocation(), owner, sealed.manifest(), scope).join();
          assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, outcome.state());
          return new Receipt(id, requestDigest, outcome.decision().orElseThrow().revision(), true);
        }
        var noChanges = assertInstanceOf(TxnCommitPreparation.NoChanges.class, ready);
        assertEquals(InvocationBinding.State.NO_CHANGES, noChanges.invocation().state());
        return new Receipt(id, requestDigest, binding.baseRevision(), false);
      } finally {
        closeScope(scope);
      }
    }

    private ResultNode.Txn replayNoChanges(Receipt receipt) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        try (var result = stores.invocationResults().resolveNoChanges(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account(scope)).join()) {
          return assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(result.payload().buffer()).root());
        }
      } finally {
        closeScope(scope);
      }
    }

    private void assertResolvedResponse(Receipt receipt, TxnCommand command, boolean changed) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var result = stores.invocationResults().resolveTransaction(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account).join();
        assertEquals(changed, result.changed());
        var emitted = new AtomicReference<Rpc.TxnResponse>();
        var completed = new AtomicBoolean();
        TxnResponseEmitter.sendResolved(command, result, ResultMessages::decodeTree,
            stores.clusterId(), 1, scope, account, new StreamObserver<>() {
              @Override public void onNext(Rpc.TxnResponse value) { emitted.set(value); }
              @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
              @Override public void onCompleted() { completed.set(true); }
            });
        assertTrue(completed.get());
        assertEquals(receipt.revision(), emitted.get().getHeader().getRevision());
      } finally {
        closeScope(scope);
      }
    }

    private Receipt runViaExecutor(TxnCommand command, int requestNumber) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        ByteKey requestDigest = digest(requestNumber);
        var proposed = InvocationBinding.begin(id, requestDigest, AUTHORIZATION, TransactionId.create(),
            owner.epoch(), control.publishedSequence(), control.publishedRevision());
        var binding = invocations.bindOrJoin(proposed, scope).join();
        assertEquals(id, mutations.executeNew(command, binding, owner, environment.identity(), scope, account).join());
        var durable = stores.invocations().read(id, scope).join().orElseThrow();
        if (durable.state() == InvocationBinding.State.NO_CHANGES) {
          return new Receipt(id, requestDigest, binding.baseRevision(), false);
        }
        var outcome = stores.outcomes().resolve(durable, scope).join();
        assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, outcome.state());
        return new Receipt(id, requestDigest, outcome.decision().orElseThrow().revision(), true);
      } finally {
        closeScope(scope);
      }
    }

    private void assertFinalizationCapacityIsReservedBeforePreparing() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        var binding = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(6),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), control.publishedSequence(),
            control.publishedRevision()), scope).join();
        try (var held = account.tryAcquire(new BudgetManager.Work(0, 4, 0, 4, 0)).orElseThrow()) {
          var command = new TxnCommand(List.of(), List.of(put("blocked", "x")), List.of());
          var failure = assertThrows(java.util.concurrent.CompletionException.class,
              () -> mutations.executeNew(command, binding, owner, environment.identity(), scope, account).join());
          assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
              assertInstanceOf(BackendFailure.class, failure.getCause()).category());
          assertEquals(InvocationBinding.State.PREPARING,
              stores.invocations().read(binding.invocationId(), scope).join().orElseThrow().state());
          assertEquals(control.publishedSequence(), stores.control().read(scope).join()
              .orElseThrow().state().publishedSequence());
        }
      } finally {
        closeScope(scope);
      }
    }

    private void assertTaggedOwnerRequest() {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        var wire = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
            .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("tagged"))
                .setValue(ByteString.copyFromUtf8("value")))).build();
        ByteKey operation = MutationOperationEnvelope.transaction(wire.toByteArray());
        ByteKey requestDigest = MutationForwardRequest.digest(environment.system().tableGroupId(), operation);
        var state = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        var request = new MutationForwardRequest(environment.system().tableGroupId(), id, requestDigest,
            AUTHORIZATION, ByteKey.copyOf(bytes(environment.identity().contextRef())), owner.epoch(),
            Duration.ofMinutes(1).toNanos(), 1, operation);
        var binding = invocations.bindOrJoin(InvocationBinding.begin(id, requestDigest, AUTHORIZATION,
            TransactionId.create(), owner.epoch(), state.publishedSequence(), state.publishedRevision()), scope).join();
        var response = assertInstanceOf(OwnerForwarding.Completed.class,
            processor.process(request, binding, environment.identity(), scope).join());
        assertArrayEquals(id.bytes(), response.operationResult());
        var persisted = stores.invocations().read(id, scope).join().orElseThrow();
        var replay = assertInstanceOf(OwnerForwarding.Completed.class,
            processor.process(request, persisted, environment.identity(), scope).join());
        assertArrayEquals(id.bytes(), replay.operationResult());
      } finally {
        closeScope(scope);
      }
    }

    private void assertReadOnlyTransaction() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("a")),
            CompareCondition.Target.VERSION, CompareCondition.Relation.GREATER, 0)),
            List.of(new TxnCommand.PointRange(key("a")), new TxnCommand.PointRange(key("tagged"))), List.of());
        var evaluated = readOnly.execute(command, AUTHORIZATION, environment.identity(), scope, account).join();
        var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
        assertTrue(result.succeeded());
        assertEquals(5, result.header().revision());
        assertArrayEquals(bytes("new"), assertInstanceOf(ResultNode.Range.class,
            result.responses().getFirst()).records().getFirst().value().orElseThrow());
        assertTrue(stores.pins().readBoundary(scope).join().protections().isEmpty());
        assertEquals(4, stores.control().read(scope).join().orElseThrow().state().publishedSequence());
        var emitted = new AtomicReference<Rpc.TxnResponse>();
        var completed = new AtomicBoolean();
        TxnResponseEmitter.sendReadOnly(command, evaluated, stores.clusterId(), 1, scope, account,
            new StreamObserver<>() {
              @Override public void onNext(Rpc.TxnResponse value) { emitted.set(value); }
              @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
              @Override public void onCompleted() { completed.set(true); }
            });
        assertTrue(completed.get());
        assertEquals(5, emitted.get().getHeader().getRevision());
        assertEquals(2, emitted.get().getResponsesCount());
        assertReadResponseRetainedUntilConsumerDrains(command, evaluated, scope, account);
      } finally {
        closeScope(scope);
      }
    }

    private void assertLostOwnerRepliesAndNoChangeTakeover() throws Exception {
      var queue = new SerialMutationQueue(0, 8L << 20, TIMEOUT);
      var receiver = receiver(owner);
      var lane = new MutationOwnerLane(environment.system().tableGroupId(), queue, receiver);
      var route = new OwnerForwarding.Route(owner.instance(), owner.epoch(), "fixture", "fixture-peer");
      var dropReply = new AtomicBoolean();
      var lastRequest = new AtomicReference<MutationForwardRequest>();
      var relay = new MutationRelay(stores.control(), (group, instance, epoch) ->
          CompletableFuture.completedFuture(route), (selected, request, scope) -> {
            lastRequest.set(request);
            return lane.receive("fixture-peer", request, scope).thenApply(reply -> {
              if (reply instanceof OwnerForwarding.Completed && dropReply.compareAndSet(true, false)) {
                throw new CompletionException(new java.io.IOException("fixture dropped owner reply"));
              }
              return reply;
            });
          });
      var gateway = new TxnGateway(environment.system().tableGroupId(), environment.identity(), AUTHORIZATION,
          stores.clusterId(), 1, 1_572_864, stores.control(), relay, stores.invocationResults(),
          readOnly, budgets, reservation(), worker, new RequestMetrics());
      var service = new KvService((request, observer) -> observer.onError(Status.UNIMPLEMENTED.asRuntimeException()), gateway);
      var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 4, 0);
      try {
        try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
            "m3-txn-fault", "127.0.0.1", 0, environment.system().tableGroupId()), resources, service)) {
          var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port()).usePlaintext().build();
          try {
            var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
            var key = ByteString.copyFromUtf8("lost-reply");
            var put = Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder().setKey(key)
                .setValue(ByteString.copyFromUtf8("value"))).build();
            var range = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder().setKey(key)).build();
            var absent = Rpc.Compare.newBuilder().setKey(key).setTarget(Rpc.Compare.CompareTarget.VERSION)
                .setResult(Rpc.Compare.CompareResult.EQUAL).setVersion(0).build();
            dropReply.set(true);
            var changed = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(put).build());
            assertFalse(dropReply.get());
            assertEquals(6, changed.getHeader().getRevision());
            dropReply.set(true);
            var unchanged = stub.txn(Rpc.TxnRequest.newBuilder().addCompare(absent)
                .addSuccess(put).addFailure(range).build());
            assertFalse(dropReply.get());
            assertFalse(unchanged.getSucceeded());
            assertEquals(6, unchanged.getHeader().getRevision());
            var originalRequest = lastRequest.get();
            assertEquals(ByteString.copyFromUtf8("value"),
                unchanged.getResponses(0).getResponseRange().getKvs(0).getValue());
            var later = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
                .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("after-no-change"))
                    .setValue(ByteString.copyFromUtf8("later")))).build());
            assertEquals(7, later.getHeader().getRevision());
            assertNoChangeReplayAfterOwnerSwitch(originalRequest, unchanged);
          } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
          }
        }
      } finally {
        queue.close();
        queue.drained().get(30, TimeUnit.SECONDS);
        assertTrue(queue.awaitTermination(Duration.ofSeconds(30)));
      }
    }

    private void assertNettyCancellationWaitsForSerialization(Receipt receipt, TxnCommand command) throws Exception {
      M3NettyResponseProbe.verifyTxnCancellationDuringSerialization((scope, response) -> {
        var account = account(scope);
        var resolved = stores.invocationResults().resolveTransaction(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account).join();
        TxnResponseEmitter.sendResolved(command, resolved, ResultMessages::decodeTree,
            stores.clusterId(), 1, scope, account, response);
      }, () -> budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    private MutationForwardReceiver receiver(CommitOwnerService.OwnerToken token) {
      var ownerProcessor = new TxnOwnerProcessor(environment.system().tableGroupId(), token, budgets,
          reservation(), mutations, stores.outcomes(), 1_572_864);
      return new MutationForwardReceiver(stores.control(), invocations,
          (peer, request, scope) -> peer.equals("fixture-peer")
              ? CompletableFuture.completedFuture(new MutationForwardReceiver.VerifiedAuthorization(
                  environment.identity(), AUTHORIZATION))
              : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)), ownerProcessor);
    }

    private void assertNoChangeReplayAfterOwnerSwitch(MutationForwardRequest request,
        Rpc.TxnResponse expected) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        long afterExpiry = System.currentTimeMillis() + Duration.ofHours(2).toMillis();
        var nextOwner = new CommitOwnerService(stores.control()).claim("m3-next-owner", afterExpiry,
            afterExpiry + Duration.ofHours(1).toMillis(), scope).join().orElseThrow();
        var replay = new MutationForwardRequest(request.tableGroupId(), request.invocationId(),
            request.requestDigest(), request.authorizationScopeDigest(), request.authorizationContextReference(),
            nextOwner.epoch(), request.remainingTimeoutNanos(), request.hopCount(), request.operationBytes());
        var receipt = assertInstanceOf(OwnerForwarding.Completed.class,
            receiver(nextOwner).receive("fixture-peer", replay, scope).join());
        assertArrayEquals(request.invocationId().bytes(), receipt.operationResult());
        try (var original = stores.invocationResults().resolveTransaction(request.invocationId(),
            request.requestDigest(), AUTHORIZATION, scope, account(scope)).join()) {
          assertFalse(original.changed());
          assertEquals(expected.getHeader().getRevision(), original.revision());
          var tree = assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(original.payload().buffer()).root());
          assertFalse(tree.succeeded());
          assertEquals("value", new String(assertInstanceOf(ResultNode.Range.class,
              tree.responses().getFirst()).records().getFirst().value().orElseThrow(), StandardCharsets.UTF_8));
        }
        assertEquals(7, stores.control().read(scope).join().orElseThrow().state().publishedRevision());
      } finally {
        closeScope(scope);
      }
    }

    private void assertReadResponseRetainedUntilConsumerDrains(TxnCommand command,
        TxnEvaluator.ValidatedRead result, RequestScope scope, BudgetManager.Account account) throws Exception {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var completed = new AtomicBoolean();
      var failure = new AtomicReference<Throwable>();
      Thread sender = Thread.ofVirtual().start(() -> {
        try {
          TxnResponseEmitter.sendReadOnly(command, result, stores.clusterId(), 1, scope, account,
              new StreamObserver<>() {
                @Override public void onNext(Rpc.TxnResponse value) {
                  entered.countDown();
                  try {
                    if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("consumer did not drain");
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                  }
                }
                @Override public void onError(Throwable error) { failure.set(error); }
                @Override public void onCompleted() { completed.set(true); }
              });
        } catch (RuntimeException | Error error) {
          failure.set(error);
        }
      });
      try {
        assertTrue(entered.await(30, TimeUnit.SECONDS));
        scope.cancelAsync(task -> Thread.ofVirtual().start(task));
        assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
        assertTrue(budgets.snapshot().allocatedBytes() > 0);
      } finally {
        release.countDown();
        sender.join(30_000);
      }
      assertFalse(sender.isAlive());
      assertFalse(completed.get());
      assertTrue(failure.get() == null);
      scope.cleanupCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }

    private void assertValue(String key, long revision, String value) throws Exception {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        var record = environment.openPrimary(scope).get(key(key), revision, ReadProjection.FULL).join().orElseThrow();
        assertArrayEquals(bytes(value), record.value().orElseThrow());
      }
    }

    private long currentRevision() { return control().publishedRevision(); }
    private long currentSequence() { return control().publishedSequence(); }
    private io.etcdhbase.core.commit.CommitState control() {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        return stores.control().read(scope).join().orElseThrow().state();
      }
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      return budgets.admit(environment.system().tableGroupId(), scope, reservation());
    }

    private static BudgetManager.Reservation reservation() {
      return new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 48L << 20);
    }

    private void closeScope(RequestScope scope) {
      scope.close();
      scope.cleanupCompletion().toCompletableFuture().join();
      scope.physicalProducersCompletion().toCompletableFuture().join();
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    @Override public void close() throws Exception {
      scans.close();
      scans.termination().toCompletableFuture().get(30, TimeUnit.SECONDS);
      handoff.close();
      handoff.drained().join();
      worker.shutdown();
      if (!worker.awaitTermination(10, TimeUnit.SECONDS)) {
        worker.shutdownNow();
        throw new IllegalStateException("transaction test workers did not terminate");
      }
    }
  }

  private static TxnCommand.Put put(String key, String value) {
    return new TxnCommand.Put(new PutCommand(key(key), bytes(value), 0, false, false, false));
  }
  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
  private static ByteKey digest(int value) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, (byte) value);
    return ByteKey.copyOf(digest);
  }
}
