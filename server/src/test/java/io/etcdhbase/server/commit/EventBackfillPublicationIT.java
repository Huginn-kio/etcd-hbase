package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryFormatUpgrade;
import io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifier;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.EventHistoryRegistryUpgrade;
import io.etcdhbase.core.commit.EventIndexBackfillCapture;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillPlanner;
import io.etcdhbase.core.commit.EventIndexFormatUpgrade;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.LeaseBindingFormatUpgrade;
import io.etcdhbase.core.commit.LeaseExpiryFormatUpgrade;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Exercises actual checkpoint selection and materialization on temporary HBase. */
class EventBackfillPublicationIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(3);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final SnapshotPinConfiguration PINS = new SnapshotPinConfiguration(2, 16, Duration.ofMinutes(1));

  @Test
  void publishesAndReopensTheInitialCheckpointOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var fixture = M3TestEnvironment.start(layout);
           var scope = RequestScope.withTimeout(TIMEOUT)) {
        enableCapabilities(fixture, scope);
        var stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, scope).join();
        long now = System.currentTimeMillis();
        var owner = new CommitOwnerService(stores.control())
            .claim("backfill-test-owner", now, now + TIMEOUT.toMillis(), scope).join().orElseThrow();
        owner = new AuxiliaryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseBindingFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseExpiryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new EventIndexFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        var selected = new EventHistoryRegistryUpgrade(stores.records(), stores.control(), PINS, Clock.systemUTC())
            .upgrade(owner, scope).join();
        owner = selected.owner();
        var pins = selected.registry();

        var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
        var budgets = new BudgetManager(limits, Map.of(fixture.system().tableGroupId(), limits), limits);
        var account = budgets.admit(fixture.system().tableGroupId(), scope,
            new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8L << 20),
            new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000));
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 64, 4L << 30);
        var checkpoint = new EventIndexBackfillPlanner(stores.control(), stores.decisions())
            .plan(owner, pins.namespace().orElseThrow(), pins.registryIdentity(), scope, budget).join();
        var capture = new EventIndexBackfillCapture(pins, stores.control(), stores.decisions());
        var window = capture.open(checkpoint, AUTHORIZATION, scope, budget).join();
        var invocations = new InvocationService(stores.invocations());
        var invocation = InvocationBinding.begin(InvocationId.create(), AUTHORIZATION, AUTHORIZATION,
            TransactionId.create(), owner.epoch(), checkpoint.sequence() - 1, checkpoint.upperRevision());
        invocation = invocations.bindOrJoin(invocation, scope).join();
        var preparation = new EventBackfillPreparation(new LeaseAuxiliaryMutationPreparation(
            new ResultPreparation(stores.resultRepository()), stores.results(),
            new ManifestPreparation(stores.manifestRepository(), new ManifestPreparation.Limits(0, 0, 0, 0)),
            invocations));
        var handoff = new BoundedPublicationHandoff(1, 1, TIMEOUT);
        try {
          var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
              new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results(),
                  stores.auxiliaryMaterialization(), new AuxiliaryLeasePlanVerifier(
                      LeaseStateMessages::decode, LeaseBindingMessages::decode)),
              stores.invocations(), handoff, stores.finalizer());
          var result = new EventBackfillExecutor(stores.control(), preparation, publisher, Clock.systemUTC())
              .publish(invocation, owner, checkpoint, window, scope, account, budget).join();
          assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
          var state = stores.control().read(scope).join().orElseThrow().state();
          assertEquals(1, state.finalizedSequence());
          assertEquals(1, state.finalizedRevision());
          var reopened = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
              fixture.primary(), fixture.current(), PINS, scope).join();
          var durable = reopened.records().latestAtOrBefore(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
              EventIndexBackfillCheckpoint.routePrefix(), state.finalizedSequence(), scope).join().orElseThrow();
          assertEquals(checkpoint, EventIndexBackfillCheckpoint.decode(durable.value()));
          assertTrue(checkpoint.complete());
        } finally {
          handoff.close();
          handoff.drained().join();
          window.close();
          window.releaseCompletion().toCompletableFuture().join();
        }
      }
    }
  }

  private static void enableCapabilities(M3TestEnvironment fixture, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    var connection = request.borrowConnection(fixture.factory(), fixture.identity()).join();
    var admin = connection.getAdmin();
    var descriptor = request.execute(() -> admin.getDescriptor(fixture.system().table())).join();
    descriptor = fixture.system().withAuxiliaryCapability(descriptor);
    descriptor = fixture.system().withLeaseBindingCapability(descriptor);
    descriptor = fixture.system().withLeaseExpiryCheckpointCapability(descriptor);
    descriptor = fixture.system().withEventRevisionIndexCapability(descriptor);
    var replacement = descriptor;
    request.execute(() -> admin.modifyTable(replacement)).join();
  }
}
