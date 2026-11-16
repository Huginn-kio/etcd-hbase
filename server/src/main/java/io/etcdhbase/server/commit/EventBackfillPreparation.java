package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Seals one protected backfill checkpoint as an auxiliary-only M2 transaction. */
final class EventBackfillPreparation {
  private final LeaseAuxiliaryMutationPreparation preparation;

  EventBackfillPreparation(LeaseAuxiliaryMutationPreparation preparation) {
    this.preparation = Objects.requireNonNull(preparation, "preparation");
  }

  /** The caller keeps the event window alive until the selected transaction is recoverable. */
  CompletableFuture<LeaseAuxiliaryMutationPreparation.Sealed> prepare(InvocationBinding invocation,
      EventIndexBackfillCheckpoint checkpoint, Supplier<CompletableFuture<Void>> checkProtection,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(checkProtection, "checkProtection");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (invocation.state() != InvocationBinding.State.PREPARING
        || invocation.preparedOwnerEpoch() != checkpoint.ownerEpoch()
        || invocation.baseSequence() == Long.MAX_VALUE
        || invocation.baseSequence() + 1 != checkpoint.sequence()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("backfill checkpoint differs from its invocation"));
    }
    var record = PreparedAuxiliaryRecord.create(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), 0, SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
        EventIndexBackfillCheckpoint.versionIdentity(checkpoint.sequence()), checkpoint.encode());
    return preparation.prepare(invocation, checkpoint.encode(), List.of(record).iterator(),
        checkProtection, scope, account);
  }
}
