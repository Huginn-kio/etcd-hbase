package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Seals a KeepAlive response with the renewed Lease state and expiry candidate. */
public final class LeaseRenewalPreparation {
  public sealed interface Prepared permits Sealed, NoChanges {}

  public record Sealed(InvocationBinding invocation, ManifestRoot manifest,
      int auxiliaryRecordCount, long auxiliaryPayloadBytes) implements Prepared {
    public Sealed {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(manifest, "manifest");
      if (auxiliaryRecordCount < 0 || auxiliaryPayloadBytes < 0
          || invocation.state() != InvocationBinding.State.SEALED
          || invocation.manifestRoot().filter(manifest.systemIdentity()::equals).isEmpty()) {
        throw new IllegalArgumentException("sealed KeepAlive invocation and manifest differ");
      }
    }
  }

  public record NoChanges(InvocationBinding invocation) implements Prepared {
    public NoChanges {
      Objects.requireNonNull(invocation, "invocation");
      if (invocation.state() != InvocationBinding.State.NO_CHANGES) {
        throw new IllegalArgumentException("KeepAlive result has not completed without changes");
      }
    }
  }

  private record RenewalRecords(List<PreparedAuxiliaryRecord> records, long payloadBytes) {
    private RenewalRecords {
      records = List.copyOf(records);
      if (payloadBytes < 0) throw new IllegalArgumentException("negative renewal payload size");
    }
  }

  private final LeaseAuxiliaryMutationPreparation auxiliaryMutations;

  public LeaseRenewalPreparation(LeaseAuxiliaryMutationPreparation auxiliaryMutations) {
    this.auxiliaryMutations = Objects.requireNonNull(auxiliaryMutations, "auxiliaryMutations");
  }

  /** The caller owns the mutation lane, fixed view, and all service-scope cleanup. */
  public CompletableFuture<Prepared> prepare(InvocationBinding invocation,
      Rpc.LeaseKeepAliveResponse response, LeaseState renewedState, RequestScope scope,
      BudgetManager.Account account, Supplier<CompletableFuture<Void>> finishPinnedRead) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(response, "response");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(finishPinnedRead, "finishPinnedRead");
    if (invocation.state() != InvocationBinding.State.PREPARING || !account.belongsTo(scope)
        || invocation.baseSequence() == Long.MAX_VALUE
        || response.getTTL() < 0 || !response.hasHeader()
        || response.getHeader().getRevision() != invocation.baseRevision()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid KeepAlive result preparation"));
    }

    ByteKey responsePayload = ByteKey.copyOf(response.toByteArray());
    if (renewedState == null) {
      if (response.getTTL() != 0) {
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("unchanged KeepAlive result must have TTL zero"));
      }
      return auxiliaryMutations.completeWithoutChanges(invocation, responsePayload,
          finishPinnedRead, scope, account).thenApply(NoChanges::new);
    }
    RenewalRecords records = renewalRecords(invocation, response, renewedState);
    return auxiliaryMutations.prepare(invocation, responsePayload,
        records.records().iterator(), finishPinnedRead, scope, account)
        .thenApply(sealed -> new Sealed(sealed.invocation(), sealed.manifest(),
            records.records().size(), records.payloadBytes()));
  }

  private static RenewalRecords renewalRecords(InvocationBinding invocation,
      Rpc.LeaseKeepAliveResponse response, LeaseState state) {
    long leaseId = state.identity().leaseId();
    long sequence = state.sequence();
    if (state.status() != LeaseState.Status.ACTIVE || state.renewalGeneration() <= 0
        || sequence != invocation.baseSequence() + 1 || response.getID() != leaseId
        || response.getTTL() != state.grantedTtlSeconds()) {
      throw new IllegalArgumentException("renewed Lease state differs from its KeepAlive response");
    }
    var records = new ArrayList<PreparedAuxiliaryRecord>(3);
    records.add(PreparedAuxiliaryRecord.create(invocation.transactionId(),
        invocation.preparedOwnerEpoch(), invocation.baseSequence(), 0,
        SystemRecordType.LEASE_STATE, LeaseRecordIdentity.stateVersion(leaseId, sequence),
        LeaseStateMessages.encode(state)));
    records.add(PreparedAuxiliaryRecord.create(invocation.transactionId(),
        invocation.preparedOwnerEpoch(), invocation.baseSequence(), 1,
        SystemRecordType.LEASE_ENUMERATION, LeaseRecordIdentity.enumeration(leaseId),
        new LeaseEnumerationPointer(sequence).encode()));
    var expiry = new ExpiryCandidateIdentity(state.deadlineEpochMillis(), leaseId,
        state.identity().incarnation(), state.renewalGeneration());
    records.add(PreparedAuxiliaryRecord.create(invocation.transactionId(),
        invocation.preparedOwnerEpoch(), invocation.baseSequence(), 2,
        SystemRecordType.EXPIRY_CANDIDATE, expiry.encode(),
        new ExpiryCandidateRecord(sequence).encode()));
    long payloadBytes = records.stream()
        .mapToLong(record -> record.payload().length()).reduce(0, Math::addExact);
    return new RenewalRecords(records, payloadBytes);
  }
}
