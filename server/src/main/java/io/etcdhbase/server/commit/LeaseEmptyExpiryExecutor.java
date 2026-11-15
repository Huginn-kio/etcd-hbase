package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Keeps the legacy empty-Lease command on the shared, atomic M2 revocation path. */
public final class LeaseEmptyExpiryExecutor {
  enum Rejection { RECHECK, UNAVAILABLE }

  static final class RejectedBeforePreparation extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Rejection rejection;

    private RejectedBeforePreparation(Rejection rejection, Throwable cause) {
      super(cause);
      this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    Rejection rejection() { return rejection; }
  }

  private final LeaseRevocationExecutor revocations;
  private final LeaseTimeSource time;

  public LeaseEmptyExpiryExecutor(LeaseRevocationExecutor revocations, LeaseTimeSource time) {
    this.revocations = Objects.requireNonNull(revocations, "revocations");
    this.time = Objects.requireNonNull(time, "time");
  }

  /** Caller owns the serialized owner lane and resolves the durable invocation binding first. */
  public CompletableFuture<InvocationId> executeNew(ExpiryCandidateIdentity candidate,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, RequestScope scope, BudgetManager.Account account) {
    final LeaseTimeObservation observation;
    try {
      observation = Objects.requireNonNull(time.observe(), "Lease time observation");
    } catch (RuntimeException unhealthyClock) {
      return CompletableFuture.failedFuture(new RejectedBeforePreparation(
          Rejection.UNAVAILABLE, unhealthyClock));
    }
    return revocations.executeExpiredEmptyCandidate(candidate, binding, owner,
        identity, observation, scope, account)
        .handle((invocation, failure) -> {
          if (failure == null) return CompletableFuture.completedFuture(invocation);
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          if (cause instanceof LeaseRevocationExecutor.RejectedBeforePreparation rejected) {
            Rejection rejection = rejected.rejection()
                == LeaseRevocationExecutor.CandidateRejection.RECHECK
                    ? Rejection.RECHECK : Rejection.UNAVAILABLE;
            return CompletableFuture.<InvocationId>failedFuture(
                new RejectedBeforePreparation(rejection, cause));
          }
          return CompletableFuture.<InvocationId>failedFuture(new CompletionException(cause));
        })
        .thenCompose(result -> result);
  }
}
