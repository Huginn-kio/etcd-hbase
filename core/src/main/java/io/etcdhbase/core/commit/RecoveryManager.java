package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Recovers only the control row's bounded pending publication after claiming a new owner epoch. */
public final class RecoveryManager {
  public enum State { OWNER_ACTIVE, NO_PENDING_PUBLICATION, FINALIZED }

  public record Result(State state, Optional<CommitOwnerService.OwnerToken> owner,
      Optional<CommitOutcomeResolver.Resolution> resolution) {
    public Result {
      Objects.requireNonNull(state, "state");
      Objects.requireNonNull(owner, "owner");
      Objects.requireNonNull(resolution, "resolution");
      boolean valid = switch (state) {
        case OWNER_ACTIVE -> owner.isEmpty() && resolution.isEmpty();
        case NO_PENDING_PUBLICATION -> owner.isPresent() && resolution.isEmpty();
        case FINALIZED -> owner.isPresent() && resolution.isPresent()
            && resolution.orElseThrow().state() == CommitOutcomeResolver.State.SELECTED_FINALIZED;
      };
      if (!valid) {
        throw new IllegalArgumentException("recovery result does not match its state");
      }
    }
  }

  private final ByteKey expectedProfileFingerprint;
  private final CommitControlStore control;
  private final CommitOwnerService owners;
  private final InvocationRepository invocations;
  private final CommitOutcomeResolver outcomes;
  private final CommitFinalizer finalizer;

  /**
   * The supplied stores must already be opened with a validated format and authorized backend
   * identity. Recovery checks the logical table-group profile before it claims the owner row.
   */
  public RecoveryManager(ByteKey expectedProfileFingerprint, CommitControlStore control,
      CommitOwnerService owners, InvocationRepository invocations, CommitOutcomeResolver outcomes,
      CommitFinalizer finalizer) {
    this.expectedProfileFingerprint = requireProfile(expectedProfileFingerprint);
    this.control = Objects.requireNonNull(control, "control");
    this.owners = Objects.requireNonNull(owners, "owners");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.finalizer = Objects.requireNonNull(finalizer, "finalizer");
  }

  /**
   * Claims/fences the prior owner, then resumes the sole selected publication if present. A
   * missing publication is authoritative: no invocation or KV scan is performed during startup.
   * The caller must keep the table group unready for writes until this method returns a state
   * other than OWNER_ACTIVE and all external startup checks have completed.
   */
  public CompletableFuture<Result> recover(String instance, long nowEpochMillis,
      long leaseDeadlineEpochMillis, RequestScope serviceScope) {
    Objects.requireNonNull(instance, "instance");
    Objects.requireNonNull(serviceScope, "serviceScope");
    if (instance.isBlank()
        || instance.getBytes(StandardCharsets.UTF_8).length > CommitState.MAX_OWNER_INSTANCE_BYTES
        || instance.codePoints().anyMatch(Character::isISOControl)
        || nowEpochMillis < 0 || leaseDeadlineEpochMillis <= nowEpochMillis) {
      return failed(new IllegalArgumentException("invalid recovery owner identity or lease interval"));
    }
    return control.read(serviceScope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        return failed(new IllegalStateException("M2 control state must be initialized before recovery"));
      }
      validateProfile(snapshot.orElseThrow().state());
      return owners.claim(instance, nowEpochMillis, leaseDeadlineEpochMillis, serviceScope)
          .thenCompose(owner -> owner.isEmpty()
              ? CompletableFuture.completedFuture(new Result(
                  State.OWNER_ACTIVE, Optional.empty(), Optional.empty()))
              : recoverClaimed(owner.orElseThrow(), serviceScope));
    });
  }

  private CompletableFuture<Result> recoverClaimed(
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) return failed(new IllegalStateException("M2 control state disappeared"));
      CommitState state = snapshot.orElseThrow().state();
      validateProfile(state);
      if (state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()) {
        return failed(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      if (state.publication().isEmpty()) {
        return CompletableFuture.completedFuture(new Result(
            State.NO_PENDING_PUBLICATION, Optional.of(owner), Optional.empty()));
      }
      PublicationSlot slot = state.publication().orElseThrow();
      CommitDecision selected = CommitDecision.from(slot);
      return invocations.read(slot.invocationId(), scope).thenCompose(stored -> {
        if (stored.isEmpty()) {
          return failed(new IllegalStateException("selected publication has no durable invocation binding"));
        }
        InvocationBinding invocation = stored.orElseThrow();
        try {
          validateBinding(slot, invocation);
        } catch (RuntimeException failure) {
          return failed(failure);
        }
        return outcomes.resolveForFinalization(invocation, scope).thenCompose(observed -> {
          if (!selectedResolution(observed, selected)) {
            return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
          }
          CompletableFuture<CommitOutcomeResolver.Resolution> finishing =
              observed.state() == CommitOutcomeResolver.State.SELECTED_PENDING
                  ? finalizer.finish(invocation, observed, scope)
                  : CompletableFuture.completedFuture(observed);
          return finishing.thenCompose(result -> verifyFinalized(owner, selected, result, scope));
        });
      });
    });
  }

  private CompletableFuture<Result> verifyFinalized(CommitOwnerService.OwnerToken owner,
      CommitDecision selected, CommitOutcomeResolver.Resolution result, RequestScope scope) {
    if (!selectedResolution(result, selected)
        || result.state() != CommitOutcomeResolver.State.SELECTED_FINALIZED) {
      return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    return control.read(scope).thenApply(snapshot -> {
      if (snapshot.isEmpty()) throw new IllegalStateException("M2 control state disappeared after recovery");
      CommitState state = snapshot.orElseThrow().state();
      validateProfile(state);
      if (state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.finalizedSequence() < selected.sequence()
          || state.finalizedRevision() < selected.revision()
          || state.publishedSequence() < selected.sequence()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return new Result(State.FINALIZED, Optional.of(owner), Optional.of(result));
    });
  }

  private void validateProfile(CommitState state) {
    if (!state.profileFingerprint().equals(expectedProfileFingerprint)) {
      throw new IllegalStateException("M2 control profile differs from the recovery configuration");
    }
  }

  private static void validateBinding(PublicationSlot slot, InvocationBinding invocation) {
    if (invocation.state() != InvocationBinding.State.SEALED
        && invocation.state() != InvocationBinding.State.SELECTED
        && invocation.state() != InvocationBinding.State.WINDOW_CLOSED) {
      throw new IllegalStateException("selected publication has an invocation in an impossible state");
    }
    if (!invocation.invocationId().equals(slot.invocationId())
        || !invocation.transactionId().equals(slot.transactionId())
        || invocation.preparedOwnerEpoch() != slot.ownerEpoch()
        || invocation.baseSequence() != slot.baseSequence()
        || invocation.baseRevision() != slot.baseRevision()
        || invocation.manifestRoot().filter(slot.manifestRoot()::equals).isEmpty()) {
      throw new IllegalStateException("selected publication differs from its durable invocation binding");
    }
  }

  private static boolean selectedResolution(
      CommitOutcomeResolver.Resolution resolution, CommitDecision selected) {
    return (resolution.state() == CommitOutcomeResolver.State.SELECTED_PENDING
        || resolution.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED)
        && resolution.decision().filter(selected::equals).isPresent();
  }

  private static ByteKey requireProfile(ByteKey fingerprint) {
    Objects.requireNonNull(fingerprint, "expectedProfileFingerprint");
    if (fingerprint.length() != 32) throw new IllegalArgumentException("profile fingerprint must be SHA-256");
    return fingerprint;
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }
}
