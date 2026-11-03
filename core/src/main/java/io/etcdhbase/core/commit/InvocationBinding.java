package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;

/** Durable one-to-one binding between an internal invocation and one preparation attempt. */
public final class InvocationBinding {
  public enum State { PREPARING, SEALED, SELECTED, NOT_SELECTED, WINDOW_CLOSED }

  public static final int MAX_RESULT_REFERENCE_BYTES = 4096;

  private final InvocationId invocationId;
  private final ByteKey requestDigest;
  private final ByteKey authorizationScopeDigest;
  private final TransactionId transactionId;
  private final long preparedOwnerEpoch;
  private final long baseSequence;
  private final long baseRevision;
  private final State state;
  private final Optional<ByteKey> manifestRoot;
  private final Optional<ByteKey> resultReference;

  private InvocationBinding(
      InvocationId invocationId,
      ByteKey requestDigest,
      ByteKey authorizationScopeDigest,
      TransactionId transactionId,
      long preparedOwnerEpoch,
      long baseSequence,
      long baseRevision,
      State state,
      Optional<ByteKey> manifestRoot,
      Optional<ByteKey> resultReference) {
    this.invocationId = Objects.requireNonNull(invocationId, "invocationId");
    this.requestDigest = requireDigest(requestDigest, "requestDigest");
    this.authorizationScopeDigest = requireDigest(authorizationScopeDigest, "authorizationScopeDigest");
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.state = Objects.requireNonNull(state, "state");
    this.manifestRoot = Objects.requireNonNull(manifestRoot, "manifestRoot");
    this.resultReference = Objects.requireNonNull(resultReference, "resultReference");
    if (preparedOwnerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0
        || manifestRoot.filter(root -> root.length() == 0 || root.length() > 8192).isPresent()
        || resultReference.filter(reference -> reference.length() == 0
            || reference.length() > MAX_RESULT_REFERENCE_BYTES).isPresent()) {
      throw new IllegalArgumentException("invalid invocation binding");
    }
    switch (state) {
      case PREPARING -> {
        if (manifestRoot.isPresent() || resultReference.isPresent()) {
          throw new IllegalArgumentException("preparing invocation cannot have sealed output");
        }
      }
      case SEALED -> {
        if (manifestRoot.isEmpty() || resultReference.isPresent()) {
          throw new IllegalArgumentException("sealed invocation requires only a manifest root");
        }
      }
      case SELECTED -> {
        if (manifestRoot.isEmpty() || resultReference.isEmpty()) {
          throw new IllegalArgumentException("selected invocation requires manifest and result references");
        }
      }
      case WINDOW_CLOSED -> {
        if (resultReference.isPresent()) {
          throw new IllegalArgumentException("closed result window must not retain a result reference");
        }
      }
      case NOT_SELECTED -> {
        if (resultReference.isPresent()) {
          throw new IllegalArgumentException("unselected invocation cannot have a result reference");
        }
      }
    }
    this.preparedOwnerEpoch = preparedOwnerEpoch;
    this.baseSequence = baseSequence;
    this.baseRevision = baseRevision;
  }

  public static InvocationBinding begin(
      InvocationId invocationId,
      ByteKey requestDigest,
      ByteKey authorizationScopeDigest,
      TransactionId transactionId,
      long preparedOwnerEpoch,
      long baseSequence,
      long baseRevision) {
    return new InvocationBinding(invocationId, requestDigest, authorizationScopeDigest,
        transactionId, preparedOwnerEpoch, baseSequence, baseRevision, State.PREPARING,
        Optional.empty(), Optional.empty());
  }

  /** Restores a durable invocation record after validating all immutable binding fields. */
  public static InvocationBinding restore(
      InvocationId invocationId,
      ByteKey requestDigest,
      ByteKey authorizationScopeDigest,
      TransactionId transactionId,
      long preparedOwnerEpoch,
      long baseSequence,
      long baseRevision,
      State state,
      Optional<ByteKey> manifestRoot,
      Optional<ByteKey> resultReference) {
    return new InvocationBinding(invocationId, requestDigest, authorizationScopeDigest,
        transactionId, preparedOwnerEpoch, baseSequence, baseRevision, state, manifestRoot,
        resultReference);
  }

  public InvocationBinding seal(ByteKey rootIdentity) {
    if (state != State.PREPARING) throw new IllegalStateException("invocation is not preparing");
    return copy(State.SEALED, Optional.of(Objects.requireNonNull(rootIdentity, "rootIdentity")), resultReference);
  }

  public InvocationBinding selected(ByteKey result) {
    if (state != State.SEALED) throw new IllegalStateException("only a sealed invocation can be selected");
    return copy(State.SELECTED, manifestRoot, Optional.of(Objects.requireNonNull(result, "result")));
  }

  /** Only the outcome resolver can create the proof required to make this transition. */
  public InvocationBinding notSelected(NotSelectedProof proof) {
    Objects.requireNonNull(proof, "proof");
    if (!proof.proves(this)) throw new IllegalArgumentException("proof belongs to another invocation");
    if (state != State.PREPARING && state != State.SEALED) {
      throw new IllegalStateException("invocation cannot become not-selected from its current state");
    }
    return copy(State.NOT_SELECTED, manifestRoot, Optional.empty());
  }

  public InvocationBinding closeQueryWindow() {
    if (state != State.SELECTED && state != State.NOT_SELECTED) {
      throw new IllegalStateException("only a resolved invocation can close its query window");
    }
    return copy(State.WINDOW_CLOSED, manifestRoot, Optional.empty());
  }

  public InvocationId invocationId() { return invocationId; }
  public ByteKey requestDigest() { return requestDigest; }
  public ByteKey authorizationScopeDigest() { return authorizationScopeDigest; }
  public TransactionId transactionId() { return transactionId; }
  public long preparedOwnerEpoch() { return preparedOwnerEpoch; }
  public long baseSequence() { return baseSequence; }
  public long baseRevision() { return baseRevision; }
  public State state() { return state; }
  public Optional<ByteKey> manifestRoot() { return manifestRoot; }
  public Optional<ByteKey> resultReference() { return resultReference; }

  public boolean sameAttempt(InvocationBinding other) {
    Objects.requireNonNull(other, "other");
    return invocationId.equals(other.invocationId)
        && requestDigest.equals(other.requestDigest)
        && authorizationScopeDigest.equals(other.authorizationScopeDigest)
        && transactionId.equals(other.transactionId)
        && preparedOwnerEpoch == other.preparedOwnerEpoch
        && baseSequence == other.baseSequence && baseRevision == other.baseRevision;
  }

  private InvocationBinding copy(State nextState, Optional<ByteKey> nextRoot,
      Optional<ByteKey> nextResult) {
    return new InvocationBinding(invocationId, requestDigest, authorizationScopeDigest,
        transactionId, preparedOwnerEpoch, baseSequence, baseRevision, nextState, nextRoot,
        nextResult);
  }

  private static ByteKey requireDigest(ByteKey digest, String name) {
    Objects.requireNonNull(digest, name);
    if (digest.length() != 32) throw new IllegalArgumentException(name + " must be SHA-256");
    return digest;
  }
}
