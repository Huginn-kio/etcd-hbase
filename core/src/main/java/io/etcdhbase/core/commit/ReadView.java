package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;

/** Immutable logical snapshot captured from one strong read of the authoritative control row. */
public final class ReadView {
  private final ByteKey profileFingerprint;
  private final ByteKey authorizationScopeDigest;
  private final long barrierSequence;
  private final long finalizedSequence;
  private final long targetRevision;
  private final long stableRevision;
  private final long compactRevision;
  private final Optional<CommitDecision> selectedDecision;
  private final Optional<ManifestRoot> selectedRoot;
  private final long pinGeneration;

  ReadView(ByteKey profileFingerprint, ByteKey authorizationScopeDigest, long barrierSequence,
      long finalizedSequence, long targetRevision, long stableRevision, long compactRevision,
      Optional<CommitDecision> selectedDecision, Optional<ManifestRoot> selectedRoot,
      long pinGeneration) {
    this.profileFingerprint = requireDigest(profileFingerprint, "profile fingerprint");
    this.authorizationScopeDigest = requireDigest(authorizationScopeDigest, "authorization scope digest");
    this.selectedDecision = Objects.requireNonNull(selectedDecision, "selectedDecision");
    this.selectedRoot = Objects.requireNonNull(selectedRoot, "selectedRoot");
    if (barrierSequence < 0 || finalizedSequence < 0 || finalizedSequence > barrierSequence
        || targetRevision <= 0 || stableRevision <= 0 || compactRevision < 0
        || compactRevision > targetRevision || stableRevision > targetRevision || pinGeneration < 0
        || selectedDecision.isPresent() != selectedRoot.isPresent()) {
      throw new IllegalArgumentException("invalid read view state");
    }
    if (selectedDecision.isPresent()) {
      CommitDecision decision = selectedDecision.orElseThrow();
      ManifestRoot root = selectedRoot.orElseThrow();
      if (decision.sequence() <= finalizedSequence || decision.sequence() > barrierSequence
          || decision.revision() > targetRevision
          || !decision.transactionId().equals(root.transactionId())
          || !decision.manifestRoot().equals(root.systemIdentity())
          || !decision.manifestDigest().equals(root.digest())
          || root.baseSequence() + 1 != decision.sequence()
          || root.baseRevision() != decision.baseRevision()
          || root.containsKeyChanges() != decision.containsKeyChanges()
          || decision.revision() != root.baseRevision() + (root.containsKeyChanges() ? 1 : 0)) {
        throw new IllegalArgumentException("selected read overlay differs from its committed manifest");
      }
    }
    this.barrierSequence = barrierSequence;
    this.finalizedSequence = finalizedSequence;
    this.targetRevision = targetRevision;
    this.stableRevision = stableRevision;
    this.compactRevision = compactRevision;
    this.pinGeneration = pinGeneration;
  }

  public ByteKey profileFingerprint() { return profileFingerprint; }
  public ByteKey authorizationScopeDigest() { return authorizationScopeDigest; }
  public long barrierSequence() { return barrierSequence; }
  public long finalizedSequence() { return finalizedSequence; }
  public long targetRevision() { return targetRevision; }
  public long stableRevision() { return stableRevision; }
  public long compactRevision() { return compactRevision; }
  public Optional<CommitDecision> selectedDecision() { return selectedDecision; }
  public Optional<ManifestRoot> selectedRoot() { return selectedRoot; }
  public long pinGeneration() { return pinGeneration; }
  public boolean includesSelectedKeyChanges() {
    return selectedDecision.filter(CommitDecision::containsKeyChanges).isPresent();
  }

  ReadView withPinGeneration(long generation) {
    return new ReadView(profileFingerprint, authorizationScopeDigest, barrierSequence,
        finalizedSequence, targetRevision, stableRevision, compactRevision,
        selectedDecision, selectedRoot, generation);
  }

  static ReadView capture(CommitState state, long targetRevision, ByteKey authorizationScopeDigest,
      Optional<CommitDecision> decision, Optional<ManifestRoot> root) {
    Objects.requireNonNull(state, "state");
    if (targetRevision < state.compactRevision()) {
      throw new IllegalArgumentException("requested revision was compacted");
    }
    if (targetRevision > state.publishedRevision()) {
      throw new IllegalArgumentException("requested revision is in the future");
    }
    return new ReadView(state.profileFingerprint(), authorizationScopeDigest,
        state.publishedSequence(), state.finalizedSequence(), targetRevision,
        Math.min(targetRevision, state.finalizedRevision()), state.compactRevision(),
        decision, root, 0);
  }

  private static ByteKey requireDigest(ByteKey value, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != 32) throw new IllegalArgumentException(name + " must be SHA-256");
    return value;
  }
}
