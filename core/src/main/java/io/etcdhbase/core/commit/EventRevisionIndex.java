package io.etcdhbase.core.commit;

import java.util.Objects;

/** A revision's event root and the durable decision that selected it. */
public record EventRevisionIndex(CommitDecision decision, EventReference events) {
  public EventRevisionIndex {
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(events, "events");
    if (!decision.containsKeyChanges() || events.firstOrdinal() != 0) {
      throw new IllegalArgumentException("event index requires a complete KV-changing decision");
    }
  }

  /** Validates the sealed root against the decision before deriving an index entry. */
  public static EventRevisionIndex fromSelected(CommitDecision decision, ManifestRoot root) {
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(root, "root");
    if (!decision.containsKeyChanges() || !root.containsKeyChanges()
        || !decision.transactionId().equals(root.transactionId())
        || decision.sequence() - 1 != root.baseSequence()
        || decision.baseRevision() != root.baseRevision()
        || !decision.manifestRoot().equals(root.systemIdentity())
        || !decision.manifestDigest().equals(root.digest())) {
      throw new IllegalStateException("event index decision differs from its sealed manifest");
    }
    EventReference events = root.eventDirectory().orElseThrow(
        () -> new IllegalStateException("selected KV change has no sealed event directory"));
    return new EventRevisionIndex(decision, events);
  }

  /** The persisted index alone does not prove selection or event-page integrity. */
  public void verifySelected(CommitDecision selected, ManifestRoot root) {
    if (!equals(fromSelected(selected, root))) {
      throw new IllegalStateException("event index differs from the selected decision or event root");
    }
  }
}
