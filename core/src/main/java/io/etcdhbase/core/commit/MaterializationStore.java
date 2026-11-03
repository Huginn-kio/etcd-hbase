package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;

/** Applies one already-selected immutable version without changing its sequence or revision. */
public interface MaterializationStore {
  enum ApplyResult {
    APPLIED,
    ALREADY_APPLIED,
    /** A newer selected version owns the current row; finalization must verify its durable state. */
    CURRENT_SUPERSEDED
  }

  CompletableFuture<ApplyResult> applySelected(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope);

  /** Must run before the publish CAS when the chosen layout can evict protected native versions. */
  CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope);
}
