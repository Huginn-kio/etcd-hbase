package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;

/** Service-owned continuation for decision archival, idempotent materialization, and final CAS. */
@FunctionalInterface
public interface CommitFinalizer {
  /** Finalizes a pending selection; unresolved and already-finalized observations pass through. */
  CompletableFuture<CommitOutcomeResolver.Resolution> finish(
      InvocationBinding invocation,
      CommitOutcomeResolver.Resolution publication,
      RequestScope serviceScope);
}
