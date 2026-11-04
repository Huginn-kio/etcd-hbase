package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable immutable decisions indexed by commit sequence. */
public interface CommitDecisionRepository {
  CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope);

  /** A duplicate sequence is accepted only when every decision field is identical. */
  CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      CommitDecision decision, RequestScope scope);
}
