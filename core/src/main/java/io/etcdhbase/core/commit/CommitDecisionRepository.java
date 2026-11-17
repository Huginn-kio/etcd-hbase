package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable immutable decisions indexed by commit sequence. */
public interface CommitDecisionRepository {
  CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope);

  /** Reads the immutable decision with its encoded storage cost for bounded proof work. */
  default CompletableFuture<SystemRecordRead<CommitDecision>> readWithSize(
      long sequence, RequestScope scope) {
    return SystemRecordRead.map(read(sequence, scope), decision ->
        Math.addExact(120L, Math.addExact(decision.manifestRoot().length(),
            decision.manifestDigest().length())));
  }

  /** A duplicate sequence is accepted only when every decision field is identical. */
  CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      CommitDecision decision, RequestScope scope);
}
