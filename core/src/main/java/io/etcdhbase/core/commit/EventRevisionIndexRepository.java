package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Immutable revision lookup; entries require verification against the selected decision. */
public interface EventRevisionIndexRepository {
  CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope);

  /** Reads a revision locator with its encoded storage cost for bounded proof work. */
  default CompletableFuture<SystemRecordRead<EventRevisionIndex>> readWithSize(
      long revision, RequestScope scope) {
    return SystemRecordRead.map(read(revision, scope), index ->
        Math.addExact(160L, Math.addExact(index.events().identity().length(),
            index.events().digest().length())));
  }

  CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      EventRevisionIndex index, RequestScope scope);

  /** Reads the first index at or after a revision in one salt bucket. */
  default CompletableFuture<EventRevisionIndexPage> firstAtOrAfter(
      int bucket, long revision, long maximumBytes, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "event revision index scans are not available"));
  }
}
