package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;

/** Append-only persistence for private candidates and the sealed manifest tree. */
public interface ManifestRepository {
  /** Validates a candidate row identity and returns its fixed salt bucket. */
  int candidateBucket(PreparedCandidate candidate);

  CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
      PreparedCandidate candidate, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
      ManifestDirectoryPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent event, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
      EventDirectoryPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot root, RequestScope scope);

  CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
      ByteKey identity, RequestScope scope);
}
