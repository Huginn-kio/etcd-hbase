package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Immutable revision lookup; entries require verification against the selected decision. */
public interface EventRevisionIndexRepository {
  CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      EventRevisionIndex index, RequestScope scope);
}
