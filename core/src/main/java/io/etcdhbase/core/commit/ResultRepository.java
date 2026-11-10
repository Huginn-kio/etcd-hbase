package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable private result pages and their directory; each identity is write-once. */
public interface ResultRepository {
  CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
      ResultDirectory directory, RequestScope scope);

  CompletableFuture<Optional<ResultPage>> readPage(
      ResultIdentity identity, int index, RequestScope scope);

  CompletableFuture<Optional<ResultDirectory>> readDirectory(
      ResultIdentity identity, RequestScope scope);
}
