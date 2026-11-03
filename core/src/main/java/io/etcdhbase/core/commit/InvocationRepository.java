package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable invocation bindings; one invocation ID can name only one immutable attempt. */
public interface InvocationRepository {
  CompletableFuture<Optional<InvocationBinding>> read(InvocationId invocationId, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      InvocationBinding binding, RequestScope scope);

  /** Compares the exact persisted state before recording a legal monotonic transition. */
  CompletableFuture<Boolean> compareAndSet(
      InvocationBinding expected, InvocationBinding replacement, RequestScope scope);
}
