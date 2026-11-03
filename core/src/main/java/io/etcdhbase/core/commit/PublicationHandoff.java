package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Transfers bounded publish resolution ownership from a request to the service lifecycle. */
@FunctionalInterface
public interface PublicationHandoff {
  /**
   * Accepts the attempt before running work. Once accepted, work uses a service-owned scope and
   * continues even if the caller stops waiting; rejection guarantees the callback was not invoked.
   */
  <T> CompletableFuture<T> transfer(
      InvocationBinding invocation,
      RequestScope requestScope,
      Function<RequestScope, CompletableFuture<T>> serviceWork);
}
