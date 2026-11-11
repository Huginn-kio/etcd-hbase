package io.etcdhbase.server;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Await only after the fixture's scopes have been closed. */
final class M3CacheScopeDrain implements AutoCloseable {
  private final RequestScope[] scopes;

  M3CacheScopeDrain(RequestScope... scopes) { this.scopes = scopes.clone(); }

  @Override public void close() throws Exception {
    var completions = new CompletableFuture<?>[scopes.length * 2];
    for (int index = 0; index < scopes.length; index++) {
      completions[index * 2] = scopes[index].cleanupCompletion().toCompletableFuture();
      completions[index * 2 + 1] = scopes[index].physicalProducersCompletion().toCompletableFuture();
    }
    CompletableFuture.allOf(completions).get(30, TimeUnit.SECONDS);
  }
}
