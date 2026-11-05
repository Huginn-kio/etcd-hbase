package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;

/** Required backend boundary for registering a view before its first data read. */
@FunctionalInterface
public interface ReadViewPinRegistry {
  /** Registration must race safely with GC; an in-memory/no-op pin is not a production implementation. */
  CompletableFuture<Pin> register(ReadView view, RequestScope scope);

  interface Pin extends AutoCloseable {
    /** Monotonic generation for this registration slot. */
    long generation();

    /** Called after every scanner and point-read producer owned by the request has stopped. */
    @Override
    void close();
  }
}
