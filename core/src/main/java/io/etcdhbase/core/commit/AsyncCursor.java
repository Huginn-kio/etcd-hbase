package io.etcdhbase.core.commit;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Sequential source that can fetch the next bounded item without blocking an I/O thread. */
@FunctionalInterface
public interface AsyncCursor<T> {
  CompletableFuture<Optional<T>> next();

  static <T> AsyncCursor<T> fromIterator(Iterator<T> source) {
    Objects.requireNonNull(source, "source");
    return () -> {
      try {
        return CompletableFuture.completedFuture(source.hasNext()
            ? Optional.of(Objects.requireNonNull(source.next(), "cursor item")) : Optional.empty());
      } catch (RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
    };
  }
}
