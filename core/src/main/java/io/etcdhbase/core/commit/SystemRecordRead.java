package io.etcdhbase.core.commit;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToLongFunction;

/** A decoded system record and its encoded identity-plus-value bytes. */
public record SystemRecordRead<T>(Optional<T> value, long encodedBytes) {
  public SystemRecordRead {
    Objects.requireNonNull(value, "value");
    if (encodedBytes < 0 || value.isEmpty() && encodedBytes != 0) {
      throw new IllegalArgumentException("invalid system-record read byte count");
    }
  }

  /** Builds a bounded model estimate for adapters that cannot expose the stored payload length. */
  public static <T> SystemRecordRead<T> estimated(
      Optional<T> value, ToLongFunction<? super T> encodedBytes) {
    Objects.requireNonNull(value, "value");
    Objects.requireNonNull(encodedBytes, "encodedBytes");
    return new SystemRecordRead<>(value, value.map(encodedBytes::applyAsLong).orElse(0L));
  }

  static <T> CompletableFuture<SystemRecordRead<T>> map(
      CompletableFuture<Optional<T>> read, ToLongFunction<? super T> encodedBytes) {
    return read.thenApply(value -> estimated(value, encodedBytes));
  }
}
