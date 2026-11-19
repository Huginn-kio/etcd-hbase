package io.etcdhbase.server.commit;

/** Aggregates unchecked failures while still attempting every Watch resource cleanup. */
final class WatchCleanup {
  private WatchCleanup() {}

  static Throwable closeAndCollect(Runnable close, Throwable failure) {
    try {
      close.run();
    } catch (RuntimeException | Error cleanupFailure) {
      if (failure == null) return cleanupFailure;
      if (failure == cleanupFailure) return failure;
      if (cleanupFailure instanceof Error && !(failure instanceof Error)) {
        cleanupFailure.addSuppressed(failure);
        return cleanupFailure;
      }
      failure.addSuppressed(cleanupFailure);
    }
    return failure;
  }

  static void throwIfPresent(Throwable failure) {
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException unchecked) throw unchecked;
    if (failure != null) {
      throw new AssertionError("Watch cleanup produced a checked failure", failure);
    }
  }
}
