package io.etcdhbase.core.storage;

/** Scanner cleanup failed or is unconfirmed; its capacity must not be reused. */
public final class ScanCleanupFailure extends Exception {
  private static final long serialVersionUID = 1L;

  public ScanCleanupFailure(Throwable cause) { super("physical scan cleanup failed", cause); }

  /** Finds an explicit cleanup marker in a bounded exception chain. */
  public static boolean isPresent(Throwable failure) {
    for (int depth = 0; failure != null && depth < 16; depth++, failure = failure.getCause()) {
      if (failure instanceof ScanCleanupFailure) return true;
      for (Throwable suppressed : failure.getSuppressed()) {
        if (suppressed instanceof ScanCleanupFailure) return true;
      }
    }
    return false;
  }
}
