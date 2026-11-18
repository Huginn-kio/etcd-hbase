package io.etcdhbase.server.commit;

import java.util.Objects;

/** A typed per-watch termination so protocol-significant boundaries survive the storage adapter. */
record WatchTermination(Kind kind, String reason, long compactRevision, Throwable failure) {
  enum Kind { CANCELLED, RESOURCE_LIMIT, UNAVAILABLE, COMPACTED, FAILURE }

  WatchTermination {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(reason, "reason");
    if (compactRevision < 0 || (kind == Kind.COMPACTED) != (compactRevision > 0)) {
      throw new IllegalArgumentException("invalid Watch termination boundary");
    }
    if ((kind == Kind.FAILURE) != (failure != null)) {
      throw new IllegalArgumentException("invalid Watch termination failure");
    }
    if (kind == Kind.COMPACTED && !reason.isEmpty()) {
      throw new IllegalArgumentException("compacted Watch termination has no cancel reason");
    }
  }

  static WatchTermination cancelled(String reason) {
    return new WatchTermination(Kind.CANCELLED, reason, 0, null);
  }

  static WatchTermination resourceLimit(String reason) {
    return new WatchTermination(Kind.RESOURCE_LIMIT, reason, 0, null);
  }

  static WatchTermination unavailable(String reason) {
    return new WatchTermination(Kind.UNAVAILABLE, reason, 0, null);
  }

  static WatchTermination compacted(long compactRevision) {
    return new WatchTermination(Kind.COMPACTED, "", compactRevision, null);
  }

  static WatchTermination failure(Throwable failure) {
    return new WatchTermination(Kind.FAILURE, "", 0,
        Objects.requireNonNull(failure, "failure"));
  }
}
