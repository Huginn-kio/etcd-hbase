package io.etcdhbase.core.kv;

import java.util.Objects;

/** A protocol-level KV failure whose identity is independent of the transport. */
public final class KvFailure extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public enum Reason {
    EMPTY_KEY,
    KEY_NOT_FOUND,
    VALUE_PROVIDED,
    LEASE_PROVIDED,
    TOO_MANY_OPS,
    DUPLICATE_KEY,
    COMPACTED,
    FUTURE_REVISION,
    REQUEST_TOO_LARGE,
    LEASE_UNAVAILABLE,
    UNSUPPORTED_OPERATION,
    REQUEST_BUDGET_EXHAUSTED
  }

  private final Reason reason;

  public KvFailure(Reason reason) {
    super(Objects.requireNonNull(reason, "reason").name());
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }
}
