package io.etcdhbase.core.commit;

import java.util.Objects;

/** Strongly read state plus an opaque exact-value condition for a subsequent HBase CAS. */
public final class ControlSnapshot {
  private final CommitState state;
  private final byte[] comparisonToken;

  public ControlSnapshot(CommitState state, byte[] comparisonToken) {
    this.state = Objects.requireNonNull(state, "state");
    Objects.requireNonNull(comparisonToken, "comparisonToken");
    if (comparisonToken.length == 0 || comparisonToken.length > 64 * 1024) {
      throw new IllegalArgumentException("invalid control comparison token");
    }
    this.comparisonToken = comparisonToken.clone();
  }

  public CommitState state() { return state; }
  public byte[] comparisonToken() { return comparisonToken.clone(); }
}
