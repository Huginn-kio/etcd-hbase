package io.etcdhbase.server;

/** Closed set of fault barriers and their external fixture protocol values. */
enum M3HistoryFaultMode {
  BEFORE_DISPATCH("before-dispatch"),
  CANDIDATE_PREPARATION("candidate-preparation"),
  RESULT_PREPARATION("result-preparation"),
  SEALED_BEFORE_PUBLISH("sealed-before-publish"),
  CAS_ACK_LOST("cas-ack-lost"),
  PARTIAL_MATERIALIZATION("partial-materialization"),
  OLD_OWNER_RESUME("old-owner-resume"),
  CANCEL_AFTER_CAS("cancel-after-cas"),
  AFTER_COMMIT("after-commit");

  private final String wire;

  M3HistoryFaultMode(String wire) { this.wire = wire; }

  String wire() { return wire; }

  static M3HistoryFaultMode fromWire(String wire) {
    for (var mode : values()) {
      if (mode.wire.equals(wire)) return mode;
    }
    throw new IllegalArgumentException("invalid history server fixture arguments");
  }

  boolean interceptsHBase() {
    return this != BEFORE_DISPATCH && this != AFTER_COMMIT;
  }

  boolean holdsAcknowledgementForRelease() {
    return this == OLD_OWNER_RESUME || this == CANCEL_AFTER_CAS;
  }

  boolean beforePublication() {
    return this == CANDIDATE_PREPARATION || this == RESULT_PREPARATION || this == SEALED_BEFORE_PUBLISH;
  }

  @Override public String toString() { return wire; }
}
