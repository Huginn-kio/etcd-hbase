package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;

/** Applies visibility, filtering and the global limit to one ordered metadata stream. */
final class RangeSelector {
  enum Action { SKIP, INCLUDE, STOP }

  private final RangeCommand command;
  private long included;
  private long observedLive;
  private boolean more;
  private boolean stopped;

  RangeSelector(RangeCommand command) {
    this.command = Objects.requireNonNull(command, "command");
    if (command.countOnly() && command.range().kind() != KeyRange.Kind.POINT && command.limit() != 1) {
      throw new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION);
    }
  }

  Action observe(StoredRecord record) {
    Objects.requireNonNull(record, "record");
    if (stopped) throw new IllegalStateException("range selector has stopped");
    if (record.key().length() == 0 || !command.range().contains(record.key())) {
      throw new IllegalStateException("range view is outside its requested interval");
    }
    if (record.metadata().state() != StorageMetadata.State.LIVE) return Action.SKIP;
    observedLive = Math.addExact(observedLive, 1);
    if (command.countOnly()) {
      stopped = true;
      return Action.STOP;
    }
    if (!command.filter().matches(record.metadata())) return Action.SKIP;
    if (command.limit() > 0 && included >= command.limit()) {
      more = true;
      stopped = true;
      return Action.STOP;
    }
    included = Math.addExact(included, 1);
    return Action.INCLUDE;
  }

  long included() { return included; }

  /** May be partial when the selector stops early. */
  long observedLive() { return observedLive; }

  boolean more() { return more; }
}
