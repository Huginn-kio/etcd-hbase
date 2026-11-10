package io.etcdhbase.core.kv;

import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Arrays;
import java.util.Objects;

/** Incremental comparison over records delivered from one protected base view. */
public final class CompareEvaluator {
  private final CompareCondition condition;
  private final byte[] expectedValue;
  private boolean sawLiveRecord;
  private boolean matched = true;
  private boolean failed;
  private boolean finished;

  public CompareEvaluator(CompareCondition condition) {
    this.condition = Objects.requireNonNull(condition, "condition");
    this.expectedValue = condition.value();
  }

  /** Accepts a record without retaining it; the caller owns the record's memory lease. */
  public void accept(StoredRecord record) {
    requireActive();
    try {
      Objects.requireNonNull(record, "record");
      if (!condition.range().contains(record.key())) {
        throw new IllegalArgumentException("comparison record is outside its requested range");
      }
      if (record.metadata().state() == StorageMetadata.State.TOMBSTONE) return;
      int order = switch (condition.target()) {
        case VERSION -> Long.compare(record.metadata().version(), condition.number());
        case CREATE -> Long.compare(record.metadata().createRevision(), condition.number());
        case MOD -> Long.compare(record.metadata().modificationRevision(), condition.number());
        case LEASE -> Long.compare(record.metadata().leaseId(), condition.number());
        case VALUE -> Arrays.compareUnsigned(record.value().orElseThrow(
            () -> new IllegalStateException("VALUE comparison needs a complete record")), expectedValue);
        case UNRECOGNIZED -> 0;
      };
      sawLiveRecord = true;
      matched &= test(order);
    } catch (RuntimeException failure) {
      failed = true;
      throw failure;
    }
  }

  /** Call only after exhausting the range; backend failures must be propagated by the caller. */
  public boolean finish() {
    requireActive();
    finished = true;
    if (sawLiveRecord) return matched;
    if (condition.target() == CompareCondition.Target.VALUE) return false;
    return test(condition.target() == CompareCondition.Target.UNRECOGNIZED
        ? 0 : Long.compare(0, condition.number()));
  }

  private boolean test(int order) {
    return switch (condition.relation()) {
      case EQUAL -> order == 0;
      case NOT_EQUAL -> order != 0;
      case GREATER -> order > 0;
      case LESS -> order < 0;
      case UNRECOGNIZED -> true;
    };
  }

  private void requireActive() {
    if (failed || finished) throw new IllegalStateException("comparison is no longer active");
  }
}
