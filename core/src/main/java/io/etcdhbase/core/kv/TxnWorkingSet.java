package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToIntFunction;

/** Small private write overlay for a flat transaction; it never modifies the backend directly. */
public final class TxnWorkingSet {
  private static final long FIXED_OPERATION_BYTES = 4096;
  private static final long COPY_ALLOWANCE = 16;

  private final InvocationBinding invocation;
  private final long maximumRetainedBytes;
  private final PutPlanner planner = new PutPlanner();
  private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
  private final List<PreparedEvent> events = new ArrayList<>();
  private long retainedBytes;

  /** The caller reserves this bound in its request ledger until all consumers have drained. */
  public TxnWorkingSet(InvocationBinding invocation, long maximumRetainedBytes) {
    this.invocation = Objects.requireNonNull(invocation, "invocation");
    if (invocation.state() != InvocationBinding.State.PREPARING || maximumRetainedBytes < 1
        || maximumRetainedBytes > ResultTree.MAX_RETAINED_BYTES) {
      throw new IllegalArgumentException("invalid transaction working-set bound or invocation");
    }
    this.maximumRetainedBytes = maximumRetainedBytes;
  }

  /** Returns a completed prior operation when present, otherwise the caller's fixed base record. */
  public Optional<StoredRecord> current(ByteKey key, Optional<StoredRecord> base) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(base, "base");
    if (base.filter(record -> !record.key().equals(key)).isPresent()) {
      throw new IllegalArgumentException("working-view base key differs from its request");
    }
    PreparedCandidate candidate = candidates.get(key);
    return candidate == null ? base : Optional.of(candidate.record());
  }

  /** Plans a unique Put without publishing it; failed planning leaves the overlay unchanged. */
  public PutResult put(PutCommand command, Optional<StoredRecord> base) throws BackendFailure {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(base, "base");
    if (candidates.containsKey(command.key())) throw new KvFailure(KvFailure.Reason.DUPLICATE_KEY);
    if (events.size() >= ResultNode.MAX_OPERATIONS) throw new KvFailure(KvFailure.Reason.TOO_MANY_OPS);
    long bytes = accountingBytes(command, base);
    if (bytes > maximumRetainedBytes - retainedBytes) {
      throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    }
    PutPlanner.Plan planned = planner.plan(command, invocation, base);
    PreparedEvent original = planned.event();
    PreparedEvent event = new PreparedEvent(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), events.size(), original.kind(),
        original.current(), original.previous());
    candidates.put(command.key(), planned.candidate());
    events.add(event);
    retainedBytes += bytes;
    return planned.result();
  }

  /** Candidates follow the manifest's bucket/key order; events keep mutation order. */
  public List<PreparedCandidate> candidates(ToIntFunction<ByteKey> bucket) {
    Objects.requireNonNull(bucket, "bucket");
    var ordered = new ArrayList<Bucketed>(candidates.size());
    for (PreparedCandidate candidate : candidates.values()) {
      int selected = bucket.applyAsInt(candidate.record().key());
      if (selected < 0 || selected >= 4096) throw new IllegalArgumentException("invalid candidate bucket");
      ordered.add(new Bucketed(selected, candidate));
    }
    ordered.sort(Comparator.comparingInt(Bucketed::bucket)
        .thenComparing(item -> item.candidate().record().key()));
    return ordered.stream().map(Bucketed::candidate).toList();
  }

  public List<PreparedEvent> events() { return List.copyOf(events); }
  public boolean hasChanges() { return !events.isEmpty(); }
  public long retainedBytes() { return retainedBytes; }

  private static long accountingBytes(PutCommand command, Optional<StoredRecord> base) {
    long data = Math.addExact(command.key().length(), command.valueLength());
    if (base.isPresent()) data = Math.addExact(data, base.orElseThrow().retainedBytes());
    return Math.addExact(FIXED_OPERATION_BYTES, Math.multiplyExact(COPY_ALLOWANCE, data));
  }

  private record Bucketed(int bucket, PreparedCandidate candidate) {}
}
