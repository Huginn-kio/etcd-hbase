package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** A bounded transaction request tree, after wire-level field validation. */
public record TxnCommand(List<CompareCondition> compares, List<Operation> success,
    List<Operation> failure) {
  public sealed interface Operation permits Range, Put, Delete, Nested, Unsupported {}

  public record Range(RangeCommand command) implements Operation {
    public Range { Objects.requireNonNull(command, "command"); }
  }

  public record Put(PutCommand command) implements Operation {
    public Put { Objects.requireNonNull(command, "command"); }
  }

  public record Delete(KeyRange range, boolean previousRequested) implements Operation {
    public Delete { Objects.requireNonNull(range, "range"); }
  }

  public record Nested(TxnCommand command) implements Operation {
    public Nested { Objects.requireNonNull(command, "command"); }
  }

  /** A statically valid operation whose dynamic behavior belongs to a later task. */
  public record Unsupported(boolean mayWrite) implements Operation {}

  public TxnCommand {
    Objects.requireNonNull(compares, "compares");
    Objects.requireNonNull(success, "success");
    Objects.requireNonNull(failure, "failure");
    if (compares.size() > ResultNode.MAX_OPERATIONS || success.size() > ResultNode.MAX_OPERATIONS
        || failure.size() > ResultNode.MAX_OPERATIONS) {
      throw new KvFailure(KvFailure.Reason.TOO_MANY_OPS);
    }
    compares = List.copyOf(compares);
    success = List.copyOf(success);
    failure = List.copyOf(failure);
    requireDistinctWrites(success);
    requireDistinctWrites(failure);
  }

  /** Both branches matter when choosing the read-only or owner/invocation route. */
  public boolean mayWrite() {
    return success.stream().anyMatch(TxnCommand::mayWrite)
        || failure.stream().anyMatch(TxnCommand::mayWrite);
  }

  private static boolean mayWrite(Operation operation) {
    return operation instanceof Put || operation instanceof Delete
        || operation instanceof Nested nested && nested.command().mayWrite()
        || operation instanceof Unsupported unsupported && unsupported.mayWrite();
  }

  private static void requireDistinctWrites(List<Operation> operations) {
    var keys = new HashSet<ByteKey>();
    for (Operation operation : operations) {
      if (operation instanceof Put put && !keys.add(put.command().key())) {
        throw new KvFailure(KvFailure.Reason.DUPLICATE_KEY);
      }
    }
  }
}
