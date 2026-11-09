package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.kv.CompareCondition;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnCommand;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Validates both branches before mapping the supported transaction operations. */
public final class TxnRequestMapper {
  private static final int MAX_TREE_DEPTH = ResultTree.MAX_DEPTH;
  private static final int MAX_TREE_NODES = 4096;
  private TxnRequestMapper() {}

  public static TxnCommand fromWire(Rpc.TxnRequest request) {
    Objects.requireNonNull(request, "request");
    validate(request, ResultNode.MAX_OPERATIONS, 1, new int[] {0});
    writes(request.getSuccessList());
    writes(request.getFailureList());
    return command(request);
  }

  private static TxnCommand command(Rpc.TxnRequest request) {
    return new TxnCommand(request.getCompareList().stream().map(TxnRequestMapper::compare).toList(),
        operations(request.getSuccessList()), operations(request.getFailureList()));
  }

  /** Etcd v3.5 submits a Txn through Raft unless both top-level branches contain only Range. */
  public static boolean requiresNativeRaftLimit(Rpc.TxnRequest request) {
    Objects.requireNonNull(request, "request");
    return java.util.stream.Stream.concat(request.getSuccessList().stream(), request.getFailureList().stream())
        .anyMatch(operation -> operation.getRequestCase() != Rpc.RequestOp.RequestCase.REQUEST_RANGE);
  }

  private static void validate(Rpc.TxnRequest request, int remaining, int depth, int[] nodes) {
    int operations = Math.max(request.getCompareCount(),
        Math.max(request.getSuccessCount(), request.getFailureCount()));
    if (operations > remaining) throw new KvFailure(KvFailure.Reason.TOO_MANY_OPS);
    nodes[0] = Math.addExact(nodes[0], 1 + request.getCompareCount()
        + request.getSuccessCount() + request.getFailureCount());
    if (depth > MAX_TREE_DEPTH || nodes[0] > MAX_TREE_NODES) {
      throw new KvFailure(KvFailure.Reason.REQUEST_BUDGET_EXHAUSTED);
    }
    for (Rpc.Compare compare : request.getCompareList()) requireKey(compare.getKey());
    for (Rpc.RequestOp operation : request.getSuccessList()) validate(operation, remaining - operations, depth, nodes);
    for (Rpc.RequestOp operation : request.getFailureList()) validate(operation, remaining - operations, depth, nodes);
  }

  private static void validate(Rpc.RequestOp operation, int remaining, int depth, int[] nodes) {
    switch (operation.getRequestCase()) {
      case REQUEST_RANGE -> { requireKey(operation.getRequestRange().getKey()); requireLeafDepth(depth); }
      case REQUEST_PUT -> { PutRequestMapper.fromWire(operation.getRequestPut()); requireLeafDepth(depth); }
      case REQUEST_DELETE_RANGE -> { requireKey(operation.getRequestDeleteRange().getKey()); requireLeafDepth(depth); }
      case REQUEST_TXN -> validate(operation.getRequestTxn(), remaining, depth + 1, nodes);
      case REQUEST_NOT_SET -> throw new KvFailure(KvFailure.Reason.KEY_NOT_FOUND);
    }
  }

  private static void requireLeafDepth(int depth) {
    if (depth >= MAX_TREE_DEPTH) throw new KvFailure(KvFailure.Reason.REQUEST_BUDGET_EXHAUSTED);
  }

  private static List<TxnCommand.Operation> operations(List<Rpc.RequestOp> operations) {
    var mapped = new ArrayList<TxnCommand.Operation>(operations.size());
    for (Rpc.RequestOp operation : operations) {
      mapped.add(switch (operation.getRequestCase()) {
        case REQUEST_RANGE -> isPointRange(operation.getRequestRange())
            ? new TxnCommand.PointRange(key(operation.getRequestRange().getKey()),
                operation.getRequestRange().getRevision())
            : new TxnCommand.Unsupported(false);
        case REQUEST_PUT -> new TxnCommand.Put(PutRequestMapper.fromWire(operation.getRequestPut()));
        case REQUEST_DELETE_RANGE -> new TxnCommand.Delete(
            range(operation.getRequestDeleteRange().getKey(), operation.getRequestDeleteRange().getRangeEnd()),
            operation.getRequestDeleteRange().getPrevKv());
        case REQUEST_TXN -> new TxnCommand.Nested(command(operation.getRequestTxn()));
        case REQUEST_NOT_SET -> throw new KvFailure(KvFailure.Reason.KEY_NOT_FOUND);
      });
    }
    return List.copyOf(mapped);
  }

  private static boolean isPointRange(Rpc.RangeRequest range) {
    return range.getRangeEnd().isEmpty() && range.getLimit() == 0
        && range.getSortOrderValue() == 0 && range.getSortTargetValue() == 0
        && !range.getKeysOnly() && !range.getCountOnly() && range.getMinModRevision() == 0
        && range.getMaxModRevision() == 0 && range.getMinCreateRevision() == 0
        && range.getMaxCreateRevision() == 0;
  }

  private static CompareCondition compare(Rpc.Compare input) {
    CompareCondition.Relation relation = switch (input.getResult()) {
      case EQUAL -> CompareCondition.Relation.EQUAL;
      case NOT_EQUAL -> CompareCondition.Relation.NOT_EQUAL;
      case GREATER -> CompareCondition.Relation.GREATER;
      case LESS -> CompareCondition.Relation.LESS;
      case UNRECOGNIZED -> CompareCondition.Relation.UNRECOGNIZED;
    };
    KeyRange range = range(input.getKey(), input.getRangeEnd());
    return switch (input.getTarget()) {
      case VERSION -> CompareCondition.numeric(range, CompareCondition.Target.VERSION, relation, input.getVersion());
      case CREATE -> CompareCondition.numeric(range, CompareCondition.Target.CREATE, relation, input.getCreateRevision());
      case MOD -> CompareCondition.numeric(range, CompareCondition.Target.MOD, relation, input.getModRevision());
      case LEASE -> CompareCondition.numeric(range, CompareCondition.Target.LEASE, relation, input.getLease());
      case VALUE -> CompareCondition.value(range, relation, input.getValue().toByteArray());
      case UNRECOGNIZED -> CompareCondition.numeric(range, CompareCondition.Target.UNRECOGNIZED, relation, 0);
    };
  }

  private static KeyRange range(ByteString key, ByteString end) {
    ByteKey start = key(key);
    if (end.isEmpty()) return KeyRange.point(start);
    if (end.size() == 1 && end.byteAt(0) == 0) {
      return key.size() == 1 && key.byteAt(0) == 0 ? KeyRange.all()
          : KeyRange.interval(Optional.of(start), Optional.empty());
    }
    return KeyRange.between(start, key(end));
  }

  /** Mirrors the upstream potential-write overlap check, including mutually exclusive children. */
  private static WriteSet writes(List<Rpc.RequestOp> operations) {
    var deletes = new ArrayList<DeleteInterval>();
    for (Rpc.RequestOp operation : operations) {
      if (operation.getRequestCase() == Rpc.RequestOp.RequestCase.REQUEST_DELETE_RANGE) {
        var delete = operation.getRequestDeleteRange();
        deletes.add(new DeleteInterval(key(delete.getKey()), key(delete.getRangeEnd()), delete.getRangeEnd().isEmpty()));
      }
    }
    var puts = new HashSet<ByteKey>();
    for (Rpc.RequestOp operation : operations) {
      if (operation.getRequestCase() != Rpc.RequestOp.RequestCase.REQUEST_TXN) continue;
      var nested = operation.getRequestTxn();
      WriteSet thenWrites = writes(nested.getSuccessList());
      WriteSet elseWrites = writes(nested.getFailureList());
      for (ByteKey key : thenWrites.puts()) {
        if (puts.contains(key) || intersects(deletes, key)) throw duplicate();
        puts.add(key);
      }
      for (ByteKey key : elseWrites.puts()) {
        if ((puts.contains(key) && !thenWrites.puts().contains(key)) || intersects(deletes, key)) throw duplicate();
        puts.add(key);
      }
      deletes.addAll(thenWrites.deletes());
      deletes.addAll(elseWrites.deletes());
    }
    for (Rpc.RequestOp operation : operations) {
      if (operation.getRequestCase() != Rpc.RequestOp.RequestCase.REQUEST_PUT) continue;
      ByteKey key = key(operation.getRequestPut().getKey());
      if (!puts.add(key) || intersects(deletes, key)) throw duplicate();
    }
    return new WriteSet(puts, deletes);
  }

  private static boolean intersects(List<DeleteInterval> intervals, ByteKey key) {
    return intervals.stream().anyMatch(interval -> interval.contains(key));
  }

  private static void requireKey(ByteString key) {
    if (key.isEmpty()) throw new KvFailure(KvFailure.Reason.EMPTY_KEY);
  }
  private static ByteKey key(ByteString value) { return ByteKey.copyOf(value.toByteArray()); }
  private static KvFailure duplicate() { return new KvFailure(KvFailure.Reason.DUPLICATE_KEY); }

  private record WriteSet(Set<ByteKey> puts, List<DeleteInterval> deletes) {}
  private record DeleteInterval(ByteKey start, ByteKey end, boolean point) {
    private boolean contains(ByteKey key) {
      return point ? start.equals(key) : start.compareTo(key) <= 0 && key.compareTo(end) < 0;
    }
  }
}
