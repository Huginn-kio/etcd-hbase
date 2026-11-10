package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import mvccpb.Kv;

/** Projects a verified transaction tree with the actual serving member's outer header. */
public final class TxnResponseMapper {
  private TxnResponseMapper() {}

  public static Rpc.TxnResponse toWire(TxnCommand command, ResultTree tree,
      long revision, boolean changed, long clusterId, long memberId) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(tree, "tree");
    if (clusterId == 0 || memberId == 0 || revision <= 0
        || !(tree.root() instanceof ResultNode.Txn txn) || !txn.header().present()
        || txn.header().revision() != revision) {
      throw new IllegalArgumentException("transaction result or serving identity differs");
    }
    boolean changes = containsSelectedMutation(command, txn);
    if (changes != changed) throw new IllegalArgumentException("transaction publication differs from its result");
    long baseRevision = revision - (changes ? 1 : 0);
    if (baseRevision <= 0) throw new IllegalArgumentException("invalid transaction base revision");
    var response = Rpc.TxnResponse.newBuilder().setSucceeded(txn.succeeded())
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, revision, 0));
    appendChildren(command, txn, new RevisionCursor(baseRevision, revision), response);
    return response.build();
  }

  private static boolean containsSelectedMutation(TxnCommand command, ResultNode.Txn result) {
    List<TxnCommand.Operation> selected = result.succeeded() ? command.success() : command.failure();
    if (selected.size() != result.responses().size()) {
      throw new IllegalArgumentException("transaction result does not match its selected branch");
    }
    boolean changed = false;
    for (int index = 0; index < selected.size(); index++) {
      TxnCommand.Operation operation = selected.get(index);
      ResultNode child = result.responses().get(index);
      if (operation instanceof TxnCommand.Put) changed = true;
      else if (operation instanceof TxnCommand.Delete) {
        if (!(child instanceof ResultNode.Delete deleted)) {
          throw new IllegalArgumentException("delete response differs from its request");
        }
        changed |= deleted.deleted() > 0;
      }
      else if (operation instanceof TxnCommand.Nested nested) {
        if (!(child instanceof ResultNode.Txn branch)) {
          throw new IllegalArgumentException("nested transaction response differs from its request");
        }
        changed |= containsSelectedMutation(nested.command(), branch);
      }
    }
    return changed;
  }

  private static void appendChildren(TxnCommand command, ResultNode.Txn txn,
      RevisionCursor revisions, Rpc.TxnResponse.Builder response) {
    List<TxnCommand.Operation> selected = txn.succeeded() ? command.success() : command.failure();
    if (selected.size() != txn.responses().size()) {
      throw new IllegalArgumentException("transaction result does not match its selected branch");
    }
    for (int index = 0; index < selected.size(); index++) {
      response.addResponses(project(selected.get(index), txn.responses().get(index), revisions));
    }
  }

  private static Rpc.ResponseOp project(TxnCommand.Operation operation, ResultNode result,
      RevisionCursor revisions) {
    if (operation instanceof TxnCommand.Nested nested && result instanceof ResultNode.Txn child) {
      return projectNested(nested, child, revisions);
    }
    if (operation instanceof TxnCommand.Put
        || operation instanceof TxnCommand.Delete && result instanceof ResultNode.Delete deleted && deleted.deleted() > 0) {
      revisions.current = revisions.written;
    }
    if (!result.header().present() || result.header().revision() != revisions.current) {
      throw new IllegalArgumentException("transaction child response has an invalid revision");
    }
    Rpc.ResponseHeader header = Rpc.ResponseHeader.newBuilder().setRevision(result.header().revision()).build();
    if (operation instanceof TxnCommand.PointRange point && result instanceof ResultNode.Range range) {
      return projectRange(point, range, header);
    }
    if (operation instanceof TxnCommand.Put put && result instanceof ResultNode.Put written) {
      return projectPut(put, written, header);
    }
    if (operation instanceof TxnCommand.Delete delete && result instanceof ResultNode.Delete deleted) {
      return projectDelete(delete, deleted, header);
    }
    throw new IllegalArgumentException("transaction response operation differs from its request");
  }

  private static Rpc.ResponseOp projectNested(TxnCommand.Nested nested, ResultNode.Txn child, RevisionCursor revisions) {
    if (!child.header().present() || child.header().revision() != 0) {
      throw new IllegalArgumentException("nested transaction header differs from the native response");
    }
    var response = Rpc.TxnResponse.newBuilder().setHeader(Rpc.ResponseHeader.getDefaultInstance())
        .setSucceeded(child.succeeded());
    appendChildren(nested.command(), child, revisions, response);
    return Rpc.ResponseOp.newBuilder().setResponseTxn(response).build();
  }

  private static Rpc.ResponseOp projectRange(TxnCommand.PointRange point, ResultNode.Range range,
      Rpc.ResponseHeader header) {
    if (range.more() || range.records().size() > 1 || range.count() != range.records().size()
        || range.records().stream().anyMatch(record -> !record.key().equals(point.key()) || !record.hasValue())) {
      throw new IllegalArgumentException("point response differs from its request");
    }
    var response = Rpc.RangeResponse.newBuilder().setHeader(header).setCount(range.count());
    range.records().forEach(record -> response.addKvs(project(record)));
    return Rpc.ResponseOp.newBuilder().setResponseRange(response).build();
  }

  private static Rpc.ResponseOp projectPut(TxnCommand.Put put, ResultNode.Put written, Rpc.ResponseHeader header) {
    if (!put.command().key().equals(written.key())
        || !put.command().previousRequested() && written.previous().isPresent()) {
      throw new IllegalArgumentException("Put response differs from its request");
    }
    var response = Rpc.PutResponse.newBuilder().setHeader(header);
    written.previous().ifPresent(previous -> response.setPrevKv(project(previous)));
    return Rpc.ResponseOp.newBuilder().setResponsePut(response).build();
  }

  private static Rpc.ResponseOp projectDelete(TxnCommand.Delete delete, ResultNode.Delete deleted,
      Rpc.ResponseHeader header) {
    if ((!delete.previousRequested() && !deleted.previous().isEmpty())
        || (delete.previousRequested() && deleted.previous().size() != deleted.deleted())) {
      throw new IllegalArgumentException("delete previous values differ from its request");
    }
    ByteKey previous = null;
    for (StoredRecord record : deleted.previous()) {
      if (!delete.range().contains(record.key()) || previous != null && previous.compareTo(record.key()) >= 0) {
        throw new IllegalArgumentException("delete previous values are unordered or outside the range");
      }
      previous = record.key();
    }
    var response = Rpc.DeleteRangeResponse.newBuilder().setHeader(header).setDeleted(deleted.deleted());
    deleted.previous().forEach(record -> response.addPrevKvs(project(record)));
    return Rpc.ResponseOp.newBuilder().setResponseDeleteRange(response).build();
  }

  private static final class RevisionCursor {
    private long current;
    private final long written;
    private RevisionCursor(long current, long written) {
      this.current = current;
      this.written = written;
    }
  }

  private static Kv.KeyValue project(StoredRecord record) {
    var metadata = record.metadata();
    return Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(record.key().bytes()))
        .setValue(ByteString.copyFrom(record.value().orElseThrow()))
        .setCreateRevision(metadata.createRevision()).setModRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLease(metadata.leaseId()).build();
  }
}
