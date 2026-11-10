package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import mvccpb.Kv;

/** Projects a verified flat transaction result with the actual serving member's outer header. */
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
    List<TxnCommand.Operation> selected = txn.succeeded() ? command.success() : command.failure();
    if (selected.size() != txn.responses().size()) {
      throw new IllegalArgumentException("transaction result does not match its selected branch");
    }
    boolean changes = selected.stream().anyMatch(TxnCommand.Put.class::isInstance);
    if (changes != changed) throw new IllegalArgumentException("transaction publication differs from its result");
    long currentRevision = revision - (changes ? 1 : 0);
    if (currentRevision <= 0) throw new IllegalArgumentException("invalid transaction base revision");
    var response = Rpc.TxnResponse.newBuilder().setSucceeded(txn.succeeded())
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, revision, 0));
    for (int index = 0; index < selected.size(); index++) {
      TxnCommand.Operation operation = selected.get(index);
      if (operation instanceof TxnCommand.Put) currentRevision = revision;
      response.addResponses(project(operation, txn.responses().get(index), currentRevision));
    }
    return response.build();
  }

  private static Rpc.ResponseOp project(TxnCommand.Operation operation, ResultNode result, long revision) {
    if (!result.header().present() || result.header().revision() != revision) {
      throw new IllegalArgumentException("transaction child response has an invalid revision");
    }
    Rpc.ResponseHeader header = Rpc.ResponseHeader.newBuilder().setRevision(result.header().revision()).build();
    if (operation instanceof TxnCommand.PointRange point && result instanceof ResultNode.Range range) {
      if (range.more() || range.records().size() > 1 || range.count() != range.records().size()
          || range.records().stream().anyMatch(record -> !record.key().equals(point.key()) || !record.hasValue())) {
        throw new IllegalArgumentException("point response differs from its request");
      }
      var response = Rpc.RangeResponse.newBuilder().setHeader(header).setCount(range.count());
      range.records().forEach(record -> response.addKvs(project(record)));
      return Rpc.ResponseOp.newBuilder().setResponseRange(response).build();
    }
    if (operation instanceof TxnCommand.Put put && result instanceof ResultNode.Put written) {
      if (!put.command().key().equals(written.key())
          || !put.command().previousRequested() && written.previous().isPresent()) {
        throw new IllegalArgumentException("Put response differs from its request");
      }
      var response = Rpc.PutResponse.newBuilder().setHeader(header);
      written.previous().ifPresent(previous -> response.setPrevKv(project(previous)));
      return Rpc.ResponseOp.newBuilder().setResponsePut(response).build();
    }
    throw new IllegalArgumentException("transaction response operation differs from its request");
  }

  private static Kv.KeyValue project(StoredRecord record) {
    var metadata = record.metadata();
    return Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(record.key().bytes()))
        .setValue(ByteString.copyFrom(record.value().orElseThrow()))
        .setCreateRevision(metadata.createRevision()).setModRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLease(metadata.leaseId()).build();
  }
}
