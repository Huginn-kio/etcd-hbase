package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import mvccpb.Kv;

/** Projects a recovered Put result with the serving instance's actual header identities. */
public final class PutResponseMapper {
  private PutResponseMapper() {}

  /** The logical result must be decoded from the retained bytes of this resolved invocation. */
  public static Rpc.PutResponse toWire(PutCommand command, PutResult result,
      InvocationResultResolver.ResolvedResult resolved, long clusterId, long memberId) {
    Objects.requireNonNull(resolved, "resolved");
    return project(command, result, resolved.decision(), clusterId, memberId);
  }

  static Rpc.PutResponse project(PutCommand command, PutResult result, CommitDecision decision,
      long clusterId, long memberId) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(decision, "decision");
    if (clusterId == 0 || memberId == 0 || !decision.containsKeyChanges()
        || result.revision() != decision.revision() || !result.key().equals(command.key())
        || !command.previousRequested() && result.previous().isPresent()) {
      throw new IllegalArgumentException("Put result differs from its request, decision, or serving identity");
    }
    var builder = Rpc.PutResponse.newBuilder().setHeader(
        ResponseHeaderFactory.create(clusterId, memberId, decision.revision(), 0));
    result.previous().ifPresent(previous -> builder.setPrevKv(project(previous)));
    return builder.build();
  }

  private static Kv.KeyValue project(StoredRecord record) {
    var metadata = record.metadata();
    return Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(record.key().bytes()))
        .setValue(ByteString.copyFrom(record.value().orElseThrow()))
        .setCreateRevision(metadata.createRevision()).setModRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLease(metadata.leaseId()).build();
  }
}
