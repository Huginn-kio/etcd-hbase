package io.etcdhbase.grpc.protocol;

import com.google.protobuf.CodedOutputStream;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import java.util.Objects;

/** Counts the fixed etcd v3.5 no-auth internal Txn envelope, including its request ID. */
public final class TxnProtocolLimit {
  private TxnProtocolLimit() {}

  public static int encodedSize(Rpc.TxnRequest request, long invocationId) {
    Objects.requireNonNull(request, "request");
    int messageBytes = request.getSerializedSize();
    int headerBytes = invocationId == 0 ? 0 : CodedOutputStream.computeUInt64Size(1, invocationId);
    return Math.addExact(
        Math.addExact(CodedOutputStream.computeTagSize(6),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(messageBytes), messageBytes)),
        Math.addExact(CodedOutputStream.computeTagSize(100),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(headerBytes), headerBytes)));
  }

  public static void requireWithin(Rpc.TxnRequest request, long invocationId, int maximumBytes) {
    if (maximumBytes < 1) throw new IllegalArgumentException("protocol request limit must be positive");
    if (encodedSize(request, invocationId) > maximumBytes) {
      throw new KvFailure(KvFailure.Reason.REQUEST_TOO_LARGE);
    }
  }
}
