package io.etcdhbase.grpc.protocol;

import com.google.protobuf.CodedOutputStream;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import java.util.Objects;

/** Counts the fixed etcd v3.5 no-auth internal DeleteRange envelope and request ID. */
public final class DeleteProtocolLimit {
  private DeleteProtocolLimit() {}

  public static int encodedSize(Rpc.DeleteRangeRequest request, long invocationId) {
    Objects.requireNonNull(request, "request");
    int messageBytes = request.getSerializedSize();
    int headerBytes = invocationId == 0 ? 0 : CodedOutputStream.computeUInt64Size(1, invocationId);
    return Math.addExact(
        Math.addExact(CodedOutputStream.computeTagSize(5),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(messageBytes), messageBytes)),
        Math.addExact(CodedOutputStream.computeTagSize(100),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(headerBytes), headerBytes)));
  }

  public static void requireWithin(Rpc.DeleteRangeRequest request, long invocationId, int maximumBytes) {
    if (maximumBytes < 1) throw new IllegalArgumentException("protocol request limit must be positive");
    if (encodedSize(request, invocationId) > maximumBytes) {
      throw new KvFailure(KvFailure.Reason.REQUEST_TOO_LARGE);
    }
  }
}
