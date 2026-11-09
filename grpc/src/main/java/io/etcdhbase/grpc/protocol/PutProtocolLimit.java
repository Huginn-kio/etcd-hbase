package io.etcdhbase.grpc.protocol;

import com.google.protobuf.CodedOutputStream;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import java.util.Objects;

/** Fixed etcd v3.5 internal Put envelope size for the no-authentication profile. */
public final class PutProtocolLimit {
  public static final int DEFAULT_ETCD_MAX_REQUEST_BYTES = 3 * 1024 * 1024 / 2;

  private PutProtocolLimit() {}

  /**
   * Counts the high 64 bits of this invocation ID as the request-header ID. Etcd's internal
   * request ID is a uint64 whose varint width depends on the member and invocation; no Raft ID is
   * sent to clients.
   * The no-auth profile has no username or auth-revision fields in this envelope. A future
   * authenticated profile must include its actual authentication header before using this check.
   */
  public static int encodedSize(Rpc.PutRequest request, long invocationId) {
    Objects.requireNonNull(request, "request");
    int messageBytes = request.getSerializedSize();
    int headerBytes = invocationId == 0 ? 0 : CodedOutputStream.computeUInt64Size(1, invocationId);
    return Math.addExact(
        Math.addExact(CodedOutputStream.computeTagSize(4),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(messageBytes), messageBytes)),
        Math.addExact(CodedOutputStream.computeTagSize(100),
            Math.addExact(CodedOutputStream.computeUInt32SizeNoTag(headerBytes), headerBytes)));
  }

  public static void requireWithin(Rpc.PutRequest request, long invocationId, int maximumBytes) {
    if (maximumBytes < 1) throw new IllegalArgumentException("protocol request limit must be positive");
    if (encodedSize(request, invocationId) > maximumBytes) {
      throw new KvFailure(KvFailure.Reason.REQUEST_TOO_LARGE);
    }
  }
}
