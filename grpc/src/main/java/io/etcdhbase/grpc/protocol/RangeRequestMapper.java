package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.RangeCommand;
import java.util.Objects;
import java.util.Optional;

/** Decodes an etcd Range request without changing its option values or revision context. */
public final class RangeRequestMapper {
  private RangeRequestMapper() {}

  public static RangeCommand fromWire(Rpc.RangeRequest request, RangeCommand.Context context) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(context, "context");
    return new RangeCommand(range(request.getKey(), request.getRangeEnd()), context,
        request.getRevision(), request.getLimit(),
        new RangeCommand.Sort(request.getSortOrderValue(), request.getSortTargetValue()),
        new RangeCommand.RevisionFilter(request.getMinModRevision(), request.getMaxModRevision(),
            request.getMinCreateRevision(), request.getMaxCreateRevision()),
        request.getSerializable(), request.getKeysOnly(), request.getCountOnly());
  }

  /** The single-zero endpoint means unbounded; two single-zero endpoints mean all keys. */
  static KeyRange range(ByteString key, ByteString end) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(end, "end");
    if (key.isEmpty()) throw new KvFailure(KvFailure.Reason.EMPTY_KEY);
    ByteKey start = ByteKey.copyOf(key.toByteArray());
    if (end.isEmpty()) return KeyRange.point(start);
    if (end.size() == 1 && end.byteAt(0) == 0) {
      return key.size() == 1 && key.byteAt(0) == 0 ? KeyRange.all()
          : KeyRange.interval(Optional.of(start), Optional.empty());
    }
    return KeyRange.between(start, ByteKey.copyOf(end.toByteArray()));
  }
}
