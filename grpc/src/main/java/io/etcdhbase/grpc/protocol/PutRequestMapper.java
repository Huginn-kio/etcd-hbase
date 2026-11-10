package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.kv.PutCommand;
import java.util.Objects;

/** Converts the upstream wire request to the transport-independent Put command. */
public final class PutRequestMapper {
  private PutRequestMapper() {}

  public static PutCommand fromWire(Rpc.PutRequest request) {
    Objects.requireNonNull(request, "request");
    return new PutCommand(ByteKey.copyOf(request.getKey().toByteArray()),
        request.getValue().toByteArray(), request.getLease(), request.getPrevKv(),
        request.getIgnoreValue(), request.getIgnoreLease());
  }
}
