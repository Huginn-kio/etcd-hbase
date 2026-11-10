package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PutProtocolLimitTest {
  @Test
  void measuresPinnedEtcdInternalPutEnvelopeIncludingTheActualInvocationId() {
    // Generated with etcd v3.5.23 InternalRaftRequest.Size(), with authentication disabled.
    long[] ids = {1L, 1L << 48, Long.MIN_VALUE};
    int[][] vectors = {{0, 21, 27, 30}, {127, 151, 157, 160},
        {128, 153, 159, 162}, {16384, 16411, 16417, 16420},
        {1572828, 1572855, 1572861, 1572864}};
    for (int[] vector : vectors) {
      var request = request(vector[0]);
      for (int index = 0; index < ids.length; index++) {
        assertEquals(vector[index + 1], PutProtocolLimit.encodedSize(request, ids[index]));
      }
    }
  }

  @Test
  void separatesEtcdProtocolRejectionFromOtherResourceLimits() {
    var request = request(1572828);
    assertDoesNotThrow(() -> PutProtocolLimit.requireWithin(request, Long.MIN_VALUE, 1572864));
    var failure = assertThrows(KvFailure.class,
        () -> PutProtocolLimit.requireWithin(request, Long.MIN_VALUE, 1572863));
    assertEquals(KvFailure.Reason.REQUEST_TOO_LARGE, failure.reason());
    assertEquals("etcdserver: request is too large",
        ErrorProjection.project(failure).getStatus().getDescription());
    assertThrows(IllegalArgumentException.class,
        () -> PutProtocolLimit.requireWithin(request, 1, 0));
  }

  private static Rpc.PutRequest request(int valueLength) {
    byte[] value = new byte[valueLength];
    java.util.Arrays.fill(value, (byte) 'v');
    return Rpc.PutRequest.newBuilder()
        .setKey(ByteString.copyFrom("m3-probe-key", StandardCharsets.UTF_8))
        .setValue(ByteString.copyFrom(value)).build();
  }
}
