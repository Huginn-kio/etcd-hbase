package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import org.junit.jupiter.api.Test;

class DeleteProtocolLimitTest {
  @Test
  void matchesPinnedNativeEnvelopeSizesAndRejectsOnlyOverTheLimit() {
    long[] ids = {1L, 1L << 48, Long.MIN_VALUE};
    int[][] vectors = {{0, 21, 27, 30}, {127, 151, 157, 160},
        {128, 153, 159, 162}, {16384, 16411, 16417, 16420}};
    for (int[] vector : vectors) {
      var request = Rpc.DeleteRangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("m3-probe-key"))
          .setRangeEnd(ByteString.copyFrom(new byte[vector[0]])).build();
      for (int index = 0; index < ids.length; index++) {
        long id = ids[index];
        int size = DeleteProtocolLimit.encodedSize(request, id);
        assertEquals(vector[index + 1], size);
        DeleteProtocolLimit.requireWithin(request, id, size);
        assertEquals(KvFailure.Reason.REQUEST_TOO_LARGE, assertThrows(KvFailure.class,
            () -> DeleteProtocolLimit.requireWithin(request, id, size - 1)).reason());
      }
    }
  }
}
