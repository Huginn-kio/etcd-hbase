package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import org.junit.jupiter.api.Test;

class MutationForwardRequestTest {
  @Test
  void constructorValidatesDigestAtTheEnvelopeBoundaryAndKeepsBytesImmutable() {
    byte[] operation = {1, 2, 3};
    ByteKey operationBytes = ByteKey.copyOf(operation);
    ByteKey digest = MutationForwardRequest.digest("group-a", operationBytes);
    var request = new MutationForwardRequest("group-a", InvocationId.create(), digest,
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[] {7}), 1, 1, 0,
        operationBytes);
    operation[0] = 99;

    assertArrayEquals(new byte[] {1, 2, 3}, request.operationBytes().bytes());
    assertThrows(IllegalArgumentException.class, () -> new MutationForwardRequest("group-a",
        InvocationId.create(), ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[] {7}), 1, 1, 0, operationBytes));
  }
}
