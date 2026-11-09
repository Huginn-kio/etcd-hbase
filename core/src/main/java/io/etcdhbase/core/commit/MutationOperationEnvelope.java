package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Arrays;
import java.util.Objects;

/** Distinguishes versioned Txn payloads from the existing raw, valid protobuf Put payloads. */
public final class MutationOperationEnvelope {
  // A zero protobuf tag cannot start a valid raw PutRequest, so old payloads cannot collide.
  private static final byte[] TXN_V1 = {0, 'T', 'X', 1};

  private MutationOperationEnvelope() {}

  public static ByteKey transaction(byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MutationForwardRequest.MAX_OPERATION_BYTES - TXN_V1.length) {
      throw new IllegalArgumentException("transaction operation exceeds its byte bound");
    }
    byte[] encoded = Arrays.copyOf(TXN_V1, TXN_V1.length + payload.length);
    System.arraycopy(payload, 0, encoded, TXN_V1.length, payload.length);
    return ByteKey.copyOf(encoded);
  }

  public static boolean isTransaction(ByteKey operation) {
    Objects.requireNonNull(operation, "operation");
    byte[] bytes = operation.bytes();
    if (bytes.length == 0) throw new IllegalArgumentException("empty mutation operation");
    if (bytes[0] != 0) return false;
    if (bytes.length < TXN_V1.length || bytes.length > MutationForwardRequest.MAX_OPERATION_BYTES
        || !Arrays.equals(TXN_V1, Arrays.copyOf(bytes, TXN_V1.length))) {
      throw new IllegalArgumentException("unsupported mutation operation envelope");
    }
    return true;
  }

  public static byte[] transactionBytes(ByteKey operation) {
    if (!isTransaction(operation)) throw new IllegalArgumentException("operation is not a transaction");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, TXN_V1.length, bytes.length);
  }
}
