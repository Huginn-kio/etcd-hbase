package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Arrays;
import java.util.Objects;

/** Distinguishes tagged Txn/Delete payloads from existing raw protobuf Put payloads. */
public final class MutationOperationEnvelope {
  // A zero protobuf tag cannot start a valid raw PutRequest, so old payloads cannot collide.
  private static final byte[] TXN_V1 = {0, 'T', 'X', 1};
  private static final byte[] DELETE_V1 = {0, 'D', 'L', 1};

  public enum Kind { PUT, TRANSACTION, DELETE_RANGE }

  private MutationOperationEnvelope() {}

  public static ByteKey transaction(byte[] payload) {
    return tagged(TXN_V1, payload);
  }

  public static ByteKey deleteRange(byte[] payload) {
    return tagged(DELETE_V1, payload);
  }

  private static ByteKey tagged(byte[] prefix, byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MutationForwardRequest.MAX_OPERATION_BYTES - prefix.length) {
      throw new IllegalArgumentException("mutation operation exceeds its byte bound");
    }
    byte[] encoded = Arrays.copyOf(prefix, prefix.length + payload.length);
    System.arraycopy(payload, 0, encoded, prefix.length, payload.length);
    return ByteKey.copyOf(encoded);
  }

  public static boolean isTransaction(ByteKey operation) {
    return kind(operation) == Kind.TRANSACTION;
  }

  public static Kind kind(ByteKey operation) {
    Objects.requireNonNull(operation, "operation");
    byte[] bytes = operation.bytes();
    if (bytes.length == 0) throw new IllegalArgumentException("empty mutation operation");
    if (bytes[0] != 0) return Kind.PUT;
    if (bytes.length < TXN_V1.length || bytes.length > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("unsupported mutation operation envelope");
    }
    byte[] prefix = Arrays.copyOf(bytes, TXN_V1.length);
    if (Arrays.equals(TXN_V1, prefix)) return Kind.TRANSACTION;
    if (Arrays.equals(DELETE_V1, prefix)) return Kind.DELETE_RANGE;
    throw new IllegalArgumentException("unsupported mutation operation envelope");
  }

  public static byte[] transactionBytes(ByteKey operation) {
    if (!isTransaction(operation)) throw new IllegalArgumentException("operation is not a transaction");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, TXN_V1.length, bytes.length);
  }

  public static byte[] deleteRangeBytes(ByteKey operation) {
    if (kind(operation) != Kind.DELETE_RANGE) throw new IllegalArgumentException("operation is not a delete");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, DELETE_V1.length, bytes.length);
  }
}
