package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Secret-free, bounded internal request envelope for one owner-forwarding attempt. */
public record MutationForwardRequest(
    String tableGroupId,
    InvocationId invocationId,
    ByteKey requestDigest,
    ByteKey authorizationScopeDigest,
    ByteKey authorizationContextReference,
    long expectedOwnerEpoch,
    long remainingTimeoutNanos,
    int hopCount,
    ByteKey operationBytes) {
  public static final int MAX_GROUP_ID_BYTES = 128;
  public static final int MAX_AUTH_REFERENCE_BYTES = 4096;
  public static final int MAX_OPERATION_BYTES = 4 * 1024 * 1024;
  public static final long MAX_TIMEOUT_NANOS = 5L * 60 * 1_000_000_000;
  public static final int MAX_HOPS = 1;

  public MutationForwardRequest {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(invocationId, "invocationId");
    requireDigest(requestDigest, "requestDigest");
    requireDigest(authorizationScopeDigest, "authorizationScopeDigest");
    Objects.requireNonNull(authorizationContextReference, "authorizationContextReference");
    Objects.requireNonNull(operationBytes, "operationBytes");
    if (!tableGroupId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
        || tableGroupId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_GROUP_ID_BYTES
        || authorizationContextReference.length() == 0
        || authorizationContextReference.length() > MAX_AUTH_REFERENCE_BYTES
        || expectedOwnerEpoch <= 0 || remainingTimeoutNanos <= 0
        || remainingTimeoutNanos > MAX_TIMEOUT_NANOS || hopCount < 0 || hopCount > MAX_HOPS
        || operationBytes.length() == 0 || operationBytes.length() > MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid bounded mutation forward request");
    }
    if (!requestDigest.equals(digest(tableGroupId, operationBytes))) {
      throw new IllegalArgumentException("mutation request digest does not match its canonical bytes");
    }
  }

  public MutationForwardRequest forwardedOnce(long ownerEpoch, long remainingNanos) {
    if (hopCount >= MAX_HOPS) throw new IllegalStateException("mutation forward hop limit exceeded");
    return new MutationForwardRequest(tableGroupId, invocationId, requestDigest,
        authorizationScopeDigest, authorizationContextReference, ownerEpoch, remainingNanos,
        hopCount + 1, operationBytes);
  }

  private static void requireDigest(ByteKey value, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != 32) throw new IllegalArgumentException(name + " must be SHA-256");
  }

  /** Digest over an unambiguous table-group and canonical operation envelope. */
  public static ByteKey digest(String tableGroupId, ByteKey operationBytes) {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(operationBytes, "operationBytes");
    byte[] group = tableGroupId.getBytes(StandardCharsets.UTF_8);
    if (group.length == 0 || group.length > MAX_GROUP_ID_BYTES
        || operationBytes.length() == 0 || operationBytes.length() > MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("mutation digest input exceeds its byte budget");
    }
    byte[] operation = operationBytes.bytes();
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(group.length).array());
      digest.update(group);
      digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(operation.length).array());
      digest.update(operation);
      return ByteKey.copyOf(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("the JDK must provide SHA-256", impossible);
    }
  }
}
