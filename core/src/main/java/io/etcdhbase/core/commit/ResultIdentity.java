package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Immutable request and preparation identity to which a durable result belongs. */
public record ResultIdentity(InvocationId invocationId, TransactionId transactionId,
    ByteKey requestDigest, ByteKey authorizationScopeDigest, long ownerEpoch,
    long baseSequence, long baseRevision) {
  public ResultIdentity {
    Objects.requireNonNull(invocationId, "invocationId");
    Objects.requireNonNull(transactionId, "transactionId");
    Objects.requireNonNull(requestDigest, "requestDigest");
    Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
    if (requestDigest.length() != 32 || authorizationScopeDigest.length() != 32
        || ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0) {
      throw new IllegalArgumentException("invalid result identity");
    }
  }

  public static ResultIdentity from(InvocationBinding binding) {
    Objects.requireNonNull(binding, "binding");
    return new ResultIdentity(binding.invocationId(), binding.transactionId(),
        binding.requestDigest(), binding.authorizationScopeDigest(), binding.preparedOwnerEpoch(),
        binding.baseSequence(), binding.baseRevision());
  }

  public boolean matches(InvocationBinding binding) {
    return equals(from(binding));
  }

  void writeTo(DataOutputStream output) throws IOException {
    output.write(invocationId.bytes());
    output.write(transactionId.bytes());
    output.write(requestDigest.bytes());
    output.write(authorizationScopeDigest.bytes());
    output.writeLong(ownerEpoch);
    output.writeLong(baseSequence);
    output.writeLong(baseRevision);
  }
}
