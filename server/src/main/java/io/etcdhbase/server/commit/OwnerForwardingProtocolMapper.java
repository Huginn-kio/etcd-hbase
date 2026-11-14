package io.etcdhbase.server.commit;

import com.google.protobuf.ByteString;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.grpc.internal.v1.ForwardMutationRequest;
import io.etcdhbase.grpc.internal.v1.ForwardMutationResponse;
import java.util.Objects;

/** Converts the bounded J21 owner envelope without trusting protobuf defaults. */
final class OwnerForwardingProtocolMapper {
  private OwnerForwardingProtocolMapper() {}

  static ForwardMutationRequest toWire(MutationForwardRequest request) {
    Objects.requireNonNull(request, "request");
    return ForwardMutationRequest.newBuilder()
        .setTableGroupId(request.tableGroupId())
        .setInvocationId(ByteString.copyFrom(request.invocationId().bytes()))
        .setRequestDigest(ByteString.copyFrom(request.requestDigest().bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(request.authorizationScopeDigest().bytes()))
        .setAuthorizationContextReference(ByteString.copyFrom(request.authorizationContextReference().bytes()))
        .setExpectedOwnerEpoch(request.expectedOwnerEpoch())
        .setRemainingTimeoutNanos(request.remainingTimeoutNanos())
        .setHopCount(request.hopCount())
        .setOperationBytes(ByteString.copyFrom(request.operationBytes().bytes()))
        .build();
  }

  static MutationForwardRequest fromWire(ForwardMutationRequest request) {
    Objects.requireNonNull(request, "request");
    return new MutationForwardRequest(request.getTableGroupId(),
        InvocationId.fromBytes(request.getInvocationId().toByteArray()),
        ByteKey.copyOf(request.getRequestDigest().toByteArray()),
        ByteKey.copyOf(request.getAuthorizationScopeDigest().toByteArray()),
        ByteKey.copyOf(request.getAuthorizationContextReference().toByteArray()),
        request.getExpectedOwnerEpoch(), request.getRemainingTimeoutNanos(), request.getHopCount(),
        ByteKey.copyOf(request.getOperationBytes().toByteArray()));
  }

  static ForwardMutationResponse toWire(OwnerForwarding.Reply reply) {
    Objects.requireNonNull(reply, "reply");
    var response = ForwardMutationResponse.newBuilder();
    if (reply instanceof OwnerForwarding.Completed completed) {
      return response.setOutcome(ForwardMutationResponse.Outcome.COMPLETED)
          .setOperationResult(ByteString.copyFrom(completed.operationResult())).build();
    }
    if (reply instanceof OwnerForwarding.Rejected rejected) {
      return response.setOutcome(ForwardMutationResponse.Outcome.REJECTED)
          .setFailureCode(rejected.category().name()).build();
    }
    if (reply instanceof OwnerForwarding.SemanticRejected rejected) {
      return response.setOutcome(ForwardMutationResponse.Outcome.SEMANTIC_REJECTED)
          .setFailureCode(rejected.reason().name()).build();
    }
    if (reply instanceof OwnerForwarding.LeaseRejected rejected) {
      return response.setOutcome(ForwardMutationResponse.Outcome.LEASE_REJECTED)
          .setFailureCode(rejected.reason().name()).build();
    }
    OwnerForwarding.Unknown unknown = (OwnerForwarding.Unknown) reply;
    return response.setOutcome(ForwardMutationResponse.Outcome.UNKNOWN)
        .setFailureCode(unknown.category().name()).build();
  }

  static OwnerForwarding.Reply fromWire(ForwardMutationResponse response) {
    Objects.requireNonNull(response, "response");
    try {
      return switch (response.getOutcome()) {
        case COMPLETED -> new OwnerForwarding.Completed(response.getOperationResult().toByteArray());
        case REJECTED -> new OwnerForwarding.Rejected(
            BackendFailure.Category.valueOf(response.getFailureCode()));
        case SEMANTIC_REJECTED -> new OwnerForwarding.SemanticRejected(
            KvFailure.Reason.valueOf(response.getFailureCode()));
        case LEASE_REJECTED -> new OwnerForwarding.LeaseRejected(
            LeaseFailure.Reason.valueOf(response.getFailureCode()));
        case UNKNOWN -> new OwnerForwarding.Unknown(
            BackendFailure.Category.valueOf(response.getFailureCode()));
        case OUTCOME_UNSPECIFIED, UNRECOGNIZED ->
            new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
      };
    } catch (IllegalArgumentException malformed) {
      return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
    }
  }
}
