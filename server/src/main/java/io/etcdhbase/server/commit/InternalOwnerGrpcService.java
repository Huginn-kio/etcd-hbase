package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.internal.v1.ForwardMutationRequest;
import io.etcdhbase.grpc.internal.v1.ForwardMutationResponse;
import io.etcdhbase.grpc.internal.v1.InternalOwnerServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/** Internal unary adapter; peer identity comes from the authenticated TLS session. */
final class InternalOwnerGrpcService extends InternalOwnerServiceGrpc.InternalOwnerServiceImplBase {
  @FunctionalInterface
  interface Handler {
    CompletionStage<OwnerForwarding.Reply> receive(
        String authenticatedPeer, MutationForwardRequest request, RequestScope scope);
  }

  private final Map<String, Handler> handlers;

  InternalOwnerGrpcService(Map<String, Handler> handlers) {
    this.handlers = Objects.requireNonNull(handlers, "handlers");
  }

  @Override
  public void forwardMutation(ForwardMutationRequest wireRequest,
      StreamObserver<ForwardMutationResponse> responseObserver) {
    Objects.requireNonNull(wireRequest, "wireRequest");
    Objects.requireNonNull(responseObserver, "responseObserver");
    final MutationForwardRequest request;
    try {
      request = OwnerForwardingProtocolMapper.fromWire(wireRequest);
    } catch (IllegalArgumentException malformed) {
      responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
          .withDescription("internal mutation envelope is invalid").asRuntimeException());
      return;
    }
    String peer = OwnerPeerIdentityInterceptor.currentPeerIdentity();
    Handler handler = handlers.get(request.tableGroupId());
    if (peer == null || handler == null) {
      complete(responseObserver, new OwnerForwarding.Unknown(
          peer == null ? BackendFailure.Category.PERMISSION : BackendFailure.Category.UNAVAILABLE));
      return;
    }
    RequestScope scope = RequestScopeInterceptor.currentScope();
    try {
      handler.receive(peer, request, scope).whenComplete((reply, failure) -> {
        complete(responseObserver, failure == null && reply != null ? reply
            : new OwnerForwarding.Unknown(category(failure)));
      });
    } catch (RuntimeException failure) {
      complete(responseObserver, new OwnerForwarding.Unknown(category(failure)));
    } catch (Error fatal) {
      responseObserver.onError(io.grpc.Status.INTERNAL
          .withDescription("internal mutation processing failed").asRuntimeException());
      throw fatal;
    }
  }

  private static void complete(StreamObserver<ForwardMutationResponse> observer,
      OwnerForwarding.Reply reply) {
    try {
      observer.onNext(OwnerForwardingProtocolMapper.toWire(reply));
      observer.onCompleted();
    } catch (RuntimeException ignored) {
      // The authenticated peer may have closed the channel after dispatch.
    }
  }

  private static BackendFailure.Category category(Throwable failure) {
    Throwable cause = failure;
    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
    return cause instanceof BackendFailure backend ? backend.category() : BackendFailure.Category.UNKNOWN;
  }
}
