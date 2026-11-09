package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.kv.ResponseBudget;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Owns one finalized Put response through decoding, projection and Netty serialization. */
public final class PutResponseEmitter {
  private PutResponseEmitter() {}

  /**
   * Takes ownership of {@code resolved}. Invoke from a bounded worker, not an event-loop thread.
   * The configured pinned Netty transport consumes the message stream inside onNext; the drain
   * remains registered while that call is in progress, including concurrent cancellation.
   */
  public static void send(PutCommand command, InvocationResultResolver.ResolvedResult resolved,
      Function<ByteBuffer, PutResult> decoder, long clusterId, long memberId,
      RequestScope scope, BudgetManager.Account account, StreamObserver<Rpc.PutResponse> observer) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(resolved, "resolved");
    Objects.requireNonNull(decoder, "decoder");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(observer, "observer");
    CompletableFuture<Void> serializationDrained = new CompletableFuture<>();
    BudgetManager.Lease responseLease = null;
    RequestScope.ScopedCleanup responseOwner = null;
    boolean terminalAttempted = false;
    try {
      if (!account.belongsTo(scope) || !resolved.payload().belongsTo(scope)) {
        throw new IllegalArgumentException("response owner belongs to another request");
      }
      if (!resolved.payload().retainForConsumer(serializationDrained)) return;
      ByteBuffer bytes = resolved.payload().buffer();
      responseLease = account.tryRetain(BudgetManager.Memory.OUTPUT,
          ResponseBudget.forEncodedResult(bytes.remaining()))
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      responseOwner = releaseAfterDrain(scope, responseLease, serializationDrained);
      PutResult result = Objects.requireNonNull(decoder.apply(bytes), "decoded Put result");
      Rpc.PutResponse response = PutResponseMapper.toWire(command, result, resolved, clusterId, memberId);
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      observer.onNext(response);
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      terminalAttempted = true;
      observer.onCompleted();
    } catch (BackendFailure | RuntimeException failure) {
      if (!terminalAttempted && !scope.isClosed()) signalError(observer, failure);
      else if (terminalAttempted) throw new IllegalStateException("Put response termination failed", failure);
    } catch (Error fatal) {
      if (!terminalAttempted && !scope.isClosed()) signalError(observer, fatal);
      throw fatal;
    } finally {
      serializationDrained.complete(null);
      try {
        resolved.close();
      } finally {
        if (responseOwner != null) close(responseOwner);
        else if (responseLease != null) responseLease.close();
      }
    }
  }

  private static RequestScope.ScopedCleanup releaseAfterDrain(RequestScope scope,
      BudgetManager.Lease lease, CompletableFuture<Void> drain) {
    CompletableFuture<Void> released = new CompletableFuture<>();
    return scope.registerScopedCleanup(() -> drain.whenComplete((ignored, failure) -> {
      try {
        lease.close();
        released.complete(null);
      } catch (RuntimeException | Error cleanup) {
        released.completeExceptionally(cleanup);
      }
    }), released);
  }

  private static void signalError(StreamObserver<Rpc.PutResponse> observer, Throwable failure) {
    try {
      observer.onError(ErrorProjection.project(failure));
    } catch (RuntimeException | Error terminationFailure) {
      if (failure != terminationFailure) failure.addSuppressed(terminationFailure);
      if (terminationFailure instanceof Error fatal) throw fatal;
    }
  }

  private static void close(RequestScope.ScopedCleanup owner) {
    try {
      owner.close();
    } catch (Exception failure) {
      throw new IllegalStateException("Put response memory did not close", failure);
    }
  }
}
