package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MutationRelayTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final OwnerForwarding.Route ROUTE =
      new OwnerForwarding.Route("owner-a", 1, "endpoint-a", "peer-a");

  @Test
  void cancellationWhileRouteResolutionIsPendingPreventsSendOwnershipHandoff() {
    var route = new CompletableFuture<OwnerForwarding.Route>();
    var sends = new AtomicInteger();
    RequestScope scope = RequestScope.withoutDeadline();
    var relay = relay(ignored -> route, (selected, forwarded, requestScope) -> {
      sends.incrementAndGet();
      return CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[] {1}));
    });
    var result = relay.forward(request(), scope);

    assertTrue(scope.cancel());
    route.complete(ROUTE);

    var rejected = assertInstanceOf(MutationRelay.KnownRejection.class, result.join());
    assertEquals(BackendFailure.Category.CANCELLED, rejected.category());
    assertEquals(0, sends.get());
  }

  @Test
  void sendOwnershipHandedOffBeforeCancellationMayStillReturnItsKnownReply() {
    var route = new CompletableFuture<OwnerForwarding.Route>();
    var sent = new CompletableFuture<OwnerForwarding.Reply>();
    var sends = new AtomicInteger();
    RequestScope scope = RequestScope.withoutDeadline();
    var relay = relay(ignored -> route, (selected, forwarded, requestScope) -> {
      sends.incrementAndGet();
      return sent;
    });
    var result = relay.forward(request(), scope);

    route.complete(ROUTE);
    assertEquals(1, sends.get());
    assertTrue(scope.cancel());
    sent.complete(new OwnerForwarding.Completed(new byte[] {4, 5}));

    var completed = assertInstanceOf(MutationRelay.KnownResult.class, result.join());
    org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] {4, 5}, completed.responseBytes());
  }

  @Test
  void failureAfterSendIsReportedAsUnknownAndNeverRetried() {
    var sends = new AtomicInteger();
    var relay = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
        (selected, forwarded, scope) -> {
          sends.incrementAndGet();
          return CompletableFuture.failedFuture(new CompletionException(
              new BackendFailure(BackendFailure.Category.UNKNOWN)));
        });

    var unknown = assertInstanceOf(MutationRelay.ResultUnknown.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertEquals(BackendFailure.Category.UNKNOWN, unknown.category());
    assertEquals(1, sends.get());
  }

  @Test
  void routeMustMatchTheAuthoritativeOwnerBeforeSend() {
    var wrongRoute = new OwnerForwarding.Route("owner-b", 2, "endpoint-b", "peer-b");
    var sends = new AtomicInteger();
    var relay = relay(ignored -> CompletableFuture.completedFuture(wrongRoute),
        (selected, forwarded, scope) -> {
          sends.incrementAndGet();
          return CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[0]));
        });

    var failure = assertThrows(CompletionException.class,
        () -> relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertEquals(0, sends.get());
  }

  private static MutationRelay relay(
      java.util.function.Function<String, CompletableFuture<OwnerForwarding.Route>> routes,
      OwnerForwarding.AuthenticatedChannel channel) {
    CommitState state = CommitState.initial(FINGERPRINT, 1).claimOwner("owner-a", 1, 10_000);
    CommitControlStore control = new CommitControlStore() {
      @Override
      public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
        return CompletableFuture.completedFuture(
            Optional.of(new ControlSnapshot(state, new byte[] {1})));
      }

      @Override
      public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
        throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Boolean> compareAndSet(
          ControlSnapshot observed, CommitState replacement, RequestScope scope) {
        throw new UnsupportedOperationException();
      }
    };
    return new MutationRelay(control,
        (group, owner, epoch) -> routes.apply(owner), channel);
  }

  private static MutationForwardRequest request() {
    ByteKey operation = ByteKey.copyOf(new byte[] {1});
    return new MutationForwardRequest("group-a", InvocationId.create(),
        MutationForwardRequest.digest("group-a", operation), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[] {7}), 1, 1_000, 0, operation);
  }
}
