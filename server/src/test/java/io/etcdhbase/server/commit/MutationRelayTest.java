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
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  void knownBackendCategoriesSurviveWrappedSendFailuresAndUnknownReplies() {
    for (BackendFailure.Category category : new BackendFailure.Category[] {
        BackendFailure.Category.AUTHENTICATION,
        BackendFailure.Category.PERMISSION,
        BackendFailure.Category.RESOURCE_LIMIT}) {
      var synchronousSends = new AtomicInteger();
      var synchronousFailure = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
          (selected, forwarded, scope) -> {
            synchronousSends.incrementAndGet();
            throw new CompletionException(new BackendFailure(category));
          });
      assertUnknownCategory(category, synchronousFailure);
      assertEquals(1, synchronousSends.get());

      var asynchronousSends = new AtomicInteger();
      var asynchronousFailure = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
          (selected, forwarded, scope) -> {
            asynchronousSends.incrementAndGet();
            return CompletableFuture.failedFuture(
                new CompletionException(new BackendFailure(category)));
          });
      assertUnknownCategory(category, asynchronousFailure);
      assertEquals(1, asynchronousSends.get());

      var unknownReply = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
          (selected, forwarded, scope) -> CompletableFuture.completedFuture(
              new OwnerForwarding.Unknown(category)));
      assertUnknownCategory(category, unknownReply);
    }
  }

  @Test
  void receiverUnknownReplyRemainsUnknownAtRelay() {
    var relay = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
        (selected, forwarded, scope) -> CompletableFuture.completedFuture(
            new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE)));

    var unknown = assertInstanceOf(MutationRelay.ResultUnknown.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertEquals(BackendFailure.Category.UNAVAILABLE, unknown.category());
  }

  @Test
  void semanticRejectionPreservesTheKnownEtcdReason() {
    var relay = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
        (selected, forwarded, scope) -> CompletableFuture.completedFuture(
            new OwnerForwarding.SemanticRejected(KvFailure.Reason.KEY_NOT_FOUND)));
    var result = assertInstanceOf(MutationRelay.SemanticRejection.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND, result.reason());
  }

  @Test
  void leaseRejectionPreservesItsOwnSemanticReason() {
    var relay = relay(ignored -> CompletableFuture.completedFuture(ROUTE),
        (selected, forwarded, scope) -> CompletableFuture.completedFuture(
            new OwnerForwarding.LeaseRejected(LeaseFailure.Reason.ALREADY_EXISTS)));
    var result = assertInstanceOf(MutationRelay.LeaseRejection.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertEquals(LeaseFailure.Reason.ALREADY_EXISTS, result.reason());
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

  @Test
  void staleGatewayOwnerSampleIsRelocatedBeforeAnyRequestIsSent() {
    AtomicReference<CommitState> state = new AtomicReference<>(ownerBState());
    List<MutationForwardRequest> sentRequests = new CopyOnWriteArrayList<>();
    List<OwnerForwarding.Route> sentRoutes = new CopyOnWriteArrayList<>();
    MutationRelay relay = new MutationRelay(control(state),
        (group, owner, epoch) -> CompletableFuture.completedFuture(route(owner, epoch)),
        (selected, forwarded, scope) -> {
          sentRoutes.add(selected);
          sentRequests.add(forwarded);
          return CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[] {9}));
        });
    MutationForwardRequest original = request();

    MutationRelay.KnownResult result = assertInstanceOf(MutationRelay.KnownResult.class,
        relay.forward(original, RequestScope.withoutDeadline()).join());

    assertEquals("owner-b", sentRoutes.getFirst().ownerInstance());
    assertEquals(2, sentRequests.getFirst().expectedOwnerEpoch());
    assertEquals(original.invocationId(), sentRequests.getFirst().invocationId());
    assertEquals(original.requestDigest(), sentRequests.getFirst().requestDigest());
    assertEquals(original.operationBytes(), sentRequests.getFirst().operationBytes());
    org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] {9}, result.responseBytes());
  }

  @Test
  void ownerChangeDuringRouteLookupDiscardsTheStaleRouteAndKeepsInvocation() throws Exception {
    AtomicReference<CommitState> state = new AtomicReference<>(ownerAState());
    var routeLookupStarted = new CompletableFuture<Void>();
    var firstRoute = new CompletableFuture<OwnerForwarding.Route>();
    var routeLookups = new AtomicInteger();
    List<MutationForwardRequest> sentRequests = new CopyOnWriteArrayList<>();
    List<OwnerForwarding.Route> sentRoutes = new CopyOnWriteArrayList<>();
    MutationRelay relay = new MutationRelay(control(state), (group, owner, epoch) -> {
      if (routeLookups.getAndIncrement() == 0) {
        assertEquals("owner-a", owner);
        assertEquals(1, epoch);
        routeLookupStarted.complete(null);
        return firstRoute;
      }
      assertEquals("owner-b", owner);
      assertEquals(2, epoch);
      return CompletableFuture.completedFuture(route(owner, epoch));
    }, (selected, forwarded, scope) -> {
      sentRoutes.add(selected);
      sentRequests.add(forwarded);
      return CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[] {4}));
    });
    MutationForwardRequest original = request();
    var result = relay.forward(original, RequestScope.withoutDeadline());

    routeLookupStarted.get(5, TimeUnit.SECONDS);
    assertTrue(sentRoutes.isEmpty(), "the relay must not send before the route is revalidated");
    state.set(ownerBState());
    firstRoute.complete(route("owner-a", 1));

    assertInstanceOf(MutationRelay.KnownResult.class, result.join());
    assertEquals(1, sentRoutes.size(), "only the current owner may receive the request");
    assertEquals("owner-b", sentRoutes.getFirst().ownerInstance());
    assertEquals(2, sentRequests.getFirst().expectedOwnerEpoch());
    assertEquals(original.invocationId(), sentRequests.getFirst().invocationId());
    assertEquals(original.requestDigest(), sentRequests.getFirst().requestDigest());
    assertEquals(original.operationBytes(), sentRequests.getFirst().operationBytes());
  }

  @Test
  void unknownReplyAfterSendIsNotRelocatedEvenWhenOwnerChanges() {
    AtomicReference<CommitState> state = new AtomicReference<>(ownerAState());
    var sends = new AtomicInteger();
    MutationRelay relay = new MutationRelay(control(state),
        (group, owner, epoch) -> CompletableFuture.completedFuture(route(owner, epoch)),
        (selected, forwarded, scope) -> {
          sends.incrementAndGet();
          state.set(ownerBState());
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
        });

    var result = assertInstanceOf(MutationRelay.ResultUnknown.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());

    assertEquals(BackendFailure.Category.UNAVAILABLE, result.category());
    assertEquals(1, sends.get(), "an uncertain sent invocation must never be retried at a new owner");
  }

  private static MutationRelay relay(
      java.util.function.Function<String, CompletableFuture<OwnerForwarding.Route>> routes,
      OwnerForwarding.AuthenticatedChannel channel) {
    return new MutationRelay(control(new AtomicReference<>(ownerAState())),
        (group, owner, epoch) -> routes.apply(owner), channel);
  }

  private static CommitControlStore control(AtomicReference<CommitState> state) {
    return new CommitControlStore() {
      @Override
      public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
        return CompletableFuture.completedFuture(
            Optional.of(new ControlSnapshot(state.get(), new byte[] {1})));
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
  }

  private static CommitState ownerAState() {
    return CommitState.initial(FINGERPRINT, 1).claimOwner("owner-a", 1, 10_000);
  }

  private static CommitState ownerBState() {
    return ownerAState().claimOwner("owner-b", 10_000, 20_000);
  }

  private static OwnerForwarding.Route route(String owner, long epoch) {
    return new OwnerForwarding.Route(owner, epoch, "endpoint-" + owner, "peer-" + owner);
  }

  private static void assertUnknownCategory(
      BackendFailure.Category category, MutationRelay relay) {
    var unknown = assertInstanceOf(MutationRelay.ResultUnknown.class,
        relay.forward(request(), RequestScope.withoutDeadline()).join());
    assertEquals(category, unknown.category());
  }

  private static MutationForwardRequest request() {
    ByteKey operation = ByteKey.copyOf(new byte[] {1});
    return new MutationForwardRequest("group-a", InvocationId.create(),
        MutationForwardRequest.digest("group-a", operation), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[] {7}), 1, 1_000, 0, operation);
  }
}
