package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MutationForwardReceiverTest {
  private static final String GROUP = "group-a";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTH_DIGEST = ByteKey.copyOf(new byte[32]);
  private static final InvocationId INVOCATION = new InvocationId(
      UUID.fromString("00000000-0000-0000-0000-000000000001"));
  private static final TransactionId TRANSACTION = new TransactionId(
      UUID.fromString("00000000-0000-0000-0000-000000000002"));

  @Test
  void stalePreparingBindingIsNotDispatchedAfterOwnerTakeover() {
    assertStaleBindingRejected(InvocationBinding.State.PREPARING);
  }

  @Test
  void staleSealedBindingIsNotDispatchedAfterOwnerTakeover() {
    assertStaleBindingRejected(InvocationBinding.State.SEALED);
  }

  @Test
  void currentBindingCanReachProcessorAndRejectedMeansNoMutationWasDispatched() {
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(repository, processors);

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var rejected = assertInstanceOf(OwnerForwarding.Rejected.class, reply);
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, rejected.category());
    assertEquals(1, processors.get(), "a processor may run and prove that it dispatched no mutation");
  }

  @Test
  void authorizationScopeMismatchIsRejectedBeforeProcessorDispatch() {
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    byte[] differentDigest = new byte[32];
    differentDigest[0] = 1;
    var fixture = receiver(repository, processors, ByteKey.copyOf(differentDigest));

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var rejected = assertInstanceOf(OwnerForwarding.Rejected.class, reply);
    assertEquals(BackendFailure.Category.PERMISSION, rejected.category());
    assertEquals(0, processors.get());
  }

  private static void assertStaleBindingRejected(InvocationBinding.State state) {
    var repository = new InMemoryInvocationRepository();
    var stale = InvocationBinding.begin(INVOCATION, request().requestDigest(), AUTH_DIGEST,
        TRANSACTION, 1, 0, 1);
    if (state == InvocationBinding.State.SEALED) {
      stale = stale.seal(ByteKey.copyOf(new byte[] {9}));
    }
    repository.binding = stale;
    var processors = new AtomicInteger();
    var fixture = receiver(repository, processors);

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var rejected = assertInstanceOf(OwnerForwarding.Rejected.class, reply);
    assertEquals(BackendFailure.Category.UNAVAILABLE, rejected.category());
    assertEquals(0, processors.get());
  }

  private static Fixture receiver(InMemoryInvocationRepository repository, AtomicInteger processors) {
    return receiver(repository, processors, AUTH_DIGEST);
  }

  private static Fixture receiver(InMemoryInvocationRepository repository,
      AtomicInteger processors, ByteKey resolvedAuthorizationDigest) {
    CommitState current = CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-old", 1, 10)
        .claimOwner("owner-current", 10, 2_000);
    CommitControlStore control = new FixedControlStore(current);
    var invocations = new InvocationService(repository);
    var identity = new AuthorizedBackendIdentity("test-provider", "test-subject", "ctx-1",
        new AuthenticationScope(GROUP, "profile-a"), Set.of(GROUP), 1);
    MutationForwardReceiver.AuthorizationResolver authorizations = (peer, request, scope) ->
        CompletableFuture.completedFuture(new MutationForwardReceiver.VerifiedAuthorization(
            identity, resolvedAuthorizationDigest));
    MutationForwardReceiver.MutationProcessor processor = (request, binding, verified, scope) -> {
      processors.incrementAndGet();
      return CompletableFuture.completedFuture(
          new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
    };
    var receiver = new MutationForwardReceiver(control, invocations, authorizations, processor,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    return new Fixture(receiver);
  }

  private static MutationForwardRequest request() {
    ByteKey operation = ByteKey.copyOf(new byte[] {1, 2, 3});
    return new MutationForwardRequest(GROUP, INVOCATION,
        MutationForwardRequest.digest(GROUP, operation), AUTH_DIGEST,
        ByteKey.copyOf("ctx-1".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        2, 1_000, MutationForwardRequest.MAX_HOPS, operation);
  }

  private record Fixture(MutationForwardReceiver receiver) {}

  private static final class FixedControlStore implements CommitControlStore {
    private final ControlSnapshot snapshot;

    private FixedControlStore(CommitState state) {
      snapshot = new ControlSnapshot(state, new byte[] {1});
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(snapshot));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class InMemoryInvocationRepository implements InvocationRepository {
    private InvocationBinding binding;

    @Override
    public CompletableFuture<Optional<InvocationBinding>> read(
        InvocationId invocationId, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(binding));
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding proposed, RequestScope scope) {
      if (binding == null) {
        binding = proposed;
        return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
      }
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.ALREADY_PRESENT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      if (binding != expected) return CompletableFuture.completedFuture(false);
      binding = replacement;
      return CompletableFuture.completedFuture(true);
    }
  }
}
