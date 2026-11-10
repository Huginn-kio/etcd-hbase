package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InvocationServiceTest {
  private static final ByteKey REQUEST_DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTH_DIGEST = ByteKey.copyOf(new byte[32]);
  private static final InvocationId INVOCATION = new InvocationId(
      UUID.fromString("00000000-0000-0000-0000-000000000001"));

  @Test
  void concurrentFirstBindsJoinOneImmutableWinner() {
    var repository = new InMemoryInvocationRepository(new CyclicBarrier(2));
    var service = new InvocationService(repository);
    var firstProposal = binding(1, 0, transaction(1));
    var secondProposal = binding(1, 0, transaction(2));

    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = CompletableFuture.supplyAsync(
          () -> service.bindOrJoin(firstProposal, RequestScope.withoutDeadline()).join(), executor);
      var second = CompletableFuture.supplyAsync(
          () -> service.bindOrJoin(secondProposal, RequestScope.withoutDeadline()).join(), executor);

      var firstBinding = first.join();
      var secondBinding = second.join();
      assertTrue(firstBinding.sameAttempt(secondBinding));
      assertNotEquals(firstProposal.transactionId(), secondProposal.transactionId());
      assertEquals(2, repository.createCount.get());
    }
  }

  @Test
  void lostCreateReplyIsResolvedByReadingTheDurableBinding() {
    var repository = new InMemoryInvocationRepository();
    repository.loseFirstCreateReply.set(true);
    var service = new InvocationService(repository);
    var proposal = binding(1, 0, transaction(3));

    var resolved = service.bindOrJoin(proposal, RequestScope.withoutDeadline()).join();

    assertTrue(proposal.sameAttempt(resolved));
    assertEquals(1, repository.createCount.get());
    assertEquals(1, repository.readCount.get());
  }

  @Test
  void bindingCannotJoinDifferentRequestOrAuthorizationAndTransitionsKeepIdentity() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var proposal = binding(1, 0, transaction(4));
    var persisted = service.bindOrJoin(proposal, RequestScope.withoutDeadline()).join();

    byte[] differentDigestBytes = new byte[32];
    differentDigestBytes[0] = 1;
    var differentRequest = InvocationBinding.begin(INVOCATION,
        ByteKey.copyOf(differentDigestBytes), AUTH_DIGEST, transaction(5), 1, 0, 1);
    var mismatch = assertThrows(CompletionException.class,
        () -> service.bindOrJoin(differentRequest, RequestScope.withoutDeadline()).join());
    assertTrue(mismatch.getCause() instanceof IllegalStateException);

    var sealed = persisted.seal(ByteKey.copyOf(new byte[] {9}));
    var sealedBinding = service.transition(persisted, sealed, RequestScope.withoutDeadline()).join();
    assertEquals(InvocationBinding.State.SEALED, sealedBinding.state());
    var selected = sealedBinding.selected(ByteKey.copyOf(new byte[] {10}));
    var selectedBinding = service.transition(
        sealedBinding, selected, RequestScope.withoutDeadline()).join();
    assertEquals(InvocationBinding.State.SELECTED, selectedBinding.state());
    assertEquals(InvocationBinding.State.WINDOW_CLOSED,
        service.transition(selectedBinding, selectedBinding.closeQueryWindow(),
            RequestScope.withoutDeadline()).join().state());

    var differentAttempt = InvocationBinding.begin(INVOCATION, REQUEST_DIGEST,
        AUTH_DIGEST, transaction(6), 1, 0, 1).seal(ByteKey.copyOf(new byte[] {9}));
    assertThrows(IllegalArgumentException.class,
        () -> service.transition(sealed, differentAttempt, RequestScope.withoutDeadline()));
  }

  @Test
  void lostCasReplyIsResolvedByReadingTheDurableProgress() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var preparing = service.bindOrJoin(
        binding(1, 0, transaction(7)), RequestScope.withoutDeadline()).join();
    repository.loseFirstCasReply.set(true);
    var sealed = preparing.seal(ByteKey.copyOf(new byte[] {11}));

    var resolved = service.transition(preparing, sealed, RequestScope.withoutDeadline()).join();

    assertEquals(InvocationBinding.State.SEALED, resolved.state());
    assertEquals(InvocationBinding.State.SEALED,
        repository.bindings.get(INVOCATION).state());
  }

  @Test
  void nullCasOutcomeIsResolvedFromDurableProgress() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var preparing = service.bindOrJoin(binding(1, 0, transaction(8)), RequestScope.withoutDeadline()).join();
    repository.nullFirstCasReply.set(true);
    var sealed = preparing.seal(ByteKey.copyOf(new byte[] {11}));
    assertEquals(InvocationBinding.State.SEALED,
        service.transition(preparing, sealed, RequestScope.withoutDeadline()).join().state());
  }

  @Test
  void definitePermissionErrorIsNotHiddenByAlreadyMatchingProgress() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var preparing = binding(1, 0, transaction(9));
    var sealed = preparing.seal(ByteKey.copyOf(new byte[] {11}));
    repository.bindings.put(INVOCATION, sealed);
    repository.failCasPermission.set(true);
    var failure = assertThrows(CompletionException.class,
        () -> service.transition(preparing, sealed, RequestScope.withoutDeadline()).join());
    assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
    assertEquals(0, repository.readCount.get());
  }

  private static InvocationBinding binding(long epoch, long base, TransactionId transaction) {
    return InvocationBinding.begin(INVOCATION, REQUEST_DIGEST, AUTH_DIGEST,
        transaction, epoch, base, 1);
  }

  private static TransactionId transaction(long value) {
    return new TransactionId(new UUID(0, value));
  }

  private static final class InMemoryInvocationRepository implements InvocationRepository {
    private final ConcurrentHashMap<InvocationId, InvocationBinding> bindings = new ConcurrentHashMap<>();
    private final CyclicBarrier createBarrier;
    private final AtomicBoolean loseFirstCreateReply = new AtomicBoolean();
    private final AtomicBoolean loseFirstCasReply = new AtomicBoolean();
    private final AtomicBoolean nullFirstCasReply = new AtomicBoolean();
    private final AtomicBoolean failCasPermission = new AtomicBoolean();
    private final AtomicInteger createCount = new AtomicInteger();
    private final AtomicInteger readCount = new AtomicInteger();

    private InMemoryInvocationRepository() {
      this(null);
    }

    private InMemoryInvocationRepository(CyclicBarrier createBarrier) {
      this.createBarrier = createBarrier;
    }

    @Override
    public CompletableFuture<Optional<InvocationBinding>> read(
        InvocationId invocationId, RequestScope scope) {
      readCount.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.ofNullable(bindings.get(invocationId)));
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding binding, RequestScope scope) {
      createCount.incrementAndGet();
      if (createBarrier != null) {
        try {
          createBarrier.await(10, TimeUnit.SECONDS);
        } catch (Exception failure) {
          return CompletableFuture.failedFuture(failure);
        }
      }
      var existing = bindings.putIfAbsent(binding.invocationId(), binding);
      if (loseFirstCreateReply.compareAndSet(true, false)) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(existing == null
          ? SystemRecordStore.CreateResult.CREATED : SystemRecordStore.CreateResult.ALREADY_PRESENT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      if (failCasPermission.get()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      }
      boolean replaced = bindings.replace(expected.invocationId(), expected, replacement);
      if (replaced && nullFirstCasReply.compareAndSet(true, false)) {
        return CompletableFuture.completedFuture(null);
      }
      if (replaced && loseFirstCasReply.compareAndSet(true, false)) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(replaced);
    }
  }
}
