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

  @Test
  void verifiedNoChangeWinsTheSameCasAsSealAndKeepsInvocationAfterWindowCloses() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = service.bindOrJoin(binding(1, 0, transaction(10)), scope).join();
    var result = resultFor(preparing);
    var guardCalls = new AtomicInteger();
    repository.loseFirstCasReply.set(true);

    var completed = service.completeWithoutChanges(preparing, result.reference(),
        new ResultCatalog(result.repository()), () -> {
          guardCalls.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        }, scope).join();
    assertEquals(InvocationBinding.State.NO_CHANGES, completed.state());
    assertEquals(Optional.of(result.reference().token()), completed.resultReference());
    assertTrue(completed.manifestRoot().isEmpty());
    assertEquals(1, guardCalls.get());
    assertThrows(CompletionException.class, () -> service.transition(preparing,
        preparing.seal(ByteKey.copyOf(new byte[] {1})), scope).join());
    assertThrows(IllegalStateException.class, () -> completed.seal(ByteKey.copyOf(new byte[] {1})));

    var closed = service.transition(completed, completed.closeQueryWindow(), scope).join();
    assertEquals(InvocationBinding.State.WINDOW_CLOSED, closed.state());
    assertTrue(closed.resultReference().isEmpty());
    assertEquals(closed, service.bindOrJoin(preparing, scope).join());
    scope.close();
  }

  @Test
  void sealWinningPreventsNoChangeTerminalAndUnverifiedTransitions() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = service.bindOrJoin(binding(1, 0, transaction(11)), scope).join();
    var result = resultFor(preparing);
    var unverified = preparing.completeWithoutChanges(result.reference().token());
    assertThrows(IllegalArgumentException.class, () -> service.transition(preparing, unverified, scope));
    var sealed = service.transition(preparing, preparing.seal(ByteKey.copyOf(new byte[] {2})), scope).join();
    assertThrows(CompletionException.class, () -> service.completeWithoutChanges(preparing,
        result.reference(), new ResultCatalog(result.repository()),
        () -> CompletableFuture.completedFuture(null), scope).join());
    assertEquals(sealed, repository.bindings.get(INVOCATION));
    scope.close();
  }

  @Test
  void concurrentSealAndNoChangeHaveExactlyOneDurableWinner() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = service.bindOrJoin(binding(1, 0, transaction(16)), scope).join();
    var result = resultFor(preparing);
    repository.compareBarrier = new CyclicBarrier(2);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var sealed = CompletableFuture.supplyAsync(() -> service.transition(preparing,
          preparing.seal(ByteKey.copyOf(new byte[] {5})), scope).join(), executor)
          .handle((value, failure) -> failure == null ? value : null);
      var noChanges = CompletableFuture.supplyAsync(() -> service.completeWithoutChanges(preparing,
          result.reference(), new ResultCatalog(result.repository()),
          () -> CompletableFuture.completedFuture(null), scope).join(), executor)
          .handle((value, failure) -> failure == null ? value : null);
      var sealedResult = sealed.join();
      var noChangeResult = noChanges.join();
      assertTrue((sealedResult == null) != (noChangeResult == null));
      var winner = sealedResult == null ? noChangeResult : sealedResult;
      assertEquals(winner, repository.bindings.get(INVOCATION));
    } finally {
      scope.close();
    }
  }

  @Test
  void firstBindCannotStartFromAnAlreadyResolvedOrSealedState() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = binding(1, 0, transaction(14));
    assertThrows(IllegalArgumentException.class, () -> service.bindOrJoin(
        preparing.completeWithoutChanges(ByteKey.copyOf(new byte[] {1})), scope));
    assertThrows(IllegalArgumentException.class, () -> service.bindOrJoin(
        preparing.seal(ByteKey.copyOf(new byte[] {2})), scope));
    assertEquals(0, repository.createCount.get());
    scope.close();
  }

  @Test
  void missingResultsOrFailedReadProtectionCannotCompleteNoChange() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = service.bindOrJoin(binding(1, 0, transaction(12)), scope).join();
    var result = resultFor(preparing);
    var guardCalls = new AtomicInteger();
    result.repository().page = null;
    assertThrows(CompletionException.class, () -> service.completeWithoutChanges(preparing,
        result.reference(), new ResultCatalog(result.repository()), () -> {
          guardCalls.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        }, scope).join());
    assertEquals(0, guardCalls.get());
    result.repository().page = result.page();
    assertThrows(CompletionException.class, () -> service.completeWithoutChanges(preparing,
        result.reference(), new ResultCatalog(result.repository()),
        () -> CompletableFuture.failedFuture(new IllegalStateException("read protection revoked")), scope).join());
    assertEquals(InvocationBinding.State.PREPARING, repository.bindings.get(INVOCATION).state());
    var other = resultFor(binding(1, 0, transaction(13)));
    assertThrows(IllegalArgumentException.class, () -> service.completeWithoutChanges(preparing,
        other.reference(), new ResultCatalog(other.repository()),
        () -> CompletableFuture.completedFuture(null), scope));
    scope.close();
  }

  @Test
  void cancellationWhileReadProtectionFinishesCannotCommitNoChange() {
    var repository = new InMemoryInvocationRepository();
    var service = new InvocationService(repository);
    var scope = RequestScope.withoutDeadline();
    var preparing = service.bindOrJoin(binding(1, 0, transaction(15)), scope).join();
    var result = resultFor(preparing);
    var finishingRead = new CompletableFuture<Void>();
    var pending = service.completeWithoutChanges(preparing, result.reference(),
        new ResultCatalog(result.repository()), () -> finishingRead, scope);
    assertTrue(scope.cancel());
    finishingRead.complete(null);
    var failure = assertThrows(CompletionException.class, pending::join);
    assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
    assertEquals(InvocationBinding.State.PREPARING, repository.bindings.get(INVOCATION).state());
  }

  private static NoChangeResult resultFor(InvocationBinding binding) {
    var identity = ResultIdentity.from(binding);
    var page = new ResultPage(identity, 0, new byte[] {1});
    var directory = ResultDirectory.create(identity, java.util.List.of(ResultPageReference.from(page)));
    return new NoChangeResult(directory.reference(), page, new FixedResultRepository(directory, page));
  }

  private record NoChangeResult(ResultReference reference, ResultPage page, FixedResultRepository repository) {}

  private static final class FixedResultRepository implements ResultRepository {
    private final ResultDirectory directory;
    private ResultPage page;
    private FixedResultRepository(ResultDirectory directory, ResultPage page) {
      this.directory = directory;
      this.page = page;
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory directory, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(page));
    }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(
        ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(directory));
    }
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
    private CyclicBarrier compareBarrier;
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
      if (compareBarrier != null) {
        try {
          compareBarrier.await(10, TimeUnit.SECONDS);
        } catch (Exception failure) {
          return CompletableFuture.failedFuture(failure);
        }
      }
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
