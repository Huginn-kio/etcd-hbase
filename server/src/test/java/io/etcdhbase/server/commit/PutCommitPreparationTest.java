package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.ResultRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PutCommitPreparationTest {
  @Test
  void invalidPutFailsBeforeAnyDurableWrite() throws Exception {
    var repository = new DelayedResultRepository();
    var fixture = new Fixture(repository, 4L << 20);
    var command = new PutCommand(key(), new byte[0], 0, false, true, false);
    CompletionException failed = assertThrows(CompletionException.class,
        () -> fixture.preparation.prepare(command, binding(), Optional.empty(),
            fixture.scope, fixture.account, PutCommitPreparationTest::completedRead).join());
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND,
        assertInstanceOf(KvFailure.class, failed.getCause()).reason());
    assertEquals(0, repository.creates.get());
    fixture.scope.close();
    fixture.assertDrained();
  }

  @Test
  void insufficientPreparationBudgetFailsBeforeAnyDurableWrite() throws Exception {
    var repository = new DelayedResultRepository();
    var fixture = new Fixture(repository, 100);
    CompletionException failed = assertThrows(CompletionException.class,
        () -> fixture.preparation.prepare(command(), binding(), Optional.empty(),
            fixture.scope, fixture.account, PutCommitPreparationTest::completedRead).join());
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
        assertInstanceOf(BackendFailure.class, failed.getCause()).category());
    assertEquals(0, repository.creates.get());
    fixture.scope.close();
    fixture.assertDrained();
  }

  @Test
  void cancellationKeepsPreparationMemoryUntilThePhysicalWriteStops() throws Exception {
    var repository = new DelayedResultRepository();
    var fixture = new Fixture(repository, 4L << 20);
    var waiting = fixture.preparation.prepare(command(), binding(), Optional.empty(),
        fixture.scope, fixture.account, PutCommitPreparationTest::completedRead);
    assertEquals(1, repository.creates.get());
    assertTrue(waiting.cancel(false));
    fixture.scope.cancel();
    assertFalse(fixture.scope.cleanupCompletion().toCompletableFuture().isDone());
    assertTrue(fixture.budgets.snapshot().allocatedBytes() > 0);

    repository.physicalWrite.complete(null);
    fixture.assertDrained();
  }

  private static ByteKey key() { return ByteKey.copyOf(new byte[] {1}); }

  private static CompletableFuture<Void> completedRead() {
    return CompletableFuture.completedFuture(null);
  }

  private static PutCommand command() {
    return new PutCommand(key(), new byte[] {2}, 0, false, false, false);
  }

  private static InvocationBinding binding() {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
  }

  private static <T> T unused(Class<T> type) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
        (proxy, method, arguments) -> {
          throw new AssertionError("unexpected " + type.getSimpleName() + "." + method.getName());
        }));
  }

  private static final class Fixture {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final PutCommitPreparation preparation;

    private Fixture(ResultRepository results, long candidateCapacity) throws Exception {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 16L << 20);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope,
          new BudgetManager.Reservation(0, 1, 1L << 20, 1, candidateCapacity));
      preparation = new PutCommitPreparation(new ResultPreparation(results), new ResultCatalog(results),
          new ManifestPreparation(unused(ManifestRepository.class),
              new ManifestPreparation.Limits(1, 1024, 1, 2048)),
          new InvocationService(unused(InvocationRepository.class)));
    }

    private void assertDrained() throws Exception {
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      scope.physicalProducersCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }
  }

  private static final class DelayedResultRepository implements ResultRepository {
    private final AtomicInteger creates = new AtomicInteger();
    private final CompletableFuture<Void> physicalWrite = new CompletableFuture<>();

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ResultPage page, RequestScope scope) {
      creates.incrementAndGet();
      var visibleWrite = new CompletableFuture<SystemRecordStore.CreateResult>();
      scope.trackPhysicalProducer(physicalWrite);
      scope.registerCleanup(() -> visibleWrite.cancel(false));
      return visibleWrite;
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory directory, RequestScope scope) {
      throw new AssertionError("unexpected result directory write");
    }

    @Override
    public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      throw new AssertionError("unexpected result page read");
    }

    @Override
    public CompletableFuture<Optional<ResultDirectory>> readDirectory(
        ResultIdentity identity, RequestScope scope) {
      throw new AssertionError("unexpected result directory read");
    }
  }
}
