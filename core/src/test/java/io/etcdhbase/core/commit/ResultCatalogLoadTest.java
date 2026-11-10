package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ResultCatalogLoadTest {
  @Test
  void returnsReadOnlyBytesAndHoldsOutputBudgetUntilTheConsumerCloses() throws Exception {
    Fixture fixture = new Fixture(false, 1_000_000);
    try (fixture) {
      var result = new ResultCatalog(fixture.repository)
          .load(fixture.reference, fixture.scope, fixture.account).join();
      var buffer = result.buffer();
      assertTrue(buffer.isReadOnly());
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      assertArrayEquals(new byte[] {1, 2, 3}, bytes);
      assertEquals(3, fixture.budgets.snapshot().allocatedBytes());
      result.close();
      result.close();
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      assertThrows(IllegalStateException.class, result::buffer);
    }
  }

  @Test
  void insufficientIoBudgetFailsAndReleasesOutputReservation() throws Exception {
    Fixture fixture = new Fixture(false, 1);
    try (fixture) {
      var failure = assertThrows(CompletionException.class, () -> new ResultCatalog(fixture.repository)
          .load(fixture.reference, fixture.scope, fixture.account).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void cancellationKeepsLeasesUntilTheActualProducerStops() throws Exception {
    Fixture fixture = new Fixture(true, 1_000_000);
    try (fixture) {
      var loading = new ResultCatalog(fixture.repository)
          .load(fixture.reference, fixture.scope, fixture.account);
      assertTrue(!loading.isDone());
      fixture.scope.cancel();
      assertThrows(CompletionException.class, loading::join);
      assertTrue(fixture.budgets.snapshot().allocatedBytes() > 0);
      fixture.repository.physical.complete(null);
      fixture.scope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void consumerDrainRetainsOutputAcrossRequestCancellation() throws Exception {
    Fixture fixture = new Fixture(false, 1_000_000);
    try (fixture) {
      var result = new ResultCatalog(fixture.repository)
          .load(fixture.reference, fixture.scope, fixture.account).join();
      var consumer = new CompletableFuture<Void>();
      assertTrue(result.retainForConsumer(consumer));
      var view = result.buffer();
      result.close();
      fixture.scope.cancel();
      assertEquals(3, fixture.budgets.snapshot().allocatedBytes());
      assertEquals(3, view.remaining());
      consumer.complete(null);
      fixture.scope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final Repository repository;
    private final ResultReference reference;

    private Fixture(boolean slow, long ioBudget) throws Exception {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, ioBudget, 100));
      ResultIdentity identity = ResultIdentity.from(InvocationBinding.begin(InvocationId.create(),
          ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1));
      ResultPage first = new ResultPage(identity, 0, new byte[] {1, 2});
      ResultPage second = new ResultPage(identity, 1, new byte[] {3});
      ResultDirectory directory = ResultDirectory.create(identity,
          List.of(ResultPageReference.from(first), ResultPageReference.from(second)));
      reference = directory.reference();
      repository = new Repository(directory, List.of(first, second), slow);
    }

    @Override public void close() { scope.close(); }
  }

  private static final class Repository implements ResultRepository {
    private final ResultDirectory directory;
    private final List<ResultPage> pages;
    private final boolean slow;
    private final CompletableFuture<Void> physical = new CompletableFuture<>();

    private Repository(ResultDirectory directory, List<ResultPage> pages, boolean slow) {
      this.directory = directory;
      this.pages = pages;
      this.slow = slow;
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ResultPage page, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(
        ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(directory));
    }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      if (!slow) return CompletableFuture.completedFuture(Optional.of(pages.get(index)));
      var request = new CompletableFuture<Optional<ResultPage>>();
      scope.trackPhysicalProducer(physical);
      scope.registerCleanup(() -> request.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED)));
      return request;
    }
  }
}
