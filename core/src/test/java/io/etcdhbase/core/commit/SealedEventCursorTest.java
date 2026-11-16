package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SealedEventCursorTest {
  @Test
  void readsEventsInOrderWithPauseAndBoundedPageReads() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      var firstBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(0, cursor.next(firstBudget).join().orElseThrow().ordinal());
      assertEquals(3, firstBudget.pages());
      var secondBudget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(1, cursor.next(secondBudget).join().orElseThrow().ordinal());
      assertTrue(cursor.next(secondBudget).join().isEmpty());
    }
  }

  @Test
  void missingPageAndExhaustedBudgetFailWithoutAdvancingCursor() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      var insufficient = new ManifestCatalog.ProofReadBudget(scope, account, 1, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(insufficient).join());
      fixture.missingPage = true;
      var missing = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(missing).join());
      fixture.missingPage = false;
      var retry = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertEquals(0, cursor.next(retry).join().orElseThrow().ordinal());
      cursor.close();
      assertThrows(CompletionException.class, () -> cursor.next(retry).join());
    }
  }

  @Test
  void corruptEventAndLateCompletionAfterCloseAreNotDelivered() throws Exception {
    Fixture fixture = new Fixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
         BudgetManager.Account account = fixture.account(scope);
         SealedEventCursor cursor = fixture.cursor(scope)) {
      fixture.wrongEvent = true;
      var corrupt = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      assertThrows(CompletionException.class, () -> cursor.next(corrupt).join());
      fixture.wrongEvent = false;
      fixture.pendingEvent = new CompletableFuture<>();
      var delayed = new ManifestCatalog.ProofReadBudget(scope, account, 3, 512L << 20);
      var result = cursor.next(delayed);
      cursor.close();
      fixture.pendingEvent.complete(Optional.of(fixture.first));
      assertThrows(CompletionException.class, result::join);
    }
  }

  private static final class Fixture {
    private final TransactionId transaction = TransactionId.create();
    private final PreparedEvent first = event(0, (byte) 'a');
    private final PreparedEvent second = event(1, (byte) 'b');
    private final EventPage page = EventPage.create(transaction, 0,
        List.of(PreparedEventReference.from(first), PreparedEventReference.from(second)));
    private final EventDirectoryPage directory = EventDirectoryPage.create(transaction, 0, 1,
        List.of(page.reference()));
    private final ByteKey key = ByteKey.copyOf(new byte[] {1});
    private final ByteKey digest = ByteKey.copyOf(new byte[32]);
    private final ManifestChildReference candidates =
        new ManifestChildReference(key, digest, 0, 0, key, key, 1, 0, 1, 0);
    private final ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 1, 0,
        Optional.of(candidates), 2, first.retainedBytes() + second.retainedBytes(),
        Optional.of(directory.reference()));
    private final CommitDecision decision = new CommitDecision(transaction, InvocationId.create(),
        3, 10, 11, root.systemIdentity(), root.digest(), true);
    private boolean missingPage;
    private boolean wrongEvent;
    private CompletableFuture<Optional<PreparedEvent>> pendingEvent;

    private PreparedEvent event(long ordinal, byte key) {
      byte[] value = {1};
      StoredRecord current = StoredRecord.live(ByteKey.copyOf(new byte[] {key}),
          new StorageMetadata(11, 11, 1, 0, StorageMetadata.State.LIVE, 1), value);
      return new PreparedEvent(transaction, 1, 2, 10, ordinal, PreparedEvent.Kind.PUT,
          current, Optional.empty());
    }

    private SealedEventCursor cursor(RequestScope scope) {
      ManifestRepository repository = (ManifestRepository) Proxy.newProxyInstance(
          ManifestRepository.class.getClassLoader(), new Class<?>[] {ManifestRepository.class},
          (proxy, method, arguments) -> switch (method.getName()) {
            case "readEventDirectoryPage" -> CompletableFuture.completedFuture(Optional.of(directory));
            case "readEventPage" -> CompletableFuture.completedFuture(missingPage
                ? Optional.empty() : Optional.of(page));
            case "readEvent" -> pendingEvent != null ? pendingEvent
                : CompletableFuture.completedFuture(Optional.of(wrongEvent ? second
                    : arguments[0].equals(first.systemIdentity()) ? first : second));
            default -> throw new UnsupportedOperationException(method.getName());
          });
      return new SealedEventCursor(repository, decision, root, scope);
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
      var manager = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
      var reads = new BudgetManager.ReadLimits(1_000_000, 8L << 30, 100_000, 65_536, 65_536);
      return manager.admit("public", scope, reservation, reads);
    }
  }
}
