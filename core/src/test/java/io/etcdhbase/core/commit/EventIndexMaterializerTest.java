package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventIndexMaterializerTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {1});

  @Test
  void persistsSelectedIndexAndReplaysIdentically() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      EventIndexMaterializer materializer = new EventIndexMaterializer(indexes);
      materializer.apply(fixture.decision, fixture.root, scope).join();
      materializer.apply(fixture.decision, fixture.root, scope).join();
      assertEquals(EventRevisionIndex.fromSelected(fixture.decision, fixture.root), indexes.stored);
      assertEquals(2, indexes.writes);
    }
  }

  @Test
  void resolvesUnknownWriteFromDurableValueAndRejectsConflicts() {
    Fixture fixture = new Fixture();
    MemoryIndexes indexes = new MemoryIndexes();
    indexes.unknownOnce = true;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      EventIndexMaterializer materializer = new EventIndexMaterializer(indexes);
      materializer.apply(fixture.decision, fixture.root, scope).join();
      assertEquals(1, indexes.writes);
      Fixture conflicting = new Fixture();
      indexes.stored = EventRevisionIndex.fromSelected(conflicting.decision, conflicting.root);
      assertThrows(CompletionException.class,
          () -> new EventIndexMaterializer(indexes).apply(fixture.decision, fixture.root, scope).join());
    }
  }

  @Test
  void metadataOnlyDoesNotCreateAnIndex() {
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 0, 0, Optional.empty());
    CommitDecision decision = new CommitDecision(transaction, InvocationId.create(), 3, 10, 10,
        root.systemIdentity(), root.digest(), false);
    MemoryIndexes indexes = new MemoryIndexes();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      new EventIndexMaterializer(indexes).apply(decision, root, scope).join();
      assertEquals(0, indexes.writes);
      assertTrue(indexes.stored == null);
    }
  }

  private static final class Fixture {
    private final TransactionId transaction = TransactionId.create();
    private final ManifestChildReference candidates =
        new ManifestChildReference(KEY, DIGEST, 0, 0, KEY, KEY, 1, 0, 1, 0);
    private final EventReference events = new EventReference(KEY, DIGEST, 0, 0, 1, 1, 1, 0);
    private final ManifestRoot root = ManifestRoot.create(transaction, 1, 2, 10, 1, 0,
        Optional.of(candidates), 1, 1, Optional.of(events));
    private final CommitDecision decision = new CommitDecision(transaction, InvocationId.create(),
        3, 10, 11, root.systemIdentity(), root.digest(), true);
  }

  private static final class MemoryIndexes implements EventRevisionIndexRepository {
    private EventRevisionIndex stored;
    private int writes;
    private boolean unknownOnce;

    @Override public CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(stored));
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        EventRevisionIndex index, RequestScope scope) {
      writes++;
      if (stored != null) return CompletableFuture.completedFuture(stored.equals(index)
          ? SystemRecordStore.CreateResult.ALREADY_PRESENT : SystemRecordStore.CreateResult.CONFLICT);
      stored = index;
      if (unknownOnce) {
        unknownOnce = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.TIMEOUT));
      }
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }
}
