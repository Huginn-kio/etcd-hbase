package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventIndexBackfillMaterializerTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0});
  private static final ByteKey NAMESPACE = ByteKey.copyOf(new byte[16]);
  private static final ByteKey ROOT = ByteKey.copyOf(new byte[] {1});
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void initialPreflightAcceptsNewerFinalizedSequencesOutsideCapturedLegacyWindow() throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var checkpoint = new EventIndexBackfillCheckpoint(7, 4, 1, 2, 1, 1, 1, 2, 1, REGISTRY);
      var first = decision(1, 1, 2, true);
      var last = decision(2, 2, 2, false);
      var state = state(7, 3, 3, 2, 2, 1);
      var materializer = materializer(state, first, last);

      assertDoesNotThrow(() -> materializer.preflight(checkpoint, OptionalLong.empty(), scope).join());
    }
  }

  @Test
  void initialPreflightStillRejectsOwnerCompactAndCapturedUpperChanges() throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var checkpoint = new EventIndexBackfillCheckpoint(7, 4, 1, 2, 1, 1, 1, 2, 1, REGISTRY);
      var first = decision(1, 1, 2, true);
      var last = decision(2, 2, 2, false);

      assertThrows(CompletionException.class, () -> materializer(
          state(8, 3, 3, 2, 2, 1), first, last)
          .preflight(checkpoint, OptionalLong.empty(), scope).join());
      assertThrows(CompletionException.class, () -> materializer(
          state(7, 3, 3, 2, 2, 2), first, last)
          .preflight(checkpoint, OptionalLong.empty(), scope).join());
      assertThrows(CompletionException.class, () -> materializer(
          state(7, 1, 1, 1, 1, 1), first, last)
          .preflight(checkpoint, OptionalLong.empty(), scope).join());
    }
  }

  private static EventIndexBackfillMaterializer materializer(CommitState state,
      CommitDecision first, CommitDecision last) {
    var control = new CommitControlStore() {
      @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope ignored) {
        return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
      }

      @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope ignored) {
        throw new AssertionError("unexpected control initialization");
      }

      @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot expected,
          CommitState replacement, RequestScope ignored) {
        throw new AssertionError("unexpected control mutation");
      }
    };
    var decisions = new CommitDecisionRepository() {
      @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope ignored) {
        return CompletableFuture.completedFuture(Optional.ofNullable(sequence == 1 ? first : last));
      }

      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          CommitDecision decision, RequestScope ignored) {
        throw new AssertionError("unexpected decision creation");
      }
    };
    var manifests = (ManifestRepository) Proxy.newProxyInstance(ManifestRepository.class.getClassLoader(),
        new Class<?>[] {ManifestRepository.class}, (proxy, method, arguments) -> {
          if (method.getName().equals("readRoot")) {
            return CompletableFuture.completedFuture(Optional.empty());
          }
          throw new AssertionError("unexpected manifest read: " + method.getName());
        });
    var indexes = new EventRevisionIndexRepository() {
      @Override public CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope ignored) {
        throw new AssertionError("unexpected index read");
      }

      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          EventRevisionIndex index, RequestScope ignored) {
        throw new AssertionError("unexpected index creation");
      }
    };
    return new EventIndexBackfillMaterializer(control, decisions, new ManifestCatalog(manifests), indexes);
  }

  private static CommitState state(long ownerEpoch, long publishedSequence, long finalizedSequence,
      long publishedRevision, long finalizedRevision, long compactRevision) {
    return CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
        Optional.of("owner"), ownerEpoch, 10_000, 1, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, Optional.empty(), compactRevision,
        Optional.of(NAMESPACE), Optional.empty(), 1);
  }

  private static CommitDecision decision(long sequence, long baseRevision, long revision,
      boolean containsKeyChanges) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), sequence, baseRevision,
        revision, ROOT, DIGEST, containsKeyChanges);
  }
}
