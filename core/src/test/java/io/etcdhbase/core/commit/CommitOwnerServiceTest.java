package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CommitOwnerServiceTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);

  @Test
  void concurrentClaimsProduceOneOwnerAndExpiredLeaseFencesTheOldOwner() {
    var barrier = new CyclicBarrier(2);
    var control = new InMemoryControlStore(CommitState.initial(FINGERPRINT, 1), barrier);
    var service = new CommitOwnerService(control);

    var first = CompletableFuture.supplyAsync(() -> service.claim("owner-a", 1, 10,
        RequestScope.withoutDeadline()).join());
    var second = CompletableFuture.supplyAsync(() -> service.claim("owner-b", 1, 10,
        RequestScope.withoutDeadline()).join());

    Optional<CommitOwnerService.OwnerToken> firstToken = first.join();
    Optional<CommitOwnerService.OwnerToken> secondToken = second.join();
    assertTrue(firstToken.isPresent() ^ secondToken.isPresent());

    var oldOwner = firstToken.orElseGet(secondToken::orElseThrow);
    var takeover = service.claim("owner-c", 10, 20, RequestScope.withoutDeadline()).join();
    assertTrue(takeover.isPresent());
    assertEquals(oldOwner.epoch() + 1, takeover.orElseThrow().epoch());
    assertFalse(service.heartbeat(oldOwner, 30, RequestScope.withoutDeadline()).join());
  }

  private static final class InMemoryControlStore implements CommitControlStore {
    private final AtomicReference<ControlSnapshot> current;
    private final CyclicBarrier initialReadBarrier;
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger tokens = new AtomicInteger();

    private InMemoryControlStore(CommitState initial, CyclicBarrier initialReadBarrier) {
      this.current = new AtomicReference<>(snapshot(initial));
      this.initialReadBarrier = initialReadBarrier;
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      if (reads.incrementAndGet() <= 2 && initialReadBarrier != null) {
        try {
          initialReadBarrier.await();
        } catch (Exception failure) {
          return CompletableFuture.failedFuture(failure);
        }
      }
      return CompletableFuture.completedFuture(Optional.of(current.get()));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      synchronized (current) {
        if (current.get().state() != observed.state()) {
          return CompletableFuture.completedFuture(false);
        }
        current.set(snapshot(replacement));
        return CompletableFuture.completedFuture(true);
      }
    }

    private ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, new byte[] {(byte) tokens.incrementAndGet()});
    }
  }
}
