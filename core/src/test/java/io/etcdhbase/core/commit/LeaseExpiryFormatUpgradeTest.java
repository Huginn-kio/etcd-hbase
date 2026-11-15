package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class LeaseExpiryFormatUpgradeTest {
  @Test
  void upgradeFencesBindingWriterAndResolvesLostControlCasResponse() {
    CommitState binding = CommitState.initial(ByteKey.copyOf(new byte[32]), 1)
        .claimOwner("owner", 1, 100).upgradeAuxiliaryFormat("owner", 1, 2)
        .upgradeLeaseBindingFormat("owner", 2, 3);
    var store = new Store(binding);
    var owner = new CommitOwnerService.OwnerToken("owner", 3);
    try (var scope = RequestScope.withoutDeadline()) {
      var oldSnapshot = store.read(scope).join().orElseThrow();
      store.loseResponse = true;
      var token = new LeaseExpiryFormatUpgrade(store).upgrade(owner, 4, scope).join();

      assertEquals(4, token.epoch());
      assertEquals(CommitState.LEASE_EXPIRY_FORMAT_VERSION, store.current.formatVersion());
      assertThrows(IllegalStateException.class,
          () -> store.current.heartbeat("owner", owner.epoch(), 200));
      assertFalse(store.compareAndSet(oldSnapshot, binding.heartbeat("owner", 3, 200), scope).join());
      assertThrows(CompletionException.class,
          () -> new LeaseExpiryFormatUpgrade(store).upgrade(owner, 4, scope).join());
    }
  }

  @Test
  void competingControlRowChangeCannotBeReportedAsSuccessfulUpgrade() {
    CommitState binding = CommitState.initial(ByteKey.copyOf(new byte[32]), 1)
        .claimOwner("owner", 1, 100).upgradeAuxiliaryFormat("owner", 1, 2)
        .upgradeLeaseBindingFormat("owner", 2, 3);
    var store = new Store(binding);
    store.reject = true;
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> new LeaseExpiryFormatUpgrade(store)
              .upgrade(new CommitOwnerService.OwnerToken("owner", 3), 4, scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          ((BackendFailure) failure.getCause()).category());
      assertEquals(CommitState.LEASE_BINDING_FORMAT_VERSION, store.current.formatVersion());
    }
  }

  private static final class Store implements CommitControlStore {
    private CommitState current;
    private int version = 1;
    private boolean loseResponse;
    private boolean reject;

    private Store(CommitState initial) { current = initial; }

    @Override
    public synchronized CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(current, new byte[] {(byte) version})));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (reject || observed.comparisonToken()[0] != (byte) version) {
        return CompletableFuture.completedFuture(false);
      }
      current = replacement;
      version++;
      if (loseResponse) {
        loseResponse = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }
}
