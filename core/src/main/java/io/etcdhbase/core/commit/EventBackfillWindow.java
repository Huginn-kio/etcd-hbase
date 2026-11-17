package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Durable protection for the entire captured legacy history while backfill is in progress. */
public final class EventBackfillWindow implements AutoCloseable {
  private final EventIndexBackfillCheckpoint captured;
  private final ProtectedEventCursor pin;
  private final CommitControlStore control;
  private final RequestScope scope;
  private final Optional<ByteKey> registryNamespace;
  private final boolean initialCapture;

  private EventBackfillWindow(EventIndexBackfillCheckpoint captured, ProtectedEventCursor pin,
      CommitControlStore control, RequestScope scope, Optional<ByteKey> registryNamespace,
      boolean initialCapture) {
    this.captured = captured;
    this.pin = pin;
    this.control = control;
    this.scope = scope;
    this.registryNamespace = registryNamespace;
    this.initialCapture = initialCapture;
  }

  /** The checkpoint must first be resolved from a selected M2 auxiliary version. */
  public static CompletableFuture<EventBackfillWindow> open(EventHistoryPinManager pins,
      CommitControlStore control, EventIndexBackfillCheckpoint checkpoint, ByteKey authorizationDigest,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return open(pins, control, checkpoint, authorizationDigest, scope, budget, false);
  }

  /** Registers the initial window before its checkpoint is selected by the owner. */
  public static CompletableFuture<EventBackfillWindow> openCapture(EventHistoryPinManager pins,
      CommitControlStore control, EventIndexBackfillCheckpoint checkpoint, ByteKey authorizationDigest,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    if (checkpoint.nextSequence() != checkpoint.firstSequence()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("capture must start at the first sequence"));
    }
    return open(pins, control, checkpoint, authorizationDigest, scope, budget, true);
  }

  private static CompletableFuture<EventBackfillWindow> open(EventHistoryPinManager pins,
      CommitControlStore control, EventIndexBackfillCheckpoint checkpoint, ByteKey authorizationDigest,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget, boolean capture) {
    Objects.requireNonNull(pins, "pins");
    Objects.requireNonNull(control, "control");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (!capture && checkpoint.complete() || !budget.belongsTo(scope)
        || !checkpoint.registryIdentity().equals(pins.registryIdentity())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event backfill protection request"));
    }
    long firstRevision = checkpoint.firstBaseRevision() == Long.MAX_VALUE
        ? Long.MAX_VALUE : checkpoint.firstBaseRevision() + 1;
    var requirement = new EventHistoryPinManager.Requirement(firstRevision, checkpoint.firstSequence(),
        authorizationDigest, Optional.empty(), Optional.empty());
    var output = new CompletableFuture<EventBackfillWindow>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    pins.register(requirement, scope).whenComplete((registered, failure) -> {
      if (failure != null) {
        scope.close();
        output.completeExceptionally(failure);
        return;
      }
      var pin = new ProtectedEventCursor(registered, scope);
      var window = new EventBackfillWindow(checkpoint, pin, control, scope, pins.namespace(), capture);
      if (output.isDone()) {
        window.close();
        return;
      }
      try {
        CompletableFuture<Void> checked = capture
            ? window.checkHistory(checkpoint, checkpoint.ownerEpoch(), budget)
            : window.check(checkpoint, Optional.empty(), Optional.empty(), budget);
        checked.whenComplete((ignored, checkFailure) -> {
          if (checkFailure != null) {
            window.close();
            output.completeExceptionally(checkFailure);
          } else if (!output.complete(window)) {
            window.close();
          }
        });
      } catch (RuntimeException | Error problem) {
        window.close();
        output.completeExceptionally(problem);
        if (problem instanceof Error fatal) throw fatal;
      }
    });
    return output;
  }

  /** Revalidates both the frozen window and the owner/base for an immediately pending M2 commit. */
  public CompletableFuture<Void> checkPublication(EventIndexBackfillCheckpoint checkpoint,
      long ownerEpoch, long commitSequence, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    if (ownerEpoch <= 0 || commitSequence <= 0 || checkpoint.ownerEpoch() != ownerEpoch
        || checkpoint.sequence() != commitSequence) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid backfill publication fence"));
    }
    return check(checkpoint, Optional.of(new Fence(ownerEpoch, commitSequence)),
        Optional.of(ownerEpoch), budget);
  }

  /** Revalidates the fixed legacy interval without requiring the old M2 base to remain current. */
  CompletableFuture<Void> checkHistory(EventIndexBackfillCheckpoint checkpoint, long ownerEpoch,
      ManifestCatalog.ProofReadBudget budget) {
    if (ownerEpoch <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid backfill history owner"));
    }
    return check(checkpoint, Optional.empty(), Optional.of(ownerEpoch), budget);
  }

  private CompletableFuture<Void> check(EventIndexBackfillCheckpoint checkpoint,
      Optional<Fence> fence, Optional<Long> expectedOwnerEpoch, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(budget, "budget");
    if (!budget.belongsTo(scope) || checkpoint.firstSequence() != captured.firstSequence()
        || checkpoint.upperSequence() != captured.upperSequence()
        || checkpoint.firstBaseRevision() != captured.firstBaseRevision()
        || checkpoint.upperRevision() != captured.upperRevision()
        || checkpoint.compactRevision() != captured.compactRevision()
        || !checkpoint.registryIdentity().equals(captured.registryIdentity())
        || checkpoint.nextSequence() < captured.nextSequence()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("event backfill window differs from its protection"));
    }
    return pin.renew().thenCompose(ignored -> budget.readMeasured(ManifestCatalog.maximumProofReadPageBytes(),
        () -> control.readWithSize(scope))).thenCompose(snapshot -> {
      CommitState state = snapshot.orElseThrow(
          () -> new IllegalStateException("authoritative commit control is missing")).state();
      if (!state.supportsEventRevisionIndex() || state.compactRevision() != captured.compactRevision()
          || registryNamespace.isPresent() && !registryNamespace.equals(state.eventRegistryNamespace())
          || state.ownerEpoch() < checkpoint.ownerEpoch()
          || expectedOwnerEpoch.filter(epoch -> state.ownerEpoch() != epoch).isPresent()
          // The v5 owner-epoch fence prevents legacy writers from selecting after upgrade, and
          // v5 key-change indexes are materialized before finalization. Newer finalized sequences
          // may therefore sit outside this fixed legacy interval; compact and registry fences stay strict.
          || !initialCapture && state.finalizedSequence() < captured.sequence()
          || state.finalizedSequence() < checkpoint.upperSequence()
          || state.finalizedRevision() < checkpoint.upperRevision()) {
        throw new IllegalStateException("event backfill protection no longer covers the captured window");
      }
      if (fence.isPresent()) {
        Fence value = fence.orElseThrow();
        if (state.ownerEpoch() != value.epoch() || state.publishedSequence() == Long.MAX_VALUE
            || state.publishedSequence() + 1 != value.sequence()) {
          throw new IllegalStateException("event backfill owner or publication base changed");
        }
      }
      return pin.renew();
    });
  }

  public CompletionStage<Void> releaseCompletion() { return pin.releaseCompletion(); }
  public Optional<ByteKey> registryNamespace() { return registryNamespace; }
  public ByteKey registryIdentity() { return captured.registryIdentity(); }
  @Override public void close() { pin.close(); }
  private record Fence(long epoch, long sequence) {}
}
