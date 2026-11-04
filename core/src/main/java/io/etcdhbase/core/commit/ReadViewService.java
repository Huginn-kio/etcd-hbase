package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Captures, pins, then revalidates one control-row read barrier before data access. */
public final class ReadViewService {
  private final CommitControlStore control;
  private final ManifestCatalog manifests;
  private final ReadViewPinRegistry pins;
  private final ByteKey expectedProfileFingerprint;

  public ReadViewService(CommitControlStore control, ManifestCatalog manifests,
      ReadViewPinRegistry pins, ByteKey expectedProfileFingerprint) {
    this.control = Objects.requireNonNull(control, "control");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.pins = Objects.requireNonNull(pins, "pins");
    this.expectedProfileFingerprint = Objects.requireNonNull(
        expectedProfileFingerprint, "expectedProfileFingerprint");
    if (expectedProfileFingerprint.length() != 32) {
      throw new IllegalArgumentException("profile fingerprint must be SHA-256");
    }
  }

  /** A missing target selects the latest published revision, including a selected pending commit. */
  public CompletableFuture<PinnedReadView> open(OptionalLong requestedRevision,
      ByteKey authorizationScopeDigest, RequestScope scope) {
    Objects.requireNonNull(requestedRevision, "requestedRevision");
    Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
    Objects.requireNonNull(scope, "scope");
    if (authorizationScopeDigest.length() != 32) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("authorization scope digest must be SHA-256"));
    }
    return readControl(scope).thenCompose(snapshot -> {
      CommitState state = snapshot.state();
      requireProfile(state);
      long target = requestedRevision.isPresent()
          ? requestedRevision.getAsLong() : state.publishedRevision();
      return captureSelection(state, target, authorizationScopeDigest, scope);
    }).thenCompose(capture -> registerAndRecheck(capture, scope));
  }

  private CompletableFuture<Capture> captureSelection(CommitState state, long target,
      ByteKey authorizationScopeDigest, RequestScope scope) {
    Optional<PublicationSlot> selected = state.publication()
        .filter(slot -> slot.revision() <= target);
    if (selected.isEmpty()) {
      return CompletableFuture.completedFuture(new Capture(ReadView.capture(state, target,
          authorizationScopeDigest, Optional.empty(), Optional.empty()), Optional.empty()));
    }
    PublicationSlot slot = selected.orElseThrow();
    CommitDecision decision = CommitDecision.from(slot);
    return manifests.readRoot(slot.manifestRoot(), scope).thenApply(rootValue -> {
      ManifestRoot root = rootValue.orElseThrow(
          () -> new IllegalStateException("selected publication manifest is missing"));
      validateSelectedRoot(slot, root);
      ReadView view = ReadView.capture(state, target, authorizationScopeDigest,
          Optional.of(decision), Optional.of(root));
      return new Capture(view, Optional.of(root));
    });
  }

  private CompletableFuture<PinnedReadView> registerAndRecheck(Capture capture,
      RequestScope scope) {
    requireActive(scope);
    return pins.register(capture.view(), scope).thenCompose(returnedPin -> {
      ReadViewPinRegistry.Pin pin = Objects.requireNonNull(returnedPin, "pin registry returned null");
      long generation = pin.generation();
      if (generation <= 0) {
        pin.close();
        return CompletableFuture.failedFuture(
            new IllegalStateException("pin registry returned a non-positive generation"));
      }
      PinnedReadView pinned;
      try {
        pinned = new PinnedReadView(capture.view().withPinGeneration(generation), pin);
      } catch (RuntimeException | Error failure) {
        // A failed construction must release its registered pin before preserving a fatal cause.
        try {
          pin.close();
        } catch (RuntimeException | Error cleanup) {
          // Keep the construction failure primary if releasing the pin also fails.
          if (failure != cleanup) failure.addSuppressed(cleanup);
        }
        throw failure;
      }
      scope.registerCleanup(pinned);
      return recheckControl(capture.view(), scope)
          .thenCompose(ignored -> recheckRoot(capture, scope))
          .thenApply(ignored -> {
            requireActive(scope);
            if (!pinned.isOpen()) {
              throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
            }
            return pinned;
          })
          .whenComplete((ignored, failure) -> {
            if (failure != null) pinned.close();
          });
    });
  }

  private CompletableFuture<Void> recheckControl(ReadView view, RequestScope scope) {
    return readControl(scope).thenAccept(snapshot -> {
      CommitState current = snapshot.state();
      requireProfile(current);
      if (current.compactRevision() > view.targetRevision()) {
        throw new IllegalStateException("read view crossed the compact revision boundary");
      }
    });
  }

  private CompletableFuture<Void> recheckRoot(Capture capture, RequestScope scope) {
    if (capture.root().isEmpty()) return CompletableFuture.completedFuture(null);
    ManifestRoot expected = capture.root().orElseThrow();
    return manifests.readRoot(expected.systemIdentity(), scope).thenAccept(rootValue -> {
      ManifestRoot current = rootValue.orElseThrow(
          () -> new IllegalStateException("pinned publication manifest is missing"));
      if (!current.systemIdentity().equals(expected.systemIdentity())
          || !current.digest().equals(expected.digest())) {
        throw new IllegalStateException("pinned publication manifest identity changed");
      }
    });
  }

  private CompletableFuture<ControlSnapshot> readControl(RequestScope scope) {
    requireActive(scope);
    return control.read(scope).thenApply(snapshot -> snapshot.orElseThrow(
        () -> new IllegalStateException("authoritative M2 control state is missing")));
  }

  private void requireProfile(CommitState state) {
    if (!state.profileFingerprint().equals(expectedProfileFingerprint)) {
      throw new IllegalStateException("control state belongs to a different storage profile");
    }
  }

  private static void validateSelectedRoot(PublicationSlot slot, ManifestRoot root) {
    if (!root.transactionId().equals(slot.transactionId())
        || root.ownerEpoch() != slot.ownerEpoch()
        || root.baseSequence() != slot.baseSequence()
        || root.baseRevision() != slot.baseRevision()
        || !root.systemIdentity().equals(slot.manifestRoot())
        || !root.digest().equals(slot.manifestDigest())
        || root.containsKeyChanges() != slot.containsKeyChanges()) {
      throw new IllegalStateException("selected publication differs from its sealed manifest");
    }
  }

  private static void requireActive(RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
  }

  private record Capture(ReadView view, Optional<ManifestRoot> root) {}
}
