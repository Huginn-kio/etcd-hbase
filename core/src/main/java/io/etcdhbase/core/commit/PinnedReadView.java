package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A captured immutable read view whose retention pin is owned by the request scope. */
public final class PinnedReadView implements AutoCloseable {
  private final ReadView view;
  private final ReadViewPinRegistry.Pin pin;
  private final RequestScope scope;
  private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
  private boolean closeRequested;
  private boolean pinClosed;
  private int activeReaders;

  PinnedReadView(ReadView view, ReadViewPinRegistry.Pin pin, RequestScope scope) {
    this.view = Objects.requireNonNull(view, "view");
    this.pin = Objects.requireNonNull(pin, "pin");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (view.pinGeneration() != pin.generation()) {
      throw new IllegalArgumentException("read view and pin generations differ");
    }
  }

  public ReadView view() { return view; }
  public Optional<ManifestRoot> selectedRoot() { return view.selectedRoot(); }
  public synchronized boolean isOpen() { return !closeRequested && !pinClosed; }
  CompletionStage<Void> cleanupCompletion() { return cleanupCompletion.minimalCompletionStage(); }

  synchronized ReaderLease retainReader() {
    if (closeRequested || pinClosed) {
      throw new IllegalStateException("read view pin has already been released");
    }
    activeReaders = Math.addExact(activeReaders, 1);
    return new ReaderLease(this);
  }

  @Override
  public void close() {
    synchronized (this) {
      closeRequested = true;
    }
    // Closing the view seals its producer set. Release the pin only after it has stopped.
    scope.close();
    releasePinIfReady();
  }

  private void releaseReader() {
    synchronized (this) {
      if (activeReaders == 0) throw new IllegalStateException("read view reader lease underflow");
      activeReaders--;
    }
    releasePinIfReady();
  }

  private void releasePinIfReady() {
    synchronized (this) {
      if (!closeRequested || activeReaders != 0 || pinClosed) return;
      pinClosed = true;
    }
    scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) -> {
      if (producerFailure != null) {
        cleanupCompletion.completeExceptionally(producerFailure);
        return;
      }
      try {
        pin.close();
        cleanupCompletion.complete(null);
      } catch (RuntimeException | Error failure) {
        cleanupCompletion.completeExceptionally(failure);
      }
    });
  }

  static final class ReaderLease implements AutoCloseable {
    private final PinnedReadView owner;
    private boolean closed;

    private ReaderLease(PinnedReadView owner) { this.owner = owner; }

    @Override
    public void close() {
      synchronized (this) {
        if (closed) return;
        closed = true;
      }
      owner.releaseReader();
    }
  }
}
