package io.etcdhbase.core.commit;

import java.util.Objects;
import java.util.Optional;

/** A captured immutable read view whose retention pin is owned by the request scope. */
public final class PinnedReadView implements AutoCloseable {
  private final ReadView view;
  private final ReadViewPinRegistry.Pin pin;
  private boolean closeRequested;
  private boolean pinClosed;
  private int activeReaders;

  PinnedReadView(ReadView view, ReadViewPinRegistry.Pin pin) {
    this.view = Objects.requireNonNull(view, "view");
    this.pin = Objects.requireNonNull(pin, "pin");
    if (view.pinGeneration() != pin.generation()) {
      throw new IllegalArgumentException("read view and pin generations differ");
    }
  }

  public ReadView view() { return view; }
  public Optional<ManifestRoot> selectedRoot() { return view.selectedRoot(); }
  public synchronized boolean isOpen() { return !closeRequested && !pinClosed; }

  synchronized ReaderLease retainReader() {
    if (closeRequested || pinClosed) {
      throw new IllegalStateException("read view pin has already been released");
    }
    activeReaders = Math.addExact(activeReaders, 1);
    return new ReaderLease(this);
  }

  @Override
  public void close() {
    boolean release;
    synchronized (this) {
      closeRequested = true;
      release = activeReaders == 0 && !pinClosed;
      if (release) pinClosed = true;
    }
    if (release) pin.close();
  }

  private void releaseReader() {
    boolean release;
    synchronized (this) {
      if (activeReaders == 0) throw new IllegalStateException("read view reader lease underflow");
      activeReaders--;
      release = closeRequested && activeReaders == 0 && !pinClosed;
      if (release) pinClosed = true;
    }
    if (release) pin.close();
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
