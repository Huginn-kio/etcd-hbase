package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class M3HistoryProcessCleanupTest {
  @Test
  void interruptionStillStopsAllProcessesAndPreservesOriginalFailure() {
    var first = new ControlledProcess(null);
    var second = new ControlledProcess(null);
    var interrupted = new InterruptedException("interrupted while reaping driver");
    var driver = new ControlledProcess(interrupted);
    var original = new IllegalStateException("fixture failed");
    try {
      var thrown = assertThrows(IllegalStateException.class, () -> {
        try (var cleanup = new M3HistoryProcessCleanup(() -> new Process[] {driver, second, first})) {
          throw original;
        }
      });
      assertSame(original, thrown);
      assertEquals(1, thrown.getSuppressed().length);
      assertSame(interrupted, thrown.getSuppressed()[0]);
      assertTrue(driver.destroyed && second.destroyed && first.destroyed);
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void timeoutDoesNotPreventRemainingReapAndIsReported() {
    var first = new ControlledProcess(null);
    var driver = new ControlledProcess(new AssertionError("reap timed out"));
    var error = assertThrows(AssertionError.class, () -> {
      try (var cleanup = new M3HistoryProcessCleanup(() -> new Process[] {driver, first})) {
      }
    });
    assertEquals("fixture process did not terminate", error.getMessage());
    assertTrue(driver.destroyed && first.destroyed);
  }

  @Test
  void multipleCleanupFailuresRemainVisible() {
    var first = new ControlledProcess(new AssertionError("first timeout"));
    var driver = new ControlledProcess(new AssertionError("driver timeout"));
    var error = assertThrows(AssertionError.class, () -> {
      try (var cleanup = new M3HistoryProcessCleanup(() -> new Process[] {driver, first})) {
        // The fixture body succeeds, so the first cleanup error remains primary.
      }
    });
    assertEquals(1, error.getSuppressed().length);
    assertTrue(driver.destroyed && first.destroyed);
  }

  private static final class ControlledProcess extends Process {
    private final Throwable waitFailure;
    private boolean destroyed;
    ControlledProcess(Throwable waitFailure) { this.waitFailure = waitFailure; }
    @Override public OutputStream getOutputStream() { return OutputStream.nullOutputStream(); }
    @Override public InputStream getInputStream() { return InputStream.nullInputStream(); }
    @Override public InputStream getErrorStream() { return InputStream.nullInputStream(); }
    @Override public int waitFor() throws InterruptedException { throw new UnsupportedOperationException(); }
    @Override public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
      if (waitFailure instanceof InterruptedException interrupted) throw interrupted;
      if (waitFailure instanceof Error) return false;
      return true;
    }
    @Override public int exitValue() { throw new IllegalThreadStateException(); }
    @Override public void destroy() { destroyed = true; }
    @Override public Process destroyForcibly() { destroyed = true; return this; }
    @Override public boolean isAlive() { return true; }
  }
}
