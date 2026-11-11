package io.etcdhbase.server;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Reaps all owned fixture processes, retaining every cleanup failure. */
final class M3HistoryProcessCleanup implements AutoCloseable {
  private final Supplier<Process[]> processes;

  M3HistoryProcessCleanup(Supplier<Process[]> processes) { this.processes = processes; }

  @Override public void close() throws Exception {
    Throwable failure = null;
    boolean interrupted = Thread.interrupted();
    for (Process process : processes.get()) {
      try {
        stop(process);
      } catch (Throwable cleanupFailure) {
        if (cleanupFailure instanceof InterruptedException) interrupted = true;
        if (failure == null) failure = cleanupFailure;
        else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
    if (failure instanceof Exception exception) throw exception;
    if (failure instanceof Error error) throw error;
    if (failure != null) throw new AssertionError(failure);
  }

  private static void stop(Process process) throws InterruptedException {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      if (!process.waitFor(10, TimeUnit.SECONDS)) throw new AssertionError("fixture process did not terminate");
    }
  }
}
