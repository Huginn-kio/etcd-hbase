package io.etcdhbase.core.concurrent;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;

/** Runs reentrant continuations iteratively on their completing thread. */
public final class ContinuationExecutor implements Executor {
  private final ThreadLocal<ArrayDeque<Runnable>> continuations = new ThreadLocal<>();

  @Override
  public void execute(Runnable command) {
    ArrayDeque<Runnable> queue = continuations.get();
    if (queue != null) {
      queue.addLast(command);
      return;
    }
    queue = new ArrayDeque<>();
    continuations.set(queue);
    try {
      queue.addLast(command);
      Runnable next;
      while ((next = queue.pollFirst()) != null) next.run();
    } finally {
      queue.clear();
      continuations.remove();
    }
  }
}
