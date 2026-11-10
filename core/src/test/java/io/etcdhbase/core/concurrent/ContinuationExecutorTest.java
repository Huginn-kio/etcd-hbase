package io.etcdhbase.core.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ContinuationExecutorTest {
  @Test
  void queuesReentrantTasksButKeepsInstancesIndependent() {
    var first = new ContinuationExecutor();
    var second = new ContinuationExecutor();
    var observed = new ArrayList<String>();
    first.execute(() -> {
      observed.add("start");
      first.execute(() -> observed.add("queued"));
      second.execute(() -> observed.add("other"));
      observed.add("end");
    });
    assertEquals(List.of("start", "other", "end", "queued"), observed);
  }

  @Test
  void drainsLongSynchronousContinuationChainsWithoutGrowingTheStack() {
    var executor = new ContinuationExecutor();
    class Repeated implements Runnable {
      int count;
      @Override public void run() {
        if (++count < 10_000) executor.execute(this);
      }
    }
    var repeated = new Repeated();
    executor.execute(repeated);
    assertEquals(10_000, repeated.count);
  }

  @Test
  void failureClearsQueuedTasksAndAllowsNextDrain() {
    var executor = new ContinuationExecutor();
    var observed = new ArrayList<String>();
    assertThrows(IllegalStateException.class, () -> executor.execute(() -> {
      executor.execute(() -> observed.add("abandoned"));
      throw new IllegalStateException("failed");
    }));
    executor.execute(() -> observed.add("next"));
    assertEquals(List.of("next"), observed);
  }
}
