package io.etcdhbase.core.error;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CompletionFailuresTest {
  @Test
  void unwrapsOnlyCompletionWrappers() {
    var root = new IllegalStateException("failure");
    assertSame(root, CompletionFailures.unwrapCompletion(
        new CompletionException(new CompletionException(root))));
  }

  @Test
  void terminatesWhenCompletionWrapperCauseChainCycles() {
    var first = new CyclicCompletionException();
    var second = new CyclicCompletionException();
    first.next = second;
    second.next = first;

    assertSame(first, CompletionFailures.unwrapCompletion(first));
  }

  private static final class CyclicCompletionException extends CompletionException {
    private Throwable next;

    private CyclicCompletionException() {
      super("cycle", null);
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }
}
