package io.etcdhbase.core.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
  void unwrapsCompletionWrappersAndPreservesSuppressedFailures() {
    var root = new IllegalStateException("read failed");
    var cleanup = new IllegalStateException("cleanup failed");
    var wrapped = new CompletionException(new CompletionException(root));
    wrapped.addSuppressed(cleanup);

    assertSame(root, CompletionFailures.unwrapCompletionPreservingSuppressed(wrapped));
    assertEquals(1, root.getSuppressed().length);
    assertSame(cleanup, root.getSuppressed()[0]);

    assertSame(root, CompletionFailures.unwrapCompletionPreservingSuppressed(wrapped));
    assertEquals(1, root.getSuppressed().length, "repeated unwrapping must not duplicate failures");
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
