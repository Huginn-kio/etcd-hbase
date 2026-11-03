package io.etcdhbase.core.error;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;

/** JDK-only helpers for inspecting failures surfaced through completion stages. */
public final class CompletionFailures {
  private CompletionFailures() {}

  /** Unwraps completion wrappers without looping if a custom throwable exposes a cycle. */
  public static Throwable unwrapCompletion(Throwable failure) {
    Throwable current = Objects.requireNonNull(failure, "failure");
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (current instanceof CompletionException && current.getCause() != null
        && seen.add(current)) {
      current = current.getCause();
    }
    return current;
  }
}
