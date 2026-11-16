package io.etcdhbase.core.commit;

import java.util.Objects;
import java.util.Optional;

/** One bounded bucket seek; a returned index still needs an independent selection proof. */
public record EventRevisionIndexPage(Optional<EventRevisionIndex> index, long encodedBytes,
    long physicalRows, long scanRpcCalls) {
  public EventRevisionIndexPage {
    Objects.requireNonNull(index, "index");
    if (encodedBytes < 0 || physicalRows < (index.isPresent() ? 1 : 0) || scanRpcCalls < 1) {
      throw new IllegalArgumentException("invalid event-index seek page");
    }
  }
}
