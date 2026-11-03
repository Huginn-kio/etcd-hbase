package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Bounded pointer to one durable ordered event record. */
public record PreparedEventReference(long ordinal, ByteKey identity, ByteKey digest, int encodedBytes) {
  public PreparedEventReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (ordinal < 0 || identity.length() == 0 || digest.length() != 32
        || encodedBytes <= 0 || encodedBytes > PreparedEvent.MAX_EVENT_BYTES) {
      throw new IllegalArgumentException("invalid event reference");
    }
  }

  public static PreparedEventReference from(PreparedEvent event) {
    return new PreparedEventReference(event.ordinal(), event.systemIdentity(), event.digest(),
        event.retainedBytes());
  }
}
