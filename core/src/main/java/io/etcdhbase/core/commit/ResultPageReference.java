package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Authenticated page metadata included in a result directory. */
public record ResultPageReference(ResultIdentity identity, int index, int payloadBytes,
    int itemCount, ByteKey digest) {
  public ResultPageReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (index < 0 || index >= ResultDirectory.MAX_PAGES || payloadBytes <= 0
        || payloadBytes > ResultPage.MAX_PAYLOAD_BYTES || itemCount != (index == 0 ? 1 : 0)
        || digest.length() != 32) {
      throw new IllegalArgumentException("invalid result page reference");
    }
  }

  public static ResultPageReference from(ResultPage page) {
    Objects.requireNonNull(page, "page");
    return new ResultPageReference(page.identity(), page.index(), page.payloadBytes(), page.itemCount(), page.digest());
  }

  public boolean matches(ResultPage page) {
    return equals(from(page));
  }
}
