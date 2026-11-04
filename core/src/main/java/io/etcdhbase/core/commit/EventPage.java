package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Bounded leaf page in transaction order; each entry points to a durable event record. */
public final class EventPage {
  public static final int MAX_EVENTS = 64;
  public static final int MAX_PAGE_BYTES = 1024 * 1024;
  private final TransactionId transactionId;
  private final int pageIndex;
  private final List<PreparedEventReference> events;
  private final long eventBytes;
  private final long encodedBytes;
  private final ByteKey digest;

  private EventPage(TransactionId transactionId, int pageIndex, List<PreparedEventReference> events,
      ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.events = List.copyOf(events);
    if (pageIndex < 0 || this.events.isEmpty() || this.events.size() > MAX_EVENTS
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid event page");
    }
    int bytes = 64;
    long payloadBytes = 0;
    for (int index = 0; index < this.events.size(); index++) {
      PreparedEventReference event = this.events.get(index);
      if (index > 0 && this.events.get(index - 1).ordinal() + 1 != event.ordinal()) {
        throw new IllegalArgumentException("event page identities or ordinals are not contiguous");
      }
      bytes = Math.addExact(bytes, 64 + event.identity().length() + event.digest().length());
      payloadBytes = Math.addExact(payloadBytes, event.encodedBytes());
    }
    if (bytes > MAX_PAGE_BYTES) throw new IllegalArgumentException("event page exceeds its byte budget");
    encodedBytes = bytes;
    eventBytes = payloadBytes;
    this.pageIndex = pageIndex;
    this.digest = calculateDigest(transactionId, pageIndex, this.events);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("event page digest mismatch");
    }
  }

  public static EventPage create(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events) {
    return new EventPage(transactionId, pageIndex, events, null);
  }

  public static EventPage restore(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events, ByteKey digest) {
    return new EventPage(transactionId, pageIndex, events, Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public int pageIndex() { return pageIndex; }
  public List<PreparedEventReference> events() { return events; }
  public long encodedBytes() { return encodedBytes; }
  public long eventBytes() { return eventBytes; }
  public ByteKey digest() { return digest; }
  public long firstOrdinal() { return events.getFirst().ordinal(); }
  public long lastOrdinal() { return events.getLast().ordinal(); }

  public EventReference reference() {
    return new EventReference(systemIdentity(), digest, firstOrdinal(), lastOrdinal(), events.size(),
        eventBytes, 1, 0);
  }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.EVENT_PAGE).putInt(pageIndex).array());
  }

  private static ByteKey calculateDigest(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeInt(pageIndex);
      output.writeInt(events.size());
      for (PreparedEventReference event : events) {
        output.writeLong(event.ordinal());
        ManifestDigest.writeBytes(output, event.identity().bytes());
        output.write(event.digest().bytes());
        output.writeInt(event.encodedBytes());
      }
    });
  }
}
