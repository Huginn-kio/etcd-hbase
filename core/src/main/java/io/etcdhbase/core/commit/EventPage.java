package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
      ByteKey digest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.events = List.copyOf(events);
    this.digest = Objects.requireNonNull(digest, "digest");
    if (pageIndex < 0 || this.events.isEmpty() || this.events.size() > MAX_EVENTS
        || digest.length() != 32) throw new IllegalArgumentException("invalid event page");
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
    if (!ByteKey.copyOf(calculateDigest(transactionId, pageIndex, this.events)).equals(digest)) {
      throw new IllegalArgumentException("event page digest mismatch");
    }
  }

  public static EventPage create(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events) {
    List<PreparedEventReference> copy = List.copyOf(events);
    return new EventPage(transactionId, pageIndex, copy,
        ByteKey.copyOf(calculateDigest(transactionId, pageIndex, copy)));
  }

  public static EventPage restore(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events, ByteKey digest) {
    return new EventPage(transactionId, pageIndex, events, digest);
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
        .put(transactionId.bytes()).put((byte) 5).putInt(pageIndex).array());
  }

  private static byte[] calculateDigest(TransactionId transactionId, int pageIndex,
      List<PreparedEventReference> events) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(1);
        output.write(transactionId.bytes());
        output.writeInt(pageIndex);
        output.writeInt(events.size());
        for (PreparedEventReference event : events) {
          output.writeLong(event.ordinal());
          output.writeInt(event.identity().length());
          output.write(event.identity().bytes());
          output.write(event.digest().bytes());
          output.writeInt(event.encodedBytes());
        }
      }
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory event page digest failed", impossible);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }
}
