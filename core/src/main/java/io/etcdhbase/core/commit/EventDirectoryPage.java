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

/** Bounded ordered event directory; child ranges are contiguous and cannot overlap or omit ordinals. */
public final class EventDirectoryPage {
  public static final int MAX_CHILDREN = 128;
  public static final int MAX_DEPTH = 8;
  private final TransactionId transactionId;
  private final int pageIndex;
  private final int level;
  private final List<EventReference> children;
  private final long itemCount;
  private final long totalBytes;
  private final int leafPageCount;
  private final ByteKey digest;

  private EventDirectoryPage(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children, ByteKey digest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.children = List.copyOf(children);
    this.digest = Objects.requireNonNull(digest, "digest");
    if (pageIndex < 0 || level < 1 || level > MAX_DEPTH || this.children.isEmpty()
        || this.children.size() > MAX_CHILDREN || digest.length() != 32) {
      throw new IllegalArgumentException("invalid event directory page");
    }
    long items = 0;
    long bytes = 0;
    int pages = 0;
    for (int index = 0; index < this.children.size(); index++) {
      EventReference child = this.children.get(index);
      if (child.level() != level - 1 || index > 0
          && this.children.get(index - 1).lastOrdinal() + 1 != child.firstOrdinal()) {
        throw new IllegalArgumentException("event directory levels or ordinal ranges are not contiguous");
      }
      items = Math.addExact(items, child.itemCount());
      bytes = Math.addExact(bytes, child.totalBytes());
      pages = Math.addExact(pages, child.leafPageCount());
    }
    this.pageIndex = pageIndex;
    this.level = level;
    this.itemCount = items;
    this.totalBytes = bytes;
    this.leafPageCount = pages;
    if (!ByteKey.copyOf(calculateDigest(transactionId, pageIndex, level, this.children)).equals(digest)) {
      throw new IllegalArgumentException("event directory digest mismatch");
    }
  }

  public static EventDirectoryPage create(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children) {
    List<EventReference> copy = List.copyOf(children);
    return new EventDirectoryPage(transactionId, pageIndex, level, copy,
        ByteKey.copyOf(calculateDigest(transactionId, pageIndex, level, copy)));
  }

  public static EventDirectoryPage restore(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children, ByteKey digest) {
    return new EventDirectoryPage(transactionId, pageIndex, level, children, digest);
  }

  public TransactionId transactionId() { return transactionId; }
  public int pageIndex() { return pageIndex; }
  public int level() { return level; }
  public List<EventReference> children() { return children; }
  public long itemCount() { return itemCount; }
  public long totalBytes() { return totalBytes; }
  public int leafPageCount() { return leafPageCount; }
  public ByteKey digest() { return digest; }
  public long firstOrdinal() { return children.getFirst().firstOrdinal(); }
  public long lastOrdinal() { return children.getLast().lastOrdinal(); }

  public EventReference reference() {
    return new EventReference(systemIdentity(), digest, firstOrdinal(), lastOrdinal(), itemCount,
        totalBytes, leafPageCount, level);
  }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(transactionId.bytes()).put((byte) 6).putInt(pageIndex).array());
  }

  private static byte[] calculateDigest(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(1);
        output.write(transactionId.bytes());
        output.writeInt(pageIndex);
        output.writeInt(level);
        output.writeInt(children.size());
        for (EventReference child : children) {
          byte[] identity = child.identity().bytes();
          output.writeInt(identity.length);
          output.write(identity);
          output.write(child.digest().bytes());
          output.writeLong(child.firstOrdinal());
          output.writeLong(child.lastOrdinal());
          output.writeLong(child.itemCount());
          output.writeLong(child.totalBytes());
          output.writeInt(child.leafPageCount());
          output.writeInt(child.level());
        }
      }
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory event directory digest failed", impossible);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }
}
