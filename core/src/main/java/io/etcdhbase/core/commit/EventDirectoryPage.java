package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
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
      List<EventReference> children, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.children = List.copyOf(children);
    if (pageIndex < 0 || level < 1 || level > MAX_DEPTH || this.children.isEmpty()
        || this.children.size() > MAX_CHILDREN
        || expectedDigest != null && expectedDigest.length() != 32) {
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
    this.digest = calculateDigest(transactionId, pageIndex, level, this.children);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("event directory digest mismatch");
    }
  }

  public static EventDirectoryPage create(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children) {
    return new EventDirectoryPage(transactionId, pageIndex, level, children, null);
  }

  public static EventDirectoryPage restore(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children, ByteKey digest) {
    return new EventDirectoryPage(transactionId, pageIndex, level, children,
        Objects.requireNonNull(digest, "digest"));
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
        .put(transactionId.bytes()).put(ManifestRecordFormat.EVENT_DIRECTORY_PAGE)
        .putInt(pageIndex).array());
  }

  private static ByteKey calculateDigest(TransactionId transactionId, int pageIndex, int level,
      List<EventReference> children) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeInt(pageIndex);
      output.writeInt(level);
      output.writeInt(children.size());
      for (EventReference child : children) {
        ManifestDigest.writeBytes(output, child.identity().bytes());
        output.write(child.digest().bytes());
        output.writeLong(child.firstOrdinal());
        output.writeLong(child.lastOrdinal());
        output.writeLong(child.itemCount());
        output.writeLong(child.totalBytes());
        output.writeInt(child.leafPageCount());
        output.writeInt(child.level());
      }
    });
  }
}
