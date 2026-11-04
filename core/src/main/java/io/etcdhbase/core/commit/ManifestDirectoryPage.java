package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Bounded Merkle directory page; bucket/key ranges must be disjoint and ordered. */
public final class ManifestDirectoryPage {
  public static final int MAX_CHILDREN = 32;
  public static final int MAX_PAGE_BYTES = 4 * 1024 * 1024;
  public static final int MAX_DEPTH = 8;
  private final TransactionId transactionId;
  private final int pageIndex;
  private final int level;
  private final List<ManifestChildReference> children;
  private final long itemCount;
  private final long valueBytes;
  private final int leafPageCount;
  private final ByteKey digest;

  private ManifestDirectoryPage(TransactionId transactionId, int pageIndex, int level,
      List<ManifestChildReference> children, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.children = List.copyOf(children);
    if (pageIndex < 0 || level < 1 || level > MAX_DEPTH || this.children.isEmpty()
        || this.children.size() > MAX_CHILDREN
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid manifest directory page");
    }
    long items = 0;
    long values = 0;
    int pages = 0;
    for (int index = 0; index < this.children.size(); index++) {
      ManifestChildReference child = this.children.get(index);
      if (child.level() != level - 1) {
        throw new IllegalArgumentException("manifest directory levels are not contiguous");
      }
      if (index > 0 && compareRanges(this.children.get(index - 1), child) >= 0) {
        throw new IllegalArgumentException("manifest directory key ranges overlap or are unordered");
      }
      items = Math.addExact(items, child.itemCount());
      values = Math.addExact(values, child.valueBytes());
      pages = Math.addExact(pages, child.leafPageCount());
    }
    this.pageIndex = pageIndex;
    this.level = level;
    this.itemCount = items;
    this.valueBytes = values;
    this.leafPageCount = pages;
    if (encodedLength(this.children) > MAX_PAGE_BYTES) {
      throw new IllegalArgumentException("manifest directory page exceeds its byte budget");
    }
    this.digest = calculateDigest(transactionId, pageIndex, level, this.children);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("manifest directory digest mismatch");
    }
  }

  public static ManifestDirectoryPage create(TransactionId transactionId, int pageIndex, int level,
      List<ManifestChildReference> children) {
    return new ManifestDirectoryPage(transactionId, pageIndex, level, children, null);
  }

  public static ManifestDirectoryPage restore(TransactionId transactionId, int pageIndex,
      int level, List<ManifestChildReference> children, ByteKey digest) {
    return new ManifestDirectoryPage(transactionId, pageIndex, level, children,
        Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public int pageIndex() { return pageIndex; }
  public int level() { return level; }
  public List<ManifestChildReference> children() { return children; }
  public long itemCount() { return itemCount; }
  public long valueBytes() { return valueBytes; }
  public int leafPageCount() { return leafPageCount; }
  public ByteKey digest() { return digest; }
  public int firstBucket() { return children.getFirst().firstBucket(); }
  public int lastBucket() { return children.getLast().lastBucket(); }
  public ByteKey firstKey() { return children.getFirst().firstKey(); }
  public ByteKey lastKey() { return children.getLast().lastKey(); }

  public ManifestChildReference reference() {
    return new ManifestChildReference(systemIdentity(), digest, firstBucket(), lastBucket(),
        firstKey(), lastKey(), itemCount,
        valueBytes, leafPageCount, level);
  }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.MANIFEST_DIRECTORY_PAGE)
        .putInt(pageIndex).array());
  }

  private static ByteKey calculateDigest(TransactionId transactionId, int pageIndex, int level,
      List<ManifestChildReference> children) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(2);
      output.write(transactionId.bytes());
      output.writeInt(pageIndex);
      output.writeInt(level);
      output.writeInt(children.size());
      for (ManifestChildReference child : children) {
        ManifestDigest.writeBytes(output, child.identity().bytes());
        ManifestDigest.writeBytes(output, child.digest().bytes());
        output.writeInt(child.firstBucket());
        output.writeInt(child.lastBucket());
        ManifestDigest.writeBytes(output, child.firstKey().bytes());
        ManifestDigest.writeBytes(output, child.lastKey().bytes());
        output.writeLong(child.itemCount());
        output.writeLong(child.valueBytes());
        output.writeInt(child.leafPageCount());
        output.writeInt(child.level());
      }
    });
  }

  private static int compareRanges(ManifestChildReference left, ManifestChildReference right) {
    if (left.lastBucket() != right.firstBucket()) {
      return Integer.compare(left.lastBucket(), right.firstBucket());
    }
    return left.lastKey().compareTo(right.firstKey());
  }

  private static long encodedLength(List<ManifestChildReference> children) {
    long total = 64;
    for (ManifestChildReference child : children) {
      total += 4L * Integer.BYTES + child.identity().length() + child.digest().length()
          + child.firstKey().length() + child.lastKey().length()
          + 2L * Integer.BYTES + 2L * Long.BYTES + 2L * Integer.BYTES;
      if (total > MAX_PAGE_BYTES) return MAX_PAGE_BYTES + 1L;
    }
    return total;
  }
}
