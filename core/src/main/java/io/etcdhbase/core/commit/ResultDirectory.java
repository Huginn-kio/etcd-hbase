package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Flat, bounded and ordered directory of all immutable pages of one result. */
public final class ResultDirectory {
  public static final int MAX_PAGES = 1024;

  private final ResultIdentity identity;
  private final List<ResultPageReference> pages;
  private final long totalBytes;
  private final ByteKey digest;

  private ResultDirectory(ResultIdentity identity, List<ResultPageReference> pages,
      ByteKey expectedDigest) {
    this.identity = Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(pages, "pages");
    if (pages.isEmpty() || pages.size() > MAX_PAGES
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid result directory bounds");
    }
    this.pages = List.copyOf(pages);
    long bytes = 0;
    for (int index = 0; index < this.pages.size(); index++) {
      ResultPageReference page = this.pages.get(index);
      if (page.index() != index || !identity.equals(page.identity())) {
        throw new IllegalArgumentException("result pages do not belong to an ordered directory");
      }
      bytes += page.payloadBytes();
    }
    this.totalBytes = bytes;
    this.digest = ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.writeByte(ManifestRecordFormat.RESULT_DIRECTORY);
      identity.writeTo(output);
      output.writeInt(this.pages.size());
      output.writeLong(totalBytes);
      output.writeInt(1);
      for (ResultPageReference page : this.pages) {
        output.writeInt(page.index());
        output.writeInt(page.payloadBytes());
        output.writeInt(page.itemCount());
        output.write(page.digest().bytes());
      }
    });
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("result directory digest mismatch");
    }
  }

  public static ResultDirectory create(ResultIdentity identity, List<ResultPageReference> pages) {
    return new ResultDirectory(identity, pages, null);
  }

  public static ResultDirectory restore(ResultIdentity identity, List<ResultPageReference> pages,
      ByteKey digest) {
    return new ResultDirectory(identity, pages, Objects.requireNonNull(digest, "digest"));
  }

  public ResultIdentity identity() { return identity; }
  public List<ResultPageReference> pages() { return pages; }
  public long totalBytes() { return totalBytes; }
  public int itemCount() { return 1; }
  public ByteKey digest() { return digest; }
  public ResultReference reference() {
    return new ResultReference(identity, digest, pages.size(), itemCount(), totalBytes);
  }

  public ByteKey systemIdentity() {
    return systemIdentity(identity);
  }

  public static ByteKey systemIdentity(ResultIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1).put(identity.transactionId().bytes())
        .put(ManifestRecordFormat.RESULT_DIRECTORY).array());
  }
}
