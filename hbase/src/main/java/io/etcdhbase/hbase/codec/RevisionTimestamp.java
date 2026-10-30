package io.etcdhbase.hbase.codec;

/** Prototype physical ordering only; a timestamp is never evidence of a committed transaction. */
public final class RevisionTimestamp {
  private RevisionTimestamp() {}

  public static long timestamp(long revision) {
    requireRevision(revision);
    return revision - 1;
  }

  public static long revision(long timestamp) {
    if (timestamp < 0 || timestamp == Long.MAX_VALUE) {
      throw new IllegalArgumentException("invalid explicit storage timestamp");
    }
    return timestamp + 1;
  }

  public static long exclusiveUpper(long revision) {
    requireRevision(revision);
    return revision;
  }

  static void requireRevision(long revision) {
    if (revision <= 0) throw new IllegalArgumentException("revision must be positive");
  }
}
