package io.etcdhbase.core.kv;

import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/** Bounded logical response tree, validated before it is serialized into result pages. */
public final class ResultTree {
  public static final int MAX_DEPTH = 16;
  public static final int MAX_NODES = 4096;
  public static final int MAX_RECORDS = 65_536;
  public static final long MAX_RETAINED_BYTES = 64L * 1024 * 1024;

  private final ResultNode root;
  private final int nodeCount;
  private final int recordCount;
  private final long retainedBytes;

  public ResultTree(ResultNode root) {
    this.root = Objects.requireNonNull(root, "root");
    if (root.ordinal() != 0) throw new IllegalArgumentException("root result ordinal must be zero");
    Deque<Entry> pending = new ArrayDeque<>();
    pending.push(new Entry(root, 1));
    int nodes = 0;
    int records = 0;
    long bytes = 0;
    while (!pending.isEmpty()) {
      Entry entry = pending.pop();
      if (entry.depth() > MAX_DEPTH || ++nodes > MAX_NODES) {
        throw new IllegalArgumentException("result tree exceeds its depth or node bound");
      }
      bytes = Math.addExact(bytes, 128);
      switch (entry.node()) {
        case ResultNode.Put put -> {
          bytes = Math.addExact(bytes, put.key().length());
          if (put.previous().isPresent()) {
            records++;
            bytes = Math.addExact(bytes, size(put.previous().orElseThrow()));
          }
        }
        case ResultNode.Delete delete -> {
          records = Math.addExact(records, delete.previous().size());
          for (StoredRecord record : delete.previous()) bytes = Math.addExact(bytes, size(record));
        }
        case ResultNode.Range range -> {
          records = Math.addExact(records, range.records().size());
          for (StoredRecord record : range.records()) bytes = Math.addExact(bytes, size(record));
        }
        case ResultNode.Txn txn -> {
          for (int index = txn.responses().size() - 1; index >= 0; index--) {
            pending.push(new Entry(txn.responses().get(index), entry.depth() + 1));
          }
        }
      }
      if (records > MAX_RECORDS || bytes > MAX_RETAINED_BYTES) {
        throw new IllegalArgumentException("result tree exceeds its record or byte bound");
      }
    }
    this.nodeCount = nodes;
    this.recordCount = records;
    this.retainedBytes = bytes;
  }

  public ResultNode root() { return root; }
  public int nodeCount() { return nodeCount; }
  public int recordCount() { return recordCount; }
  public long retainedBytes() { return retainedBytes; }

  private static long size(StoredRecord record) {
    return Math.addExact(128L + record.key().length(), record.hasValue() ? record.metadata().valueLength() : 0);
  }

  private record Entry(ResultNode node, int depth) {}
}
