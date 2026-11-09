package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Endpoint-independent response nodes; order within a transaction is part of the result. */
public sealed interface ResultNode permits ResultNode.Put, ResultNode.Delete, ResultNode.Range, ResultNode.Txn {
  int MAX_OPERATIONS = 128;
  int MAX_RECORDS_PER_NODE = 65_536;

  int ordinal();
  Header header();

  /** Presence and revision are saved per response position, independent of serving identity. */
  record Header(boolean present, long revision) {
    public Header {
      if (revision < 0 || !present && revision != 0) throw new IllegalArgumentException("invalid result header");
    }
  }

  record Put(int ordinal, Header header, ByteKey key, Optional<StoredRecord> previous) implements ResultNode {
    public Put {
      requirePosition(ordinal, header);
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(previous, "previous");
      if (key.length() == 0) throw new IllegalArgumentException("result key is empty");
      previous.ifPresent(record -> {
        requireRecord(record, true);
        if (!record.key().equals(key)) throw new IllegalArgumentException("previous Put key differs");
        requireNotFuture(record, header);
      });
    }
  }

  record Delete(int ordinal, Header header, long deleted, List<StoredRecord> previous) implements ResultNode {
    public Delete {
      requirePosition(ordinal, header);
      previous = bounded(previous, MAX_RECORDS_PER_NODE);
      if (deleted < 0 || !previous.isEmpty() && previous.size() != deleted) {
        throw new IllegalArgumentException("invalid Delete result count");
      }
      previous.forEach(record -> {
        requireRecord(record, true);
        requireNotFuture(record, header);
      });
    }
  }

  record Range(int ordinal, Header header, List<StoredRecord> records, long count, boolean more)
      implements ResultNode {
    public Range {
      requirePosition(ordinal, header);
      records = bounded(records, MAX_RECORDS_PER_NODE);
      if (count < 0) throw new IllegalArgumentException("negative Range count");
      records.forEach(record -> {
        requireRecord(record, false);
        requireNotFuture(record, header);
      });
    }
  }

  record Txn(int ordinal, Header header, boolean succeeded, List<ResultNode> responses) implements ResultNode {
    public Txn {
      requirePosition(ordinal, header);
      responses = bounded(responses, MAX_OPERATIONS);
      for (int index = 0; index < responses.size(); index++) {
        if (responses.get(index).ordinal() != index) {
          throw new IllegalArgumentException("transaction responses are not in operation order");
        }
      }
    }
  }

  private static void requirePosition(int ordinal, Header header) {
    Objects.requireNonNull(header, "header");
    if (ordinal < 0 || ordinal >= MAX_OPERATIONS) throw new IllegalArgumentException("invalid result ordinal");
  }

  private static void requireRecord(StoredRecord record, boolean valueRequired) {
    Objects.requireNonNull(record, "record");
    if (record.key().length() == 0 || record.metadata().state() != StorageMetadata.State.LIVE
        || valueRequired && !record.hasValue()) {
      throw new IllegalArgumentException("result requires a live KV projection");
    }
  }

  private static void requireNotFuture(StoredRecord record, Header header) {
    if (header.present() && header.revision() > 0
        && record.metadata().modificationRevision() > header.revision()) {
      throw new IllegalArgumentException("result record is newer than its response header");
    }
  }

  private static <T> List<T> bounded(List<T> values, int limit) {
    Objects.requireNonNull(values, "values");
    if (values.size() > limit) throw new IllegalArgumentException("result list exceeds its bound");
    return List.copyOf(values);
  }
}
