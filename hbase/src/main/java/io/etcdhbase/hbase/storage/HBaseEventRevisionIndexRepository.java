package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.EventRevisionIndexPage;
import io.etcdhbase.core.commit.EventRevisionIndexRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.EventRevisionIndexMessages;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** HBase revision lookup records; the caller verifies decision and manifest provenance. */
public final class HBaseEventRevisionIndexRepository implements EventRevisionIndexRepository {
  private final SystemRecordStore records;

  public HBaseEventRevisionIndexRepository(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @Override
  public CompletableFuture<Optional<EventRevisionIndex>> read(long revision, RequestScope scope) {
    return records.read(SystemRecordType.EVENT_REVISION_INDEX, identity(revision), scope)
        .thenApply(value -> value.map(encoded -> {
          EventRevisionIndex index = EventRevisionIndexMessages.decode(encoded.bytes());
          if (index.decision().revision() != revision) {
            throw new IllegalStateException("event index row and decision revision differ");
          }
          return index;
        }));
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      EventRevisionIndex index, RequestScope scope) {
    Objects.requireNonNull(index, "index");
    return records.createIfAbsent(SystemRecordType.EVENT_REVISION_INDEX,
        identity(index.decision().revision()), ByteKey.copyOf(EventRevisionIndexMessages.encode(index)),
        scope);
  }

  @Override
  public CompletableFuture<EventRevisionIndexPage> firstAtOrAfter(
      int bucket, long revision, long maximumBytes, RequestScope scope) {
    if (revision <= 0 || maximumBytes <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event index seek"));
    }
    return records.scanEventRevisions(bucket, revision, Optional.empty(), 1, maximumBytes, scope)
        .thenApply(page -> {
          long actualBytes = page.entries().stream().mapToLong(entry -> entry.encodedBytes()).sum();
          if (page.entries().size() > 1 || page.encodedBytes() != actualBytes
              || page.encodedBytes() > maximumBytes || page.physicalRows() > 2
              || page.entries().isEmpty() && !page.exhausted()) {
            throw new IllegalStateException("invalid event index seek page");
          }
          var index = page.entries().stream().findFirst().map(entry -> {
            EventRevisionIndex decoded = EventRevisionIndexMessages.decode(entry.value().bytes());
            if (entry.identity().length() != Long.BYTES
                || ByteBuffer.wrap(entry.identity().bytes()).getLong() != decoded.decision().revision()
                || decoded.decision().revision() < revision) {
              throw new IllegalStateException("event index seek returned an inconsistent revision");
            }
            return decoded;
          });
          return new EventRevisionIndexPage(index, page.encodedBytes(),
              page.physicalRows(), page.scanRpcCalls());
        });
  }

  private static ByteKey identity(long revision) {
    if (revision <= 0) throw new IllegalArgumentException("revision must be positive");
    return ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(revision).array());
  }
}
