package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Point-addressed result records stored in the system table under record type 11. */
public final class HBaseResultRepository implements ResultRepository {
  private final SystemRecordStore records;

  public HBaseResultRepository(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createPage(
      ResultPage page, RequestScope scope) {
    Objects.requireNonNull(page, "page");
    return records.createIfAbsent(SystemRecordType.RESULT, page.systemIdentity(),
        ByteKey.copyOf(ResultMessages.encode(page)), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
      ResultDirectory directory, RequestScope scope) {
    Objects.requireNonNull(directory, "directory");
    return records.createIfAbsent(SystemRecordType.RESULT, directory.systemIdentity(),
        ByteKey.copyOf(ResultMessages.encode(directory)), scope);
  }

  @Override
  public CompletableFuture<Optional<ResultPage>> readPage(
      ResultIdentity identity, int index, RequestScope scope) {
    ByteKey row = ResultPage.systemIdentity(identity, index);
    return records.read(SystemRecordType.RESULT, row, scope).thenApply(encoded -> encoded.map(bytes -> {
      ResultPage page = ResultMessages.decodePage(bytes.bytes());
      if (!page.identity().equals(identity) || page.index() != index || !page.systemIdentity().equals(row)) {
        throw new IllegalArgumentException("result page identity differs from its row or request");
      }
      return page;
    }));
  }

  @Override
  public CompletableFuture<Optional<ResultDirectory>> readDirectory(
      ResultIdentity identity, RequestScope scope) {
    ByteKey row = ResultDirectory.systemIdentity(identity);
    return records.read(SystemRecordType.RESULT, row, scope).thenApply(encoded -> encoded.map(bytes -> {
      ResultDirectory directory = ResultMessages.decodeDirectory(bytes.bytes());
      if (!directory.identity().equals(identity) || !directory.systemIdentity().equals(row)) {
        throw new IllegalArgumentException("result directory identity differs from its row or request");
      }
      return directory;
    }));
  }
}
