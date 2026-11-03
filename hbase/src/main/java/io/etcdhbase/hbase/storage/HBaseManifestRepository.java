package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestDirectoryPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.EventDirectoryPage;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.ManifestMessages;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Immutable, txn-isolated candidate and Merkle manifest records in the system table. */
public final class HBaseManifestRepository implements ManifestRepository {
  private final SystemRecordStore records;

  public HBaseManifestRepository(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @Override
  public int candidateBucket(PreparedCandidate candidate) {
    Objects.requireNonNull(candidate, "candidate");
    return records.bucket(SystemRecordType.CANDIDATE, candidate.systemIdentity());
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
      PreparedCandidate candidate, RequestScope scope) {
    Objects.requireNonNull(candidate, "candidate");
    candidateBucket(candidate);
    return create(SystemRecordType.CANDIDATE, candidate.systemIdentity(),
        ManifestMessages.encode(candidate), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createPage(
      ManifestPage page, RequestScope scope) {
    Objects.requireNonNull(page, "page");
    return create(SystemRecordType.MANIFEST, page.systemIdentity(), ManifestMessages.encode(page), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
      ManifestDirectoryPage page, RequestScope scope) {
    Objects.requireNonNull(page, "page");
    return create(SystemRecordType.MANIFEST, page.systemIdentity(),
        ManifestMessages.encode(page), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
      PreparedEvent event, RequestScope scope) {
    Objects.requireNonNull(event, "event");
    return create(SystemRecordType.EVENT, event.systemIdentity(), ManifestMessages.encode(event), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
      EventPage page, RequestScope scope) {
    Objects.requireNonNull(page, "page");
    return create(SystemRecordType.MANIFEST, page.systemIdentity(), ManifestMessages.encode(page), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
      EventDirectoryPage page, RequestScope scope) {
    Objects.requireNonNull(page, "page");
    return create(SystemRecordType.MANIFEST, page.systemIdentity(), ManifestMessages.encode(page), scope);
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot root, RequestScope scope) {
    Objects.requireNonNull(root, "root");
    return create(SystemRecordType.MANIFEST, root.systemIdentity(), ManifestMessages.encode(root), scope);
  }

  @Override
  public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.MANIFEST, identity, ManifestMessages::decodeRoot, scope);
  }

  @Override
  public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.CANDIDATE, identity, ManifestMessages::decodeCandidate, scope);
  }

  @Override
  public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.MANIFEST, identity, ManifestMessages::decodePage, scope);
  }

  @Override
  public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
      ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.MANIFEST, identity, ManifestMessages::decodeDirectory, scope);
  }

  @Override
  public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.EVENT, identity, ManifestMessages::decodeEvent, scope);
  }

  @Override
  public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.MANIFEST, identity, ManifestMessages::decodeEventPage, scope);
  }

  @Override
  public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
      ByteKey identity, RequestScope scope) {
    return read(SystemRecordType.MANIFEST, identity, ManifestMessages::decodeEventDirectory, scope);
  }

  private <T> CompletableFuture<Optional<T>> read(SystemRecordType type, ByteKey identity,
      java.util.function.Function<byte[], T> decoder, RequestScope scope) {
    return records.read(type, identity, scope).thenApply(value -> value.map(encoded -> {
      T decoded = decoder.apply(encoded.bytes());
      if (decoded instanceof ManifestRoot root && !root.systemIdentity().equals(identity)
          || decoded instanceof PreparedCandidate candidate && !candidate.systemIdentity().equals(identity)
          || decoded instanceof ManifestPage page && !page.systemIdentity().equals(identity)
          || decoded instanceof ManifestDirectoryPage page && !page.systemIdentity().equals(identity)
          || decoded instanceof PreparedEvent event && !event.systemIdentity().equals(identity)
          || decoded instanceof EventPage page && !page.systemIdentity().equals(identity)
          || decoded instanceof EventDirectoryPage page && !page.systemIdentity().equals(identity)) {
        throw new IllegalArgumentException("manifest record row identity differs from its payload");
      }
      return decoded;
    }));
  }

  private CompletableFuture<SystemRecordStore.CreateResult> create(
      SystemRecordType type, ByteKey identity, byte[] encoded, RequestScope scope) {
    return records.createIfAbsent(type, identity, ByteKey.copyOf(encoded), scope);
  }
}
