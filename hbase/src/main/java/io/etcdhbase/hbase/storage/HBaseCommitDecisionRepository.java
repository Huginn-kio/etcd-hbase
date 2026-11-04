package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitDecisionRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Sequence-indexed decision archive backed by the HBase system record table. */
public final class HBaseCommitDecisionRepository implements CommitDecisionRepository {
  private final SystemRecordStore records;

  public HBaseCommitDecisionRepository(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @Override
  public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
    ByteKey identity = identity(sequence);
    return records.read(SystemRecordType.DECISION, identity, scope).thenApply(value ->
        value.map(encoded -> {
          CommitDecision decision = CommitDecisionMessages.decode(encoded.bytes());
          if (decision.sequence() != sequence) {
            throw new IllegalArgumentException("decision row key and sequence do not match");
          }
          return decision;
        }));
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      CommitDecision decision, RequestScope scope) {
    byte[] encoded = CommitDecisionMessages.encode(decision);
    return records.createIfAbsent(SystemRecordType.DECISION, identity(decision.sequence()),
        ByteKey.copyOf(encoded), scope).thenApply(result -> {
          if (result == SystemRecordStore.CreateResult.CONFLICT) {
            throw new IllegalStateException("commit sequence already has a different decision");
          }
          return result;
        });
  }

  private static ByteKey identity(long sequence) {
    if (sequence <= 0) throw new IllegalArgumentException("sequence must be positive");
    return ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(sequence).array());
  }
}
