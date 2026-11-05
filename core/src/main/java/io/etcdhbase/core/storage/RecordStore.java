package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Request-bound physical storage port. No transaction commit or concurrent snapshot guarantee. */
public interface RecordStore extends ScanSource {
  CompletableFuture<Optional<StoredRecord>> get(ByteKey key, long revisionUpperBound, ReadProjection projection);
  CompletableFuture<Void> put(StoredRecord record);
  CompletableFuture<Boolean> compareAndSet(ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement);
  CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records);
  ScanOperation startScan(ScanRequest request);

  /** Fences a pinned unary result after all data bytes have been materialized; ordinary stores need no action. */
  default CompletableFuture<Void> finishRead() {
    return CompletableFuture.completedFuture(null);
  }

  default CompletableFuture<StoragePage> scan(ScanRequest request) {
    return startScan(request).completion().toCompletableFuture();
  }
}
