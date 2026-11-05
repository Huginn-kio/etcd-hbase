package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Internal request-account-aware reads for stores that retain data beyond the physical read. */
public interface BudgetedRecordStore {
  CompletableFuture<Optional<StoredRecord>> get(
      ByteKey key, long revisionUpperBound, ReadProjection projection, BudgetManager.Account account);

  ScanOperation startScan(ScanRequest request, BudgetManager.Account account);
}
