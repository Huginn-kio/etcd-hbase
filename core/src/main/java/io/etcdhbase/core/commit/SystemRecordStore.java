package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Point-addressed durable M2 record port; persisted values are immutable byte strings. */
public interface SystemRecordStore {
  enum CreateResult { CREATED, ALREADY_PRESENT, CONFLICT }

  CompletableFuture<Optional<ByteKey>> read(
      SystemRecordType type, ByteKey identity, RequestScope scope);

  /** Validates the physical row identity and returns its configured salt bucket without I/O. */
  int bucket(SystemRecordType type, ByteKey identity);

  /** Returns CONFLICT for a durable value under the same identity with different bytes. */
  CompletableFuture<CreateResult> createIfAbsent(
      SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope);

  /** Exact-value CAS; empty expected means the row must not exist yet. */
  CompletableFuture<Boolean> compareAndSet(
      SystemRecordType type,
      ByteKey identity,
      Optional<ByteKey> expected,
      ByteKey replacement,
      RequestScope scope);
}
