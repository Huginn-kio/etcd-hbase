package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.InvocationMessages;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact-byte CAS adapter for one invocationId-to-attempt row. */
public final class HBaseInvocationRepository implements InvocationRepository {
  private final SystemRecordStore records;

  public HBaseInvocationRepository(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  @Override
  public CompletableFuture<Optional<InvocationBinding>> read(
      InvocationId invocationId, RequestScope scope) {
    Objects.requireNonNull(invocationId, "invocationId");
    return records.read(SystemRecordType.INVOCATION,
        io.etcdhbase.core.bytes.ByteKey.copyOf(invocationId.bytes()), scope).thenApply(value ->
            value.map(encoded -> {
              InvocationBinding binding = InvocationMessages.decode(encoded.bytes());
              if (!binding.invocationId().equals(invocationId)) {
                throw new IllegalArgumentException("invocation row identity differs from its payload");
              }
              return binding;
            }));
  }

  @Override
  public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
      InvocationBinding binding, RequestScope scope) {
    Objects.requireNonNull(binding, "binding");
    return records.createIfAbsent(SystemRecordType.INVOCATION,
        io.etcdhbase.core.bytes.ByteKey.copyOf(binding.invocationId().bytes()),
        io.etcdhbase.core.bytes.ByteKey.copyOf(InvocationMessages.encode(binding)), scope);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(
      InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(replacement, "replacement");
    if (!expected.sameAttempt(replacement)
        || !expected.invocationId().equals(replacement.invocationId())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "invocation CAS cannot change its immutable identity"));
    }
    return records.compareAndSet(SystemRecordType.INVOCATION,
        io.etcdhbase.core.bytes.ByteKey.copyOf(expected.invocationId().bytes()),
        Optional.of(io.etcdhbase.core.bytes.ByteKey.copyOf(InvocationMessages.encode(expected))),
        io.etcdhbase.core.bytes.ByteKey.copyOf(InvocationMessages.encode(replacement)), scope);
  }
}
