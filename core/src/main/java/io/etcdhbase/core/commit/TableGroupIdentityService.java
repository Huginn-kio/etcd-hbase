package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** Initializes and reads one durable, table-group-scoped cluster ID. */
public final class TableGroupIdentityService {
  private static final int VERSION = 1;
  private static final ByteKey CLUSTER_ID_ROW = ByteKey.copyOf(new byte[] {'c'});
  private final SystemRecordStore records;
  private final LongSupplier identifiers;

  public TableGroupIdentityService(SystemRecordStore records) {
    this(records, new SecureRandom()::nextLong);
  }

  TableGroupIdentityService(SystemRecordStore records, LongSupplier identifiers) {
    this.records = Objects.requireNonNull(records, "records");
    this.identifiers = Objects.requireNonNull(identifiers, "identifiers");
  }

  /**
   * Initializes a verified new table group. Zero is reserved; all other long values retain their
   * raw uint64 bits. Runtime startup must use {@link #readExisting(RequestScope)} instead.
   */
  public CompletableFuture<Long> initialize(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    long proposed = 0;
    for (int attempts = 0; attempts < 16 && proposed == 0; attempts++) proposed = identifiers.getAsLong();
    if (proposed == 0) return CompletableFuture.failedFuture(new IllegalStateException("unable to generate cluster ID"));
    long expected = proposed;
    ByteKey value = ByteKey.copyOf(ByteBuffer.allocate(1 + Long.BYTES).put((byte) VERSION).putLong(proposed).array());
    CompletableFuture<SystemRecordStore.CreateResult> created;
    try {
      created = Objects.requireNonNull(records.createIfAbsent(SystemRecordType.TABLE_GROUP_IDENTITY,
          CLUSTER_ID_ROW, value, scope), "identity create future");
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return created.handle(Attempt::new).thenCompose(attempt -> {
      if (attempt.failure() != null && !isUnknown(attempt.failure())) {
        return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(attempt.failure()));
      }
      if (attempt.failure() == null && attempt.result() == null) {
        return CompletableFuture.failedFuture(new IllegalStateException("identity store returned no create result"));
      }
      return records.read(SystemRecordType.TABLE_GROUP_IDENTITY, CLUSTER_ID_ROW, scope).handle((found, failure) -> {
        if (failure != null) {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          if (attempt.failure() != null) {
            Throwable prior = CompletionFailures.unwrapCompletion(attempt.failure());
            if (prior != cause) cause.addSuppressed(prior);
          }
          throw new java.util.concurrent.CompletionException(cause);
        }
        if (found.isEmpty()) {
          throw new java.util.concurrent.CompletionException(new BackendFailure(
              BackendFailure.Category.UNKNOWN, attempt.failure()));
        }
        long actual = decode(found.orElseThrow());
        if (attempt.result() == SystemRecordStore.CreateResult.CREATED && actual != expected) {
          throw new IllegalStateException("created cluster ID differs from its durable record");
        }
        return actual;
      });
    });
  }

  /** Refuses to invent or replace the identity of an established group. */
  public CompletableFuture<Long> readExisting(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    return records.read(SystemRecordType.TABLE_GROUP_IDENTITY, CLUSTER_ID_ROW, scope)
        .thenApply(found -> decode(found.orElseThrow(() ->
            new IllegalStateException("table-group cluster ID is missing"))));
  }

  private static long decode(ByteKey encoded) {
    byte[] bytes = encoded.bytes();
    if (bytes.length != 1 + Long.BYTES || Byte.toUnsignedInt(bytes[0]) != VERSION) {
      throw new IllegalArgumentException("invalid table-group identity record");
    }
    long value = ByteBuffer.wrap(bytes, 1, Long.BYTES).getLong();
    if (value == 0) throw new IllegalArgumentException("table-group cluster ID is zero");
    return value;
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    return cause instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record Attempt(SystemRecordStore.CreateResult result, Throwable failure) {}
}
