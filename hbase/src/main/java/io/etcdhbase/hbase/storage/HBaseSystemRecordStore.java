package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/** Strong-read, exact-CAS adapter for immutable system records; borrowed connections stay shared. */
public final class HBaseSystemRecordStore implements SystemRecordStore {
  public static final int MAX_RECORD_BYTES = 9 * 1024 * 1024;
  private static final byte[] VALUE = {'r'};

  private final AsyncConnection connection;
  private final SystemTableBinding binding;
  private final boolean resultsSupported;

  private HBaseSystemRecordStore(AsyncConnection connection, SystemTableBinding binding,
      boolean resultsSupported) {
    this.connection = connection;
    this.binding = binding;
    this.resultsSupported = resultsSupported;
  }

  @Override
  public int bucket(SystemRecordType type, ByteKey identity) {
    requireTypeSupported(type);
    return binding.keyCodec().bucket(type, identity);
  }

  public static CompletableFuture<HBaseSystemRecordStore> open(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      SystemTableBinding binding,
      RequestScope scope) {
    var context = new HBaseRequestContext(scope);
    return context.borrowConnection(factory, identity).thenCompose(connection ->
        HBaseRequestCompletion.startRequest(scope,
            () -> connection.getAdmin().getDescriptor(binding.table()))
            .thenApply(descriptor -> {
              boolean supportsResults = binding.supportsResults(descriptor);
              return new HBaseSystemRecordStore(connection, binding, supportsResults);
            })).exceptionallyCompose(failure -> CompletableFuture.failedFuture(
                HBaseFailures.classify(failure)));
  }

  @Override
  public CompletableFuture<Optional<ByteKey>> read(
      SystemRecordType type, ByteKey identity, RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    return execute(scope, table -> table.get(
        new Get(row).setConsistency(Consistency.STRONG).addFamily(binding.family())))
        .thenApply(result -> decode(result));
  }

  @Override
  public CompletableFuture<CreateResult> createIfAbsent(
      SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    byte[] bytes = checkedValue(value);
    Put put = put(row, bytes);
    return execute(scope, table -> table.checkAndMutate(
        CheckAndMutate.newBuilder(row).ifNotExists(binding.family(), VALUE).build(put)))
        .thenCompose(result -> result.isSuccess()
            ? CompletableFuture.completedFuture(CreateResult.CREATED)
            : read(type, identity, scope).thenApply(existing -> existing
                .map(found -> Arrays.equals(found.bytes(), bytes)
                    ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT)
                .orElseThrow(() -> new CompletionException(
                    new BackendFailure(BackendFailure.Category.UNKNOWN)))));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(
      SystemRecordType type,
      ByteKey identity,
      Optional<ByteKey> expected,
      ByteKey replacement,
      RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    byte[] bytes = checkedValue(replacement);
    var builder = CheckAndMutate.newBuilder(row);
    if (expected.isPresent()) {
      builder.ifEquals(binding.family(), VALUE, checkedValue(expected.orElseThrow()));
    } else {
      builder.ifNotExists(binding.family(), VALUE);
    }
    return execute(scope, table -> table.checkAndMutate(builder.build(put(row, bytes))))
        .thenApply(result -> result.isSuccess());
  }

  private Optional<ByteKey> decode(Result result) {
    if (result.isEmpty()) return Optional.empty();
    var cells = result.rawCells();
    if (cells == null || cells.length != 1
        || !Arrays.equals(CellUtil.cloneQualifier(cells[0]), VALUE)) {
      throw new IllegalArgumentException("system record contains an unexpected cell layout");
    }
    byte[] bytes = result.getValue(binding.family(), VALUE);
    if (bytes == null) throw new IllegalArgumentException("system record is missing its value");
    validateValueLength(bytes.length);
    return Optional.of(ByteKey.copyOf(bytes));
  }

  private Put put(byte[] row, byte[] value) {
    return new Put(row).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), VALUE, value);
  }

  private <T> CompletableFuture<T> execute(
      RequestScope scope,
      java.util.function.Function<AsyncTable<?>, CompletableFuture<T>> operation) {
    return HBaseRequestCompletion.startRequest(scope, () -> {
      long timeout = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30), scope.remainingDeadlineNanos()));
      AsyncTable<?> table = connection.getTableBuilder(binding.table())
          .setOperationTimeout(timeout, TimeUnit.NANOSECONDS)
          .setRpcTimeout(timeout, TimeUnit.NANOSECONDS).build();
      return operation.apply(table);
    });
  }

  private static byte[] checkedValue(ByteKey value) {
    validateValueLength(value.length());
    return value.bytes();
  }

  private void requireTypeSupported(SystemRecordType type) {
    Objects.requireNonNull(type, "type");
    if ((type == SystemRecordType.RESULT || type == SystemRecordType.TABLE_GROUP_IDENTITY)
        && !resultsSupported) {
      throw new IllegalStateException("system table does not declare M3 record capability");
    }
  }

  private static void validateValueLength(int length) {
    if (length == 0 || length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("system record value exceeds its storage budget");
    }
  }
}
