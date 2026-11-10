package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.PointResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/** Reads one ordered range within an already pinned transaction view. */
final class RangeReadExecutor {
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();

  record Selection(long included, long observedLive, boolean more) {}

  private final TxnReadSession session;
  private final CompareReadExecutor comparisons;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final ToIntFunction<ByteKey> bucket;
  private final long pointBytes;

  RangeReadExecutor(TxnReadSession session, CompareReadExecutor comparisons,
      BudgetManager.Account account, RequestScope scope, ToIntFunction<ByteKey> bucket, long pointBytes) {
    this.session = Objects.requireNonNull(session, "session");
    this.comparisons = Objects.requireNonNull(comparisons, "comparisons");
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    this.pointBytes = pointBytes;
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || !comparisons.belongsTo(session, account, scope) || pointBytes < 1
        || pointBytes > account.reservation().output()) {
      throw new IllegalArgumentException("range inputs belong to different requests or exceed the reservation");
    }
  }

  CompletableFuture<Selection> read(RangeCommand command, long targetRevision,
      Optional<TxnWorkingSet> working, Consumer<StoredRecord> output) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(working, "working");
    Objects.requireNonNull(output, "output");
    if (targetRevision <= 0 || working.isPresent()
        && (command.context() != RangeCommand.Context.TRANSACTION || command.revision() > 0
            || targetRevision != session.currentRevision())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid range view target"));
    }
    if (command.sort().target() != 0 || command.sort().order() < 0 || command.sort().order() > 2) {
      return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION));
    }
    LogicalViewCursor cursor = null;
    try {
      requireActive();
      var selector = new RangeSelector(command);
      var reader = new DirectRecordReader(session.reader(targetRevision), account, pointBytes);
      if (command.range().kind() == KeyRange.Kind.POINT && working.isEmpty()) {
        return readPoint(command, targetRevision, selector, reader, output);
      }
      var limits = comparisons.limits();
      var direction = command.sort().order() == 2 ? ScanDirection.DESCENDING : ScanDirection.ASCENDING;
      var request = new ScanRequest(0, command.range(), Optional.empty(), targetRevision,
          ReadProjection.METADATA, 1, limits.pageBytes(), limits.physicalRowsPerPage(), direction);
      cursor = new LogicalViewCursor(comparisons.scanSource(targetRevision), working,
          request, limits.buckets(), bucket, account, scope, command.countOnly()
              ? LogicalViewCursor.Traversal.ANY : direction == ScanDirection.DESCENDING
                  ? LogicalViewCursor.Traversal.KEY_DESCENDING : LogicalViewCursor.Traversal.KEY_ASCENDING);
      var scan = new Scan(command, targetRevision, selector, reader, cursor, output);
      scan.advance();
      return scan.result.copy();
    } catch (BackendFailure | RuntimeException failure) {
      closeFailedCursor(cursor, failure);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      closeFailedCursor(cursor, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<Selection> readPoint(RangeCommand command, long revision,
      RangeSelector selector, DirectRecordReader reader, Consumer<StoredRecord> output) {
    ReadProjection projection = command.keysOnly() || command.countOnly()
        ? ReadProjection.METADATA : ReadProjection.FULL;
    ByteKey key = command.range().lowerInclusive().orElseThrow();
    return reader.readForPreparation(key, revision, projection).thenApply(point -> {
      try (point) {
        requireActive();
        point.record().ifPresent(record -> {
          if (selector.observe(record) == RangeSelector.Action.INCLUDE) output.accept(record);
        });
        return new Selection(selector.included(), selector.observedLive(), false);
      } catch (BackendFailure failure) {
        throw new java.util.concurrent.CompletionException(failure);
      }
    });
  }

  private static void closeFailedCursor(LogicalViewCursor cursor, Throwable failure) {
    if (cursor == null) return;
    try { cursor.close(); }
    catch (RuntimeException | Error cleanup) {
      if (cleanup != failure) failure.addSuppressed(cleanup);
      if (cleanup instanceof Error fatal && !(failure instanceof Error)) throw fatal;
    }
  }

  private final class Scan {
    private final RangeCommand command;
    private final long revision;
    private final RangeSelector selector;
    private final DirectRecordReader values;
    private final LogicalViewCursor cursor;
    private final Consumer<StoredRecord> output;
    private final CompletableFuture<Selection> result = new CompletableFuture<>();
    private final AtomicBoolean finished = new AtomicBoolean();

    private Scan(RangeCommand command, long revision, RangeSelector selector,
        DirectRecordReader values, LogicalViewCursor cursor, Consumer<StoredRecord> output) {
      this.command = command;
      this.revision = revision;
      this.selector = selector;
      this.values = values;
      this.cursor = cursor;
      this.output = output;
    }

    private void advance() {
      try {
        requireActive();
        cursor.next().whenCompleteAsync(this::accept, CONTINUE);
      } catch (BackendFailure | RuntimeException failure) {
        finish(failure);
      } catch (Error fatal) {
        finish(fatal);
        throw fatal;
      }
    }

    private void accept(Optional<StoredRecord> found, Throwable failure) {
      if (failure != null) { finish(failure); return; }
      try {
        requireActive();
        if (Objects.requireNonNull(found, "range view result").isEmpty()) {
          finish(null);
          return;
        }
        StoredRecord selected = found.orElseThrow();
        switch (selector.observe(selected)) {
          case SKIP -> advance();
          case STOP -> finish(null);
          case INCLUDE -> emit(selected);
        }
      } catch (BackendFailure | RuntimeException problem) {
        finish(problem);
      } catch (Error fatal) {
        finish(fatal);
        throw fatal;
      }
    }

    private void emit(StoredRecord selected) {
      if (command.keysOnly()) {
        output.accept(StoredRecord.metadataOnly(selected.key(), selected.metadata()));
        advance();
      } else if (selected.hasValue()) {
        output.accept(selected);
        advance();
      } else {
        values.readForPreparation(selected.key(), revision, ReadProjection.FULL)
            .whenCompleteAsync((point, failure) -> acceptValue(selected, point, failure), CONTINUE);
      }
    }

    private void acceptValue(StoredRecord selected, PointResult point, Throwable failure) {
      if (failure != null) { finish(failure); return; }
      try (var value = Objects.requireNonNull(point, "range value result")) {
        requireActive();
        StoredRecord actual = value.record().orElseThrow(() -> new IllegalStateException("range value is missing"));
        if (!actual.hasValue() || !actual.key().equals(selected.key())
            || !actual.metadata().equals(selected.metadata())) {
          throw new IllegalStateException("range value differs from its selected metadata");
        }
        output.accept(actual);
      } catch (BackendFailure | RuntimeException problem) {
        finish(problem);
        return;
      } catch (Error fatal) {
        finish(fatal);
        throw fatal;
      }
      advance();
    }

    private void finish(Throwable failure) {
      if (!finished.compareAndSet(false, true)) return;
      if (failure != null) failure = CompletionFailures.unwrapCompletion(failure);
      Error fatal = failure instanceof Error error ? error : null;
      try { cursor.close(); }
      catch (RuntimeException | Error cleanup) {
        if (failure == null) failure = cleanup;
        else if (failure != cleanup) failure.addSuppressed(cleanup);
        if (fatal == null && cleanup instanceof Error error) fatal = error;
      }
      if (failure == null) {
        result.complete(new Selection(selector.included(), selector.observedLive(), selector.more()));
      } else {
        result.completeExceptionally(failure);
      }
      if (fatal != null) throw fatal;
    }
  }

  private void requireActive() throws BackendFailure {
    if (scope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
    if (!account.isActive()) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
