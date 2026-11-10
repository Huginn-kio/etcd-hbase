package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Evaluates a bounded AND of comparisons against a transaction's protected base view. */
public final class CompareReadExecutor {
  private static final int MAX_CONDITIONS = 128;

  public record Limits(int buckets, long pageBytes, int physicalRowsPerPage, long pointBytes) {
    public Limits {
      if (buckets < 1 || buckets > 4096 || pageBytes < 1 || pageBytes > 64L * 1024 * 1024
          || physicalRowsPerPage < 1 || physicalRowsPerPage > 1_000_000 || pointBytes < 1) {
        throw new IllegalArgumentException("invalid comparison read limits");
      }
    }
  }

  private final TxnReadSession session;
  private final ScanScheduler scheduler;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final Limits limits;

  public CompareReadExecutor(TxnReadSession session, ScanScheduler scheduler,
      BudgetManager.Account account, RequestScope scope, Limits limits) {
    this.session = Objects.requireNonNull(session, "session");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.limits = Objects.requireNonNull(limits, "limits");
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || limits.pointBytes() > account.reservation().output()
        || account.reservation().heads() / limits.buckets() < limits.pageBytes()) {
      throw new IllegalArgumentException("comparison limits exceed the request reservation");
    }
  }

  boolean belongsTo(TxnReadSession expectedSession, BudgetManager.Account expectedAccount,
      RequestScope expectedScope) {
    return session == expectedSession && account == expectedAccount && scope == expectedScope;
  }

  Limits limits() { return limits; }

  ScanSource scanSource(long revision) {
    return scheduler.bind(session.reader(revision), account);
  }

  /** The caller must complete the session's final protection check before using this result. */
  public CompletableFuture<Boolean> evaluate(List<CompareCondition> conditions) {
    Objects.requireNonNull(conditions, "conditions");
    if (conditions.size() > MAX_CONDITIONS) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("too many comparisons"));
    }
    try {
      requireActive();
      RecordStore base = session.reader(0);
      return evaluateNext(List.copyOf(conditions), 0, base);
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<Boolean> evaluateNext(List<CompareCondition> conditions,
      int index, RecordStore base) {
    if (index == conditions.size()) return CompletableFuture.completedFuture(true);
    try {
      requireActive();
      return evaluateOne(conditions.get(index), base).thenCompose(matched -> matched
          ? evaluateNext(conditions, index + 1, base) : CompletableFuture.completedFuture(false));
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<Boolean> evaluateOne(CompareCondition condition, RecordStore base) {
    var evaluator = new CompareEvaluator(condition);
    if (condition.range().kind() == KeyRange.Kind.EMPTY) {
      return CompletableFuture.completedFuture(evaluator.finish());
    }
    if (condition.range().kind() == KeyRange.Kind.POINT) {
      ReadProjection projection = condition.target() == CompareCondition.Target.VALUE
          ? ReadProjection.FULL : ReadProjection.METADATA;
      var reader = new DirectRecordReader(base, account, limits.pointBytes());
      return reader.readForPreparation(condition.range().lowerInclusive().orElseThrow(),
          session.currentRevision(), projection).thenApply(point -> {
            try (point) {
              point.record().ifPresent(evaluator::accept);
              return evaluator.finish();
            }
          });
    }
    var scan = new ScanRequest(0, condition.range(), Optional.empty(), session.currentRevision(),
        ReadProjection.METADATA, 1, limits.pageBytes(), limits.physicalRowsPerPage());
    var source = scheduler.bind(base, account);
    var cursor = condition.target() == CompareCondition.Target.VALUE
        ? SortedMergeCursor.values(base, source, scan, limits.buckets(), account, scope)
        : SortedMergeCursor.metadata(base, source, scan, limits.buckets(), account, scope);
    return cursor.read(0).thenApply(page -> {
      try (page) {
        page.records().forEach(evaluator::accept);
        return evaluator.finish();
      }
    }).whenComplete((ignored, failure) -> cursor.close());
  }

  private void requireActive() throws BackendFailure {
    if (scope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
    if (!account.isActive()) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
