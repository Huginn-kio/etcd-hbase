package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

/** Plans a flat transaction privately; publishing and final read validation remain caller-owned. */
public final class TxnEvaluator {
  public record Limits(long pointBytes, long resultBytes, long workingBytes) {
    public Limits {
      if (pointBytes < 1 || resultBytes < 128 || resultBytes > ResultTree.MAX_RETAINED_BYTES
          || workingBytes < 1 || workingBytes > ResultTree.MAX_RETAINED_BYTES) {
        throw new IllegalArgumentException("invalid transaction memory limits");
      }
    }
  }

  /** Private result remains bound to its original invocation, scope and protected read session. */
  public static final class Evaluation {
    private final ResultTree result;
    private final List<PreparedCandidate> candidates;
    private final List<PreparedEvent> events;
    private final Optional<InvocationBinding> invocation;
    private final TxnReadSession session;

    private Evaluation(ResultTree result, List<PreparedCandidate> candidates,
        List<PreparedEvent> events, Optional<InvocationBinding> invocation, TxnReadSession session) {
      this.result = Objects.requireNonNull(result, "result");
      this.candidates = List.copyOf(candidates);
      this.events = List.copyOf(events);
      this.invocation = Objects.requireNonNull(invocation, "invocation");
      this.session = Objects.requireNonNull(session, "session");
    }

    public ResultTree result() { return result; }
    public List<PreparedCandidate> candidates() { return candidates; }
    public List<PreparedEvent> events() { return events; }
    public boolean belongsTo(InvocationBinding expected, RequestScope scope) {
      return session.belongsTo(scope) && invocation.filter(binding -> binding.sameAttempt(expected)).isPresent();
    }
    public CompletableFuture<Void> finishRead() { return session.finish(); }

    /** Produces an externally projectable read only after all transaction pins validate. */
    public CompletableFuture<ValidatedRead> finishReadOnly() {
      if (invocation.isPresent() || !candidates.isEmpty() || !events.isEmpty()) {
        return CompletableFuture.failedFuture(new IllegalStateException("result is not a read-only transaction"));
      }
      return session.finish().thenApply(ignored -> new ValidatedRead(result, session));
    }
  }

  public static final class ValidatedRead {
    private final ResultTree result;
    private final TxnReadSession session;
    private ValidatedRead(ResultTree result, TxnReadSession session) {
      this.result = result;
      this.session = session;
    }
    public ResultTree result() { return result; }
    public long revision() { return result.root().header().revision(); }
    public boolean belongsTo(RequestScope scope) { return session.belongsTo(scope); }
  }

  private final TxnReadSession session;
  private final CompareReadExecutor compares;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final Limits limits;
  private final ToIntFunction<ByteKey> bucket;
  private final AtomicBoolean started = new AtomicBoolean();

  public TxnEvaluator(TxnReadSession session, CompareReadExecutor compares,
      BudgetManager.Account account, RequestScope scope, Limits limits,
      ToIntFunction<ByteKey> bucket) {
    this.session = Objects.requireNonNull(session, "session");
    this.compares = Objects.requireNonNull(compares, "compares");
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || !compares.belongsTo(session, account, scope)
        || limits.resultBytes() > account.reservation().output()
        || limits.pointBytes() > account.reservation().output() - limits.resultBytes()) {
      throw new IllegalArgumentException("transaction inputs do not share a sufficient request budget");
    }
  }

  /**
   * A potentially writing request requires its original invocation. The caller closes the scope
   * after publication or rejection, and must register subsequent physical producers before use.
   * Neither this result nor any partial response may be exposed before session.finish succeeds.
   */
  public CompletableFuture<Evaluation> evaluate(TxnCommand command,
      Optional<InvocationBinding> invocation) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(invocation, "invocation");
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(new IllegalStateException("transaction evaluator already used"));
    }
    if (command.mayWrite() && invocation.isEmpty()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("writing transaction requires an invocation"));
    }
    if (invocation.filter(binding -> binding.state() != InvocationBinding.State.PREPARING
        || binding.baseRevision() != session.currentRevision()
        || binding.baseSequence() != session.barrierSequence()
        || !session.authorizes(binding.authorizationScopeDigest())).isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invocation and transaction barrier differ"));
    }
    if (command.mayWrite() && !session.readyForMutation()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive());
    }
    CompletableFuture<Evaluation> operation;
    try {
      retainUntilDrain(requireMemory(BudgetManager.Memory.OUTPUT, limits.resultBytes()));
      TxnWorkingSet working = null;
      if (command.mayWrite()) {
        retainUntilDrain(requireMemory(BudgetManager.Memory.CANDIDATE, limits.workingBytes()));
        working = new TxnWorkingSet(invocation.orElseThrow(), limits.workingBytes());
      }
      var state = new State(working, invocation);
      operation = compares.evaluate(command.compares()).thenCompose(matched -> {
        state.succeeded = matched;
        return execute(matched ? command.success() : command.failure(), 0, state);
      });
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> drained.complete(null));
    return operation.copy();
  }

  private CompletableFuture<Evaluation> execute(List<TxnCommand.Operation> operations,
      int index, State state) {
    try {
      requireActive();
      session.reader(0);
      if (index == operations.size()) return CompletableFuture.completedFuture(finish(state));
      TxnCommand.Operation operation = operations.get(index);
      if (operation instanceof TxnCommand.Unsupported) {
        return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION));
      }
      if (operation instanceof TxnCommand.PointRange range) {
        Optional<StoredRecord> overlay = state.working == null ? Optional.empty()
            : state.working.current(range.key(), Optional.empty());
        if (overlay.isPresent()) {
          appendRange(state, index, overlay);
          return execute(operations, index + 1, state);
        }
        return point(range.key()).thenCompose(point -> {
          try (point) { appendRange(state, index, point.record()); }
          catch (BackendFailure failure) { return CompletableFuture.failedFuture(failure); }
          return execute(operations, index + 1, state);
        });
      }
      TxnCommand.Put put = (TxnCommand.Put) operation;
      return point(put.command().key()).thenCompose(point -> {
        try (point) { appendPut(state, index, put.command(), point.record()); }
        catch (BackendFailure failure) { return CompletableFuture.failedFuture(failure); }
        return execute(operations, index + 1, state);
      });
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<io.etcdhbase.core.scan.PointResult> point(ByteKey key) {
    return new DirectRecordReader(session.reader(0), account, limits.pointBytes())
        .readForPreparation(key, session.currentRevision(), ReadProjection.FULL);
  }

  private void appendRange(State state, int ordinal, Optional<StoredRecord> observed) throws BackendFailure {
    List<StoredRecord> records = observed.filter(record -> record.metadata().state() == StorageMetadata.State.LIVE)
        .map(List::of).orElseGet(List::of);
    long bytes = 128;
    if (!records.isEmpty()) bytes = Math.addExact(bytes, resultRecordBytes(records.getFirst()));
    state.reserve(bytes);
    state.responses.add(new ResultNode.Range(ordinal, new ResultNode.Header(true, state.currentRevision()),
        records, records.size(), false));
  }

  private void appendPut(State state, int ordinal, PutCommand command,
      Optional<StoredRecord> base) throws BackendFailure {
    if (state.working == null) throw new IllegalStateException("write was not routed through an invocation");
    PutResult result = state.working.put(command, base);
    long bytes = Math.addExact(128, command.key().length());
    if (result.previous().isPresent()) bytes = Math.addExact(bytes, resultRecordBytes(result.previous().orElseThrow()));
    state.reserve(bytes);
    state.responses.add(new ResultNode.Put(ordinal, new ResultNode.Header(true, result.revision()),
        result.key(), result.previous()));
  }

  private Evaluation finish(State state) throws BackendFailure {
    requireActive();
    var result = new ResultTree(new ResultNode.Txn(0,
        new ResultNode.Header(true, state.currentRevision()), state.succeeded, state.responses));
    if (result.retainedBytes() > limits.resultBytes()) throw resourceLimit();
    return state.working == null ? new Evaluation(result, List.of(), List.of(), state.invocation, session)
        : new Evaluation(result, state.working.candidates(bucket), state.working.events(), state.invocation, session);
  }

  private BudgetManager.Lease requireMemory(BudgetManager.Memory lane, long bytes) throws BackendFailure {
    return account.tryRetain(lane, bytes).orElseThrow(TxnEvaluator::resourceLimit);
  }

  private void retainUntilDrain(BudgetManager.Lease lease) {
    var released = new CompletableFuture<Void>();
    try {
      scope.registerScopedCleanup(() -> scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
        try {
          lease.close();
          released.complete(null);
        } catch (RuntimeException | Error cleanup) {
          released.completeExceptionally(cleanup);
        }
      }), released);
    } catch (RuntimeException | Error failure) {
      try { lease.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private void requireActive() throws BackendFailure {
    if (scope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
    if (!account.isActive()) throw resourceLimit();
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  private static long resultRecordBytes(StoredRecord record) {
    return Math.addExact(128L + record.key().length(), record.hasValue() ? record.metadata().valueLength() : 0);
  }

  private final class State {
    private final TxnWorkingSet working;
    private final Optional<InvocationBinding> invocation;
    private final List<ResultNode> responses = new ArrayList<>();
    private long resultBytes = 128;
    private boolean succeeded;

    private State(TxnWorkingSet working, Optional<InvocationBinding> invocation) {
      this.working = working;
      this.invocation = invocation;
    }
    private long currentRevision() {
      return working != null && working.hasChanges()
          ? Math.addExact(session.currentRevision(), 1) : session.currentRevision();
    }
    private void reserve(long bytes) throws BackendFailure {
      if (bytes > limits.resultBytes() - resultBytes) throw resourceLimit();
      resultBytes += bytes;
    }
  }
}
