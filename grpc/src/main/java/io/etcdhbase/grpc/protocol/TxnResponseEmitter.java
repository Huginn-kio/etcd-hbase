package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.ResponseBudget;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Holds result and response capacity until the actual synchronous transport consumer drains. */
public final class TxnResponseEmitter {
  private TxnResponseEmitter() {}

  public static void sendReadOnly(TxnCommand command, TxnEvaluator.ValidatedRead read,
      long clusterId, long memberId, RequestScope scope, BudgetManager.Account account,
      StreamObserver<Rpc.TxnResponse> observer) {
    send(command, new ReadSource(Objects.requireNonNull(read, "read")),
        clusterId, memberId, scope, account, observer);
  }

  /** Takes ownership of the retained durable result, including when response delivery fails. */
  public static void sendResolved(TxnCommand command, InvocationResultResolver.ResolvedTransaction resolved,
      Function<ByteBuffer, ResultTree> decoder, long clusterId, long memberId,
      RequestScope scope, BudgetManager.Account account, StreamObserver<Rpc.TxnResponse> observer) {
    Objects.requireNonNull(resolved, "resolved");
    if (decoder == null) {
      var failure = new NullPointerException("decoder");
      try { resolved.close(); }
      catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
    send(command, new StoredSource(resolved, decoder), clusterId, memberId, scope, account, observer);
  }

  private static void send(TxnCommand command, Source source, long clusterId, long memberId,
      RequestScope scope, BudgetManager.Account account, StreamObserver<Rpc.TxnResponse> observer) {
    try {
      Objects.requireNonNull(command, "command");
      Objects.requireNonNull(scope, "scope");
      Objects.requireNonNull(account, "account");
      Objects.requireNonNull(observer, "observer");
      sendOwned(command, source, clusterId, memberId, scope, account, observer);
    } catch (RuntimeException | Error failure) {
      try { source.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static void sendOwned(TxnCommand command, Source source, long clusterId, long memberId,
      RequestScope scope, BudgetManager.Account account, StreamObserver<Rpc.TxnResponse> observer) {
    var drained = new CompletableFuture<Void>();
    BudgetManager.Lease responseLease = null;
    RequestScope.ScopedCleanup responseOwner = null;
    boolean terminalAttempted = false;
    try {
      if (!account.belongsTo(scope)) throw new IllegalArgumentException("response account belongs to another request");
      if (!source.retain(scope, drained)) return;
      responseLease = account.tryRetain(BudgetManager.Memory.OUTPUT,
          ResponseBudget.forEncodedResult(source.retainedBytes()))
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      responseOwner = releaseAfterDrain(scope, responseLease, drained);
      ResultTree tree = Objects.requireNonNull(source.decode(), "decoded transaction result");
      Rpc.TxnResponse response = TxnResponseMapper.toWire(command, tree, source.revision(),
          source.changed(), clusterId, memberId);
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      observer.onNext(response);
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      terminalAttempted = true;
      observer.onCompleted();
    } catch (BackendFailure | RuntimeException failure) {
      if (!terminalAttempted && !scope.isClosed()) signalError(observer, failure);
      else if (terminalAttempted) throw new IllegalStateException("transaction response termination failed", failure);
    } catch (Error fatal) {
      if (!terminalAttempted && !scope.isClosed()) signalError(observer, fatal);
      throw fatal;
    } finally {
      drained.complete(null);
      try {
        source.close();
      } finally {
        if (responseOwner != null) close(responseOwner);
        else if (responseLease != null) responseLease.close();
      }
    }
  }

  private static RequestScope.ScopedCleanup releaseAfterDrain(RequestScope scope,
      BudgetManager.Lease lease, CompletableFuture<Void> drained) {
    var released = new CompletableFuture<Void>();
    return scope.registerScopedCleanup(() -> drained.whenComplete((ignored, failure) -> {
      try {
        lease.close();
        released.complete(null);
      } catch (RuntimeException | Error cleanup) {
        released.completeExceptionally(cleanup);
      }
    }), released);
  }

  private static void signalError(StreamObserver<Rpc.TxnResponse> observer, Throwable failure) {
    try { observer.onError(ErrorProjection.project(failure)); }
    catch (RuntimeException | Error terminationFailure) {
      if (failure != terminationFailure) failure.addSuppressed(terminationFailure);
      if (terminationFailure instanceof Error fatal) throw fatal;
    }
  }

  private static void close(RequestScope.ScopedCleanup owner) {
    try { owner.close(); }
    catch (Exception failure) { throw new IllegalStateException("transaction response memory did not close", failure); }
  }

  private sealed interface Source permits ReadSource, StoredSource {
    boolean retain(RequestScope scope, CompletableFuture<Void> drained);
    long retainedBytes();
    ResultTree decode();
    long revision();
    boolean changed();
    void close();
  }

  private record ReadSource(TxnEvaluator.ValidatedRead read) implements Source {
    @Override public boolean retain(RequestScope scope, CompletableFuture<Void> drained) {
      if (!read.belongsTo(scope)) throw new IllegalArgumentException("read result belongs to another request");
      return scope.startIfOpen(() -> scope.trackPhysicalProducer(drained));
    }
    @Override public long retainedBytes() { return read.result().retainedBytes(); }
    @Override public ResultTree decode() { return read.result(); }
    @Override public long revision() { return read.revision(); }
    @Override public boolean changed() { return false; }
    @Override public void close() { }
  }

  private record StoredSource(InvocationResultResolver.ResolvedTransaction result,
      Function<ByteBuffer, ResultTree> decoder) implements Source {
    @Override public boolean retain(RequestScope scope, CompletableFuture<Void> drained) {
      if (!result.payload().belongsTo(scope)) throw new IllegalArgumentException("stored result belongs to another request");
      return result.payload().retainForConsumer(drained);
    }
    @Override public long retainedBytes() { return result.payload().buffer().remaining(); }
    @Override public ResultTree decode() { return decoder.apply(result.payload().buffer()); }
    @Override public long revision() { return result.revision(); }
    @Override public boolean changed() { return result.changed(); }
    @Override public void close() { result.close(); }
  }
}
