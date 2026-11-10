package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.scan.BudgetManager;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Verifies the durable directory and every referenced page without assembling a response. */
public final class ResultCatalog {
  private final ResultRepository repository;

  public ResultCatalog(ResultRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  /** The caller reserves IO capacity until this check and any late producers have drained. */
  public CompletableFuture<ResultDirectory> verify(ResultReference reference, RequestScope scope) {
    Objects.requireNonNull(reference, "reference");
    Objects.requireNonNull(scope, "scope");
    return repository.readDirectory(reference.identity(), scope).thenCompose(found -> {
      ResultDirectory directory = matchedDirectory(reference, found);
      CompletableFuture<Void> traversal = CompletableFuture.completedFuture(null);
      for (ResultPageReference page : directory.pages()) {
        traversal = traversal.thenCompose(ignored ->
            repository.readPage(reference.identity(), page.index(), scope).thenAccept(foundPage -> {
              matchedPage(page, foundPage);
            }));
      }
      return traversal.thenApply(ignored -> directory);
    });
  }

  /** Loads a complete result under output and IO leases; the caller closes the returned owner. */
  public CompletableFuture<RetainedResult> load(
      ResultReference reference, RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(reference, "reference");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (!account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("result budget belongs to another scope"));
    }
    LeaseOwner output = null;
    LeaseOwner io = null;
    try {
      var outputLease = account.tryRetain(BudgetManager.Memory.OUTPUT, reference.totalBytes())
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      output = own(scope, outputLease);
      var ioLease = account.tryRetain(BudgetManager.Memory.IO, 8L * ResultPage.MAX_PAYLOAD_BYTES)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      io = own(scope, ioLease);
      byte[] bytes = new byte[Math.toIntExact(reference.totalBytes())];
      RetainedResult result = new RetainedResult(bytes, output);
      LeaseOwner readIo = io;
      return readResult(reference, scope, bytes).thenApply(ignored -> {
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          throw new java.util.concurrent.CompletionException(new BackendFailure(
              scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
        }
        return result;
      }).whenComplete((ignored, failure) -> {
        try {
          readIo.close();
        } catch (RuntimeException | Error cleanup) {
          closeAfterFailure(null, result.output, cleanup);
          throw cleanup;
        }
        if (failure != null) result.close();
      });
    } catch (RuntimeException | BackendFailure failure) {
      closeAfterFailure(io, output, failure);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      closeAfterFailure(io, output, fatal);
      throw fatal;
    }
  }

  private static LeaseOwner own(RequestScope scope, BudgetManager.Lease lease) {
    try {
      return new LeaseOwner(scope, lease);
    } catch (RuntimeException | Error failure) {
      try { lease.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static void closeAfterFailure(LeaseOwner io, LeaseOwner output, Throwable failure) {
    for (LeaseOwner owner : new LeaseOwner[] {io, output}) {
      if (owner == null) continue;
      try { owner.close(); } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
    }
  }

  private CompletableFuture<Void> readResult(
      ResultReference reference, RequestScope scope, byte[] target) {
    return repository.readDirectory(reference.identity(), scope).thenCompose(found -> {
      ResultDirectory directory = matchedDirectory(reference, found);
      int[] offset = {0};
      CompletableFuture<Void> traversal = CompletableFuture.completedFuture(null);
      for (ResultPageReference page : directory.pages()) {
        traversal = traversal.thenCompose(ignored ->
            repository.readPage(reference.identity(), page.index(), scope).thenAccept(foundPage -> {
              ResultPage actual = matchedPage(page, foundPage);
              byte[] payload = actual.payload();
              System.arraycopy(payload, 0, target, offset[0], payload.length);
              offset[0] += payload.length;
            }));
      }
      return traversal.thenRun(() -> {
        if (offset[0] != target.length) throw new IllegalStateException("result length changed during load");
      });
    });
  }

  private static ResultDirectory matchedDirectory(ResultReference reference, Optional<ResultDirectory> found) {
    ResultDirectory directory = found.orElseThrow(() ->
        new IllegalStateException("durable result directory is missing"));
    if (!reference.matches(directory)) {
      throw new IllegalStateException("durable result directory does not match its reference");
    }
    return directory;
  }

  private static ResultPage matchedPage(ResultPageReference reference, Optional<ResultPage> found) {
    ResultPage page = found.orElseThrow(() -> new IllegalStateException("durable result page is missing"));
    if (!reference.matches(page)) {
      throw new IllegalStateException("durable result page does not match its reference");
    }
    return page;
  }

  /**
   * A read-only result buffer. An asynchronous consumer must register its drain before transfer;
   * without that registration no view may outlive the owner.
   */
  public static final class RetainedResult implements AutoCloseable {
    private final byte[] bytes;
    private final LeaseOwner output;
    private final Object lock = new Object();
    private CompletionStage<?> consumerDrain;
    private boolean closing;

    private RetainedResult(byte[] bytes, LeaseOwner output) {
      this.bytes = bytes;
      this.output = output;
    }

    public ByteBuffer buffer() {
      synchronized (lock) {
        if (closing || output.closed.get() || output.scope.isClosed() || output.scope.isDeadlineExceeded()) {
          throw new IllegalStateException("result owner is closed");
        }
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
      }
    }

    public boolean belongsTo(RequestScope scope) { return output.scope == scope; }

    /** Registers the actual transport/consumer drain before handing it a buffer view. */
    public boolean retainForConsumer(CompletionStage<?> drained) {
      Objects.requireNonNull(drained, "drained");
      synchronized (lock) {
        if (closing || consumerDrain != null) throw new IllegalStateException("result already transferred or closed");
        boolean registered = output.scope.startIfOpen(() -> output.scope.trackPhysicalProducer(drained));
        if (registered) consumerDrain = drained;
        return registered;
      }
    }

    @Override public void close() {
      CompletionStage<?> drain;
      synchronized (lock) {
        if (closing) return;
        closing = true;
        drain = consumerDrain;
      }
      if (drain == null) output.close();
      else drain.whenComplete((ignored, failure) -> output.close());
    }
  }

  /** Keeps memory accounted until a cancelled scope's actual backend producers stop. */
  private static final class LeaseOwner implements AutoCloseable {
    private final RequestScope scope;
    private final BudgetManager.Lease lease;
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final RequestScope.ScopedCleanup registration;

    private LeaseOwner(RequestScope scope, BudgetManager.Lease lease) {
      this.scope = scope;
      this.lease = lease;
      this.registration = scope.registerScopedCleanup(this::release, released);
    }

    private void release() {
      if (!closed.compareAndSet(false, true)) return;
      if (scope.isClosed()) {
        scope.physicalProducersCompletion().whenComplete((ignored, failure) -> completeRelease());
      } else {
        completeRelease();
      }
    }

    private void completeRelease() {
      try {
        lease.close();
        released.complete(null);
      } catch (RuntimeException | Error failure) {
        released.completeExceptionally(failure);
        throw failure;
      }
    }

    @Override public void close() {
      try {
        registration.close();
      } catch (Exception failure) {
        throw new IllegalStateException("result memory lease did not close", failure);
      }
    }
  }
}
