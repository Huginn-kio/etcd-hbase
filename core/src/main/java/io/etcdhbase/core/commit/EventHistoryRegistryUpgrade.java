package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Stages an isolated event registry before selecting it with the owner-fenced control CAS. */
public final class EventHistoryRegistryUpgrade {
  private static final long STAGING_TIMEOUT_NANOS = Duration.ofSeconds(30).toNanos();
  private final SystemRecordStore records;
  private final CommitControlStore control;
  private final SnapshotPinConfiguration configuration;
  private final Clock clock;

  public record Selection(CommitOwnerService.OwnerToken owner, EventHistoryPinManager registry) {
    public Selection {
      Objects.requireNonNull(owner, "owner");
      Objects.requireNonNull(registry, "registry");
    }
  }

  public EventHistoryRegistryUpgrade(SystemRecordStore records, CommitControlStore control,
      SnapshotPinConfiguration configuration, Clock clock) {
    this.records = Objects.requireNonNull(records, "records");
    this.control = Objects.requireNonNull(control, "control");
    this.configuration = Objects.requireNonNull(configuration, "configuration");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** On failure the caller must reacquire its owner token; reserved staging rows remain isolated. */
  public CompletableFuture<Selection> upgrade(CommitOwnerService.OwnerToken owner, RequestScope scope) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    ByteKey namespace = ByteKey.copyOf(TransactionId.create().bytes());
    var output = new CompletableFuture<Selection>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    reserve(owner, namespace, scope)
        .thenCompose(reserved -> stage(namespace, scope).thenCompose(registry ->
            select(reserved, namespace, scope).thenApply(selected -> new Selection(selected, registry))))
        .whenComplete((selection, failure) -> {
          if (failure != null) output.completeExceptionally(failure);
          else if (!output.complete(selection)) scope.cancel();
        });
    return output;
  }

  private CompletableFuture<CommitOwnerService.OwnerToken> reserve(
      CommitOwnerService.OwnerToken owner, ByteKey namespace, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) return inactive(scope);
    return control.read(scope).thenCompose(found -> {
      if (scope.isClosed() || scope.isDeadlineExceeded()) return inactive(scope);
      ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("event control row is missing"));
      CommitState replacement = observed.state().reserveEventRegistry(owner.instance(), owner.epoch(),
          clock.millis(), namespace);
      return compare(observed, replacement, namespace, false, scope);
    });
  }

  private CompletableFuture<EventHistoryPinManager> stage(ByteKey namespace, RequestScope parent) {
    if (parent.isClosed() || parent.isDeadlineExceeded()) return inactive(parent);
    RequestScope staging = parent.childWithTimeoutNanos(STAGING_TIMEOUT_NANOS);
    CompletableFuture<Void> drained = CompletableFuture.allOf(
        staging.physicalProducersCompletion().toCompletableFuture(),
        staging.cleanupCompletion().toCompletableFuture());
    parent.registerScopedCleanup(staging, drained);
    return EventHistoryPinManager.stage(records, namespace, configuration, clock, staging)
        .handle((registry, failure) -> {
          staging.close();
          return new Staged(registry, failure);
        }).thenCompose(result -> drained.thenApply(ignored -> {
          if (result.failure() != null) throw new CompletionException(result.failure());
          return Objects.requireNonNull(result.registry(), "staged event registry");
        }));
  }

  private CompletableFuture<CommitOwnerService.OwnerToken> select(
      CommitOwnerService.OwnerToken reserved, ByteKey namespace, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) return inactive(scope);
    return control.read(scope).thenCompose(found -> {
      if (scope.isClosed() || scope.isDeadlineExceeded()) return inactive(scope);
      ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("event control row is missing"));
      CommitState replacement = observed.state().upgradeEventRegistryFormat(reserved.instance(), reserved.epoch(),
          clock.millis(), namespace);
      return compare(observed, replacement, namespace, true, scope);
    });
  }

  private CompletableFuture<CommitOwnerService.OwnerToken> compare(ControlSnapshot observed,
      CommitState replacement, ByteKey namespace, boolean selected, RequestScope scope) {
    var token = new CommitOwnerService.OwnerToken(replacement.ownerInstance().orElseThrow(), replacement.ownerEpoch());
    return control.compareAndSet(observed, replacement, scope)
        .handle((applied, failure) -> new CasResult(applied, failure))
        .thenCompose(result -> {
          if (result.failure() == null) {
            return Boolean.TRUE.equals(result.applied()) ? CompletableFuture.completedFuture(token)
                : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
          }
          Throwable cause = CompletionFailures.unwrapCompletion(result.failure());
          if (!(cause instanceof BackendFailure backend) || !unknown(backend.category())) {
            return CompletableFuture.failedFuture(cause);
          }
          return control.read(scope).thenApply(found -> {
            CommitState current = found.orElseThrow(() -> new CompletionException(cause)).state();
            Optional<ByteKey> actual = selected ? current.eventRegistryNamespace()
                : current.pendingEventRegistryNamespace();
            if (actual.equals(Optional.of(namespace)) && current.ownerEpoch() == token.epoch()
                && current.ownerInstance().filter(token.instance()::equals).isPresent()) return token;
            throw new CompletionException(cause);
          });
        });
  }

  private static boolean unknown(BackendFailure.Category category) {
    return switch (category) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private static <T> CompletableFuture<T> inactive(RequestScope scope) {
    return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
  }

  private record Staged(EventHistoryPinManager registry, Throwable failure) {}
  private record CasResult(Boolean applied, Throwable failure) {}
}
