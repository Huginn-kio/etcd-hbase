package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Claims and renews the single commit owner through the authoritative control-row CAS. */
public final class CommitOwnerService {
  private static final int MAX_CAS_ATTEMPTS = 16;

  public record OwnerToken(String instance, long epoch) {
    public OwnerToken {
      Objects.requireNonNull(instance, "instance");
      if (instance.isBlank() || epoch <= 0) throw new IllegalArgumentException("invalid owner token");
    }
  }

  private final CommitControlStore control;

  public CommitOwnerService(CommitControlStore control) {
    this.control = Objects.requireNonNull(control, "control");
  }

  /** Generates a new process incarnation; do not reuse a pod name across restarts. */
  public static String newInstanceId() {
    return UUID.randomUUID().toString();
  }

  /** Returns empty while another unexpired owner holds the slot. */
  public CompletableFuture<Optional<OwnerToken>> claim(
      String instance, long nowEpochMillis, long leaseDeadlineEpochMillis, RequestScope scope) {
    return claim(instance, nowEpochMillis, leaseDeadlineEpochMillis, scope, 0);
  }

  /** Refreshes owner liveness without changing the KV revision or commit sequence. */
  public CompletableFuture<Boolean> heartbeat(
      OwnerToken owner, long leaseDeadlineEpochMillis, RequestScope scope) {
    return heartbeat(owner, leaseDeadlineEpochMillis, scope, 0);
  }

  private CompletableFuture<Optional<OwnerToken>> claim(
      String instance, long nowMillis, long leaseDeadlineMillis, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "M2 control state must be explicitly initialized before owner claim"));
      }
      CommitState current = snapshot.orElseThrow().state();
      if (current.ownerInstance().filter(instance::equals).isPresent()
          && !current.ownerLeaseExpired(nowMillis)) {
        return CompletableFuture.completedFuture(Optional.of(new OwnerToken(instance, current.ownerEpoch())));
      }
      if (current.ownerEpoch() > 0 && !current.ownerLeaseExpired(nowMillis)) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      CommitState claimed = current.claimOwner(instance, nowMillis, leaseDeadlineMillis);
      return control.compareAndSet(snapshot.orElseThrow(), claimed, scope)
          .handle((updated, failure) -> new CasResult(updated, failure))
          .thenCompose(result -> resolveClaim(instance, nowMillis, leaseDeadlineMillis,
              claimed, scope, attempt, result));
    });
  }

  private CompletableFuture<Optional<OwnerToken>> resolveClaim(
      String instance, long nowMillis, long leaseDeadlineMillis, CommitState claimed,
      RequestScope scope, int attempt, CasResult result) {
    if (result.failure() == null) {
      return result.updated()
          ? CompletableFuture.completedFuture(Optional.of(new OwnerToken(instance, claimed.ownerEpoch())))
          : claim(instance, nowMillis, leaseDeadlineMillis, scope, attempt + 1);
    }
    if (!isUnknown(result.failure())) return CompletableFuture.failedFuture(unwrap(result.failure()));
    return control.read(scope).handle((snapshot, rereadFailure) -> {
      if (rereadFailure != null) {
        Throwable unknown = unwrap(result.failure());
        unknown.addSuppressed(unwrap(rereadFailure));
        throw new CompletionException(unknown);
      }
      if (snapshot.isPresent()) {
        CommitState observed = snapshot.orElseThrow().state();
        if (observed.ownerEpoch() == claimed.ownerEpoch()
            && observed.ownerInstance().filter(instance::equals).isPresent()) {
          return Optional.of(new OwnerToken(instance, claimed.ownerEpoch()));
        }
      }
      return null;
    }).thenCompose(token -> token != null
        ? CompletableFuture.completedFuture(token)
        : claim(instance, nowMillis, leaseDeadlineMillis, scope, attempt + 1));
  }

  private CompletableFuture<Boolean> heartbeat(
      OwnerToken owner, long leaseDeadlineMillis, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) return CompletableFuture.failedFuture(
          new IllegalStateException("M2 control state disappeared"));
      ControlSnapshot observed = snapshot.orElseThrow();
      CommitState current = observed.state();
      if (current.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || current.ownerEpoch() != owner.epoch()) {
        return CompletableFuture.completedFuture(false);
      }
      if (current.ownerLeaseDeadlineEpochMillis() >= leaseDeadlineMillis) {
        return CompletableFuture.completedFuture(true);
      }
      CommitState renewed = current.heartbeat(owner.instance(), owner.epoch(), leaseDeadlineMillis);
      return control.compareAndSet(observed, renewed, scope)
          .handle((updated, failure) -> new CasResult(updated, failure))
          .thenCompose(result -> resolveHeartbeat(owner, leaseDeadlineMillis, renewed,
              scope, attempt, result));
    });
  }

  private CompletableFuture<Boolean> resolveHeartbeat(
      OwnerToken owner, long leaseDeadlineMillis, CommitState renewed,
      RequestScope scope, int attempt, CasResult result) {
    if (result.failure() == null) {
      return result.updated()
          ? CompletableFuture.completedFuture(true)
          : heartbeat(owner, leaseDeadlineMillis, scope, attempt + 1);
    }
    if (!isUnknown(result.failure())) return CompletableFuture.failedFuture(unwrap(result.failure()));
    return control.read(scope).handle((snapshot, rereadFailure) -> {
      if (rereadFailure != null) {
        Throwable unknown = unwrap(result.failure());
        unknown.addSuppressed(unwrap(rereadFailure));
        throw new CompletionException(unknown);
      }
      if (snapshot.isPresent()) {
        CommitState observed = snapshot.orElseThrow().state();
        if (observed.ownerInstance().filter(owner.instance()::equals).isPresent()
            && observed.ownerEpoch() == owner.epoch()
            && observed.ownerLeaseDeadlineEpochMillis() >= renewed.ownerLeaseDeadlineEpochMillis()) {
          return true;
        }
        if (observed.ownerInstance().filter(owner.instance()::equals).isEmpty()
            || observed.ownerEpoch() != owner.epoch()) return false;
      }
      return null;
    }).thenCompose(renewedOrStale -> renewedOrStale != null
        ? CompletableFuture.completedFuture(renewedOrStale)
        : heartbeat(owner, leaseDeadlineMillis, scope, attempt + 1));
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof BackendFailure backend) {
      return switch (backend.category()) {
        case TIMEOUT, UNAVAILABLE, UNKNOWN -> true;
        default -> false;
      };
    }
    return false;
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private record CasResult(Boolean updated, Throwable failure) {}
}
