package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Reads the authoritative owner and forwards at most once; a post-send failure remains unknown. */
public final class MutationRelay {
  public sealed interface Result permits KnownResult, KnownRejection, ResultUnknown {}
  public record KnownResult(byte[] responseBytes) implements Result {
    public KnownResult { responseBytes = responseBytes.clone(); }
    @Override public byte[] responseBytes() { return responseBytes.clone(); }
  }
  public record KnownRejection(BackendFailure.Category category) implements Result {
    public KnownRejection { Objects.requireNonNull(category, "category"); }
  }
  public record ResultUnknown(BackendFailure.Category category) implements Result {
    public ResultUnknown {
      Objects.requireNonNull(category, "category");
      if (category != BackendFailure.Category.TIMEOUT && category != BackendFailure.Category.CANCELLED
          && category != BackendFailure.Category.UNAVAILABLE && category != BackendFailure.Category.UNKNOWN) {
        throw new IllegalArgumentException("result-unknown requires an indeterminate failure category");
      }
    }
  }

  private final CommitControlStore control;
  private final OwnerForwarding.Directory directory;
  private final OwnerForwarding.AuthenticatedChannel channel;

  public MutationRelay(CommitControlStore control, OwnerForwarding.Directory directory,
      OwnerForwarding.AuthenticatedChannel channel) {
    this.control = Objects.requireNonNull(control, "control");
    this.directory = Objects.requireNonNull(directory, "directory");
    this.channel = Objects.requireNonNull(channel, "channel");
  }

  public CompletableFuture<Result> forward(MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    if (request.hopCount() >= MutationForwardRequest.MAX_HOPS) {
      return CompletableFuture.failedFuture(new IllegalStateException("mutation forward hop limit exceeded"));
    }
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) return CompletableFuture.failedFuture(
          new IllegalStateException("authoritative M2 control state is missing"));
      CommitState state = snapshot.orElseThrow().state();
      String owner = state.ownerInstance().orElseThrow(() ->
          new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      if (state.ownerEpoch() != request.expectedOwnerEpoch()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return directory.resolve(request.tableGroupId(), owner, state.ownerEpoch()).thenCompose(route -> {
        if (!route.ownerInstance().equals(owner) || route.ownerEpoch() != state.ownerEpoch()) {
          return CompletableFuture.failedFuture(new IllegalStateException(
              "owner directory result differs from authoritative control state"));
        }
        long remaining = Math.min(request.remainingTimeoutNanos(), scope.remainingDeadlineNanos());
        if (remaining <= 0) return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.TIMEOUT));
        MutationForwardRequest forwarded = request.forwardedOnce(state.ownerEpoch(), remaining);
        // Once sendOnce is invoked, any transport failure may follow an accepted remote request.
        CompletableFuture<OwnerForwarding.Reply> sent;
        try {
          sent = channel.sendOnce(route, forwarded, scope);
        } catch (RuntimeException failure) {
          return CompletableFuture.completedFuture(
              new ResultUnknown(classifyUnknown(failure)));
        }
        if (sent == null) {
          return CompletableFuture.completedFuture(
              new ResultUnknown(BackendFailure.Category.UNKNOWN));
        }
        return sent.<Result>handle((reply, failure) -> {
          if (failure != null) return new ResultUnknown(classifyUnknown(failure));
          if (reply == null) return new ResultUnknown(BackendFailure.Category.UNKNOWN);
          if (reply instanceof OwnerForwarding.Completed completed) {
            return new KnownResult(completed.operationResult());
          }
          return new KnownRejection(((OwnerForwarding.Rejected) reply).category());
        });
      });
    });
  }

  private static BackendFailure.Category classifyUnknown(Throwable failure) {
    Throwable cause = failure;
    while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
    if (cause instanceof BackendFailure backend) {
      return switch (backend.category()) {
        case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> backend.category();
        default -> BackendFailure.Category.UNKNOWN;
      };
    }
    return BackendFailure.Category.UNKNOWN;
  }
}
