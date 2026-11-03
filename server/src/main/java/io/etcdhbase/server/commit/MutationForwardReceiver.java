package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Revalidates a forwarded request at the owner before dispatching it to the commit kernel. */
public final class MutationForwardReceiver {
  public record VerifiedAuthorization(
      AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest) {
    public VerifiedAuthorization {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
      if (authorizationScopeDigest.length() != 32) {
        throw new IllegalArgumentException("authorization scope digest must be SHA-256");
      }
    }
  }

  @FunctionalInterface
  public interface AuthorizationResolver {
    /**
     * Authenticates the transport peer and resolves the opaque context reference. The peer value
     * must come from the authenticated channel, never from an untrusted request field.
     */
    CompletableFuture<VerifiedAuthorization> verifyAndResolve(
        String authenticatedPeerIdentity, MutationForwardRequest request, RequestScope scope);
  }

  @FunctionalInterface
  public interface MutationProcessor {
    /**
     * Processes or resolves this durable invocation. Implementations must use the returned
     * binding, never allocate a second txnId for the same invocation, and only return Rejected
     * when they can prove no mutation was dispatched.
     */
    CompletableFuture<OwnerForwarding.Reply> process(
        MutationForwardRequest request,
        InvocationBinding binding,
        AuthorizedBackendIdentity identity,
        RequestScope scope);
  }

  private final CommitControlStore control;
  private final InvocationService invocations;
  private final AuthorizationResolver authorizations;
  private final MutationProcessor processor;
  private final Clock clock;

  public MutationForwardReceiver(
      CommitControlStore control,
      InvocationService invocations,
      AuthorizationResolver authorizations,
      MutationProcessor processor) {
    this(control, invocations, authorizations, processor, Clock.systemUTC());
  }

  MutationForwardReceiver(
      CommitControlStore control,
      InvocationService invocations,
      AuthorizationResolver authorizations,
      MutationProcessor processor,
      Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.authorizations = Objects.requireNonNull(authorizations, "authorizations");
    this.processor = Objects.requireNonNull(processor, "processor");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** A returned Rejected reply guarantees that the mutation processor was never invoked. */
  public CompletableFuture<OwnerForwarding.Reply> receive(
      String authenticatedPeerIdentity, MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    if (!validPeerIdentity(authenticatedPeerIdentity)
        || request.hopCount() != MutationForwardRequest.MAX_HOPS
        || !request.requestDigest().equals(
            MutationForwardRequest.digest(request.tableGroupId(), request.operationBytes()))) {
      return rejected(BackendFailure.Category.PERMISSION);
    }
    if (scope.isDeadlineExceeded() || scope.isClosed()) {
      return rejected(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    }
    return control.read(scope).handle((snapshot, failure) -> new ControlRead(snapshot, failure))
        .thenCompose(read -> {
          if (read.failure() != null) return rejected(category(read.failure()));
          if (read.snapshot().isEmpty()) return rejected(BackendFailure.Category.UNAVAILABLE);
          CommitState state = read.snapshot().orElseThrow().state();
          if (!isCurrentOwner(state, request.expectedOwnerEpoch())
              || state.publication().isPresent()) {
            return rejected(BackendFailure.Category.UNAVAILABLE);
          }
          return authorizeAndBind(authenticatedPeerIdentity, request, scope, state);
        });
  }

  private CompletableFuture<OwnerForwarding.Reply> authorizeAndBind(
      String peerIdentity, MutationForwardRequest request, RequestScope scope, CommitState observed) {
    CompletableFuture<VerifiedAuthorization> authorization;
    try {
      authorization = authorizations.verifyAndResolve(peerIdentity, request, scope);
    } catch (RuntimeException failure) {
      return rejected(category(failure));
    }
    if (authorization == null) return rejected(BackendFailure.Category.UNKNOWN);
    return authorization.handle((verified, failure) -> new AuthorizationResult(verified, failure))
        .thenCompose(result -> {
          if (result.failure() != null) return rejected(category(result.failure()));
          if (!matchesAuthorization(request, result.authorization())) {
            return rejected(BackendFailure.Category.PERMISSION);
          }
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return rejected(scope.isDeadlineExceeded()
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
          }
          InvocationBinding proposed = InvocationBinding.begin(
              request.invocationId(), request.requestDigest(),
              result.authorization().authorizationScopeDigest(), TransactionId.create(),
              observed.ownerEpoch(), observed.publishedSequence(), observed.publishedRevision());
          return bindAndDispatch(request, scope, observed, result.authorization(), proposed);
        });
  }

  private CompletableFuture<OwnerForwarding.Reply> bindAndDispatch(
      MutationForwardRequest request,
      RequestScope scope,
      CommitState observed,
      VerifiedAuthorization authorization,
      InvocationBinding proposed) {
    CompletableFuture<InvocationBinding> bound;
    try {
      bound = invocations.bindOrJoin(proposed, scope);
    } catch (RuntimeException failure) {
      return rejected(category(failure));
    }
    if (bound == null) return rejected(BackendFailure.Category.UNKNOWN);
    return bound.handle((binding, failure) -> new BindingResult(binding, failure))
        .thenCompose(result -> {
          if (result.failure() != null) return rejected(category(result.failure()));
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return rejected(scope.isDeadlineExceeded()
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
          }
          return verifyOwnerBeforeDispatch(request, scope, observed, authorization, result.binding());
        });
  }

  private CompletableFuture<OwnerForwarding.Reply> verifyOwnerBeforeDispatch(
      MutationForwardRequest request,
      RequestScope scope,
      CommitState observed,
      VerifiedAuthorization authorization,
      InvocationBinding binding) {
    return control.read(scope).handle((snapshot, failure) -> new ControlRead(snapshot, failure))
        .thenCompose(read -> {
          if (read.failure() != null) return rejected(category(read.failure()));
          if (read.snapshot().isEmpty()) return rejected(BackendFailure.Category.UNAVAILABLE);
          CommitState current = read.snapshot().orElseThrow().state();
          if (!isCurrentOwner(current, request.expectedOwnerEpoch())
              || !current.ownerInstance().equals(observed.ownerInstance())
              || current.publication().isPresent()
              || current.publishedSequence() != observed.publishedSequence()
              || current.publishedRevision() != observed.publishedRevision()) {
            return rejected(BackendFailure.Category.UNAVAILABLE);
          }
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return rejected(scope.isDeadlineExceeded()
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
          }
          try {
            CompletableFuture<OwnerForwarding.Reply> processing = processor.process(
                request, binding, authorization.identity(), scope);
            return processing == null
                ? CompletableFuture.failedFuture(new IllegalStateException(
                    "mutation processor returned no future"))
                : processing;
          } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
          }
        });
  }

  private boolean isCurrentOwner(CommitState state, long expectedEpoch) {
    return state.ownerInstance().isPresent() && state.ownerEpoch() == expectedEpoch
        && !state.ownerLeaseExpired(clock.millis());
  }

  private static boolean matchesAuthorization(
      MutationForwardRequest request, VerifiedAuthorization verified) {
    if (verified == null) return false;
    AuthorizedBackendIdentity identity = verified.identity();
    byte[] contextReference = identity.contextRef().getBytes(StandardCharsets.UTF_8);
    return identity.scope().tableGroupId().equals(request.tableGroupId())
        && identity.allowedTableGroupIds().contains(request.tableGroupId())
        && verified.authorizationScopeDigest().equals(request.authorizationScopeDigest())
        && Arrays.equals(contextReference, request.authorizationContextReference().bytes());
  }

  private static boolean validPeerIdentity(String identity) {
    return identity != null && !identity.isBlank() && identity.length() <= 512
        && identity.codePoints().noneMatch(Character::isISOControl);
  }

  private static CompletableFuture<OwnerForwarding.Reply> rejected(BackendFailure.Category category) {
    return CompletableFuture.completedFuture(new OwnerForwarding.Rejected(category));
  }

  private static BackendFailure.Category category(Throwable failure) {
    Throwable cause = failure;
    while (cause instanceof CompletionException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause instanceof BackendFailure backend
        ? backend.category() : BackendFailure.Category.UNKNOWN;
  }

  private record ControlRead(Optional<ControlSnapshot> snapshot, Throwable failure) {}
  private record AuthorizationResult(VerifiedAuthorization authorization, Throwable failure) {}
  private record BindingResult(InvocationBinding binding, Throwable failure) {}
}
