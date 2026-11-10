package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Resolves a finalized invocation's original result without rereading current business data. */
public final class InvocationResultResolver {
  /** The selected decision accompanies the bytes so protocol projection can verify its revision. */
  public static final class ResolvedResult implements AutoCloseable {
    private final CommitDecision decision;
    private final ResultCatalog.RetainedResult payload;

    private ResolvedResult(CommitDecision decision, ResultCatalog.RetainedResult payload) {
      this.decision = Objects.requireNonNull(decision, "decision");
      this.payload = Objects.requireNonNull(payload, "payload");
    }
    public CommitDecision decision() { return decision; }
    public ResultCatalog.RetainedResult payload() { return payload; }
    @Override public void close() { payload.close(); }
  }

  private final InvocationRepository repository;
  private final InvocationService invocations;
  private final CommitOutcomeResolver outcomes;
  private final ManifestCatalog manifests;
  private final ResultCatalog results;

  public InvocationResultResolver(InvocationRepository repository, CommitOutcomeResolver outcomes,
      ManifestCatalog manifests, ResultCatalog results) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.invocations = new InvocationService(repository);
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.results = Objects.requireNonNull(results, "results");
  }

  public CompletableFuture<ResolvedResult> resolve(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(requestDigest, "requestDigest");
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (requestDigest.length() != 32 || authorizationDigest.length() != 32 || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid result query identity or budget"));
    }
    return readBinding(id, requestDigest, authorizationDigest, scope)
        .thenCompose(binding -> resolveSelectedInvocation(binding, scope))
        .thenCompose(selected -> readVerifiedResult(selected, scope))
        .thenCompose(verified -> loadAndMark(verified.binding(), verified.decision(),
            verified.reference(), scope, account));
  }

  private CompletableFuture<InvocationBinding> readBinding(InvocationId id, ByteKey requestDigest,
      ByteKey authorizationDigest, RequestScope scope) {
    return repository.read(id, scope).thenCompose(found -> {
      InvocationBinding binding = found.orElseThrow(() -> new IllegalStateException("invocation is missing"));
      if (!binding.invocationId().equals(id) || !binding.requestDigest().equals(requestDigest)
          || !binding.authorizationScopeDigest().equals(authorizationDigest)) {
        return CompletableFuture.failedFuture(new IllegalArgumentException("invocation request or authorization differs"));
      }
      if (binding.state() != InvocationBinding.State.SEALED
          && binding.state() != InvocationBinding.State.SELECTED) {
        return CompletableFuture.failedFuture(new IllegalStateException("invocation result is not queryable"));
      }
      return CompletableFuture.completedFuture(binding);
    });
  }

  private CompletableFuture<SelectedInvocation> resolveSelectedInvocation(
      InvocationBinding binding, RequestScope scope) {
    return outcomes.resolve(binding, scope).thenCompose(outcome -> {
      if (outcome.state() != CommitOutcomeResolver.State.SELECTED_FINALIZED) {
        return CompletableFuture.failedFuture(new IllegalStateException("invocation is not finalized"));
      }
      return CompletableFuture.completedFuture(new SelectedInvocation(
          binding, outcome.decision().orElseThrow()));
    });
  }

  private CompletableFuture<VerifiedResult> readVerifiedResult(
      SelectedInvocation selected, RequestScope scope) {
    InvocationBinding binding = selected.binding();
    CommitDecision decision = selected.decision();
    return manifests.readRoot(decision.manifestRoot(), scope).thenCompose(root -> {
      ManifestRoot manifest = root.orElseThrow(() -> new IllegalStateException("result manifest is missing"));
      if (!manifest.systemIdentity().equals(decision.manifestRoot())
          || !manifest.digest().equals(decision.manifestDigest())
          || !manifest.transactionId().equals(binding.transactionId())
          || manifest.ownerEpoch() != binding.preparedOwnerEpoch()
          || manifest.baseSequence() != binding.baseSequence()
          || manifest.baseRevision() != binding.baseRevision()) {
        return CompletableFuture.failedFuture(new IllegalStateException("result manifest differs from the decision"));
      }
      ResultReference reference = manifest.resultReference().orElseThrow(() ->
          new IllegalStateException("finalized business manifest has no result"));
      if (!reference.identity().matches(binding)) {
        return CompletableFuture.failedFuture(new IllegalStateException("result belongs to another invocation"));
      }
      return CompletableFuture.completedFuture(new VerifiedResult(binding, decision, reference));
    });
  }

  private CompletableFuture<ResolvedResult> loadAndMark(InvocationBinding binding,
      CommitDecision decision, ResultReference reference, RequestScope scope, BudgetManager.Account account) {
    if (binding.state() == InvocationBinding.State.SELECTED
        && binding.resultReference().filter(reference.token()::equals).isEmpty()) {
      return CompletableFuture.failedFuture(new IllegalStateException("stored invocation result token differs"));
    }
    return results.load(reference, scope, account).thenCompose(result -> {
      try {
        CompletableFuture<InvocationBinding> marked = binding.state() == InvocationBinding.State.SEALED
            ? invocations.transition(binding, binding.selected(reference.token()), scope)
            : CompletableFuture.completedFuture(binding);
        return marked.thenCompose(ignored -> repository.read(binding.invocationId(), scope))
            .thenApply(current -> {
              InvocationBinding latest = current.orElseThrow(() -> new IllegalStateException("invocation disappeared"));
              if (!latest.sameAttempt(binding) || latest.state() != InvocationBinding.State.SELECTED
                  || latest.resultReference().filter(reference.token()::equals).isEmpty()) {
                throw new IllegalStateException("invocation result window changed during query");
              }
              return new ResolvedResult(decision, result);
            }).whenComplete((ignored, failure) -> {
              if (failure != null) result.close();
            });
      } catch (RuntimeException | Error failure) {
        try { result.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    });
  }

  private record SelectedInvocation(InvocationBinding binding, CommitDecision decision) {}

  private record VerifiedResult(
      InvocationBinding binding, CommitDecision decision, ResultReference reference) {}
}
