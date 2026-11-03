package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Verifies every sealed candidate and event reference before the publication CAS. */
public final class CommitPlanVerifier {
  private final ManifestCatalog catalog;

  public CommitPlanVerifier(ManifestCatalog catalog) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
  }

  public CompletableFuture<ManifestRoot> verify(
      InvocationBinding invocation, ManifestRoot proposed, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(proposed, "proposed");
    if (invocation.state() != InvocationBinding.State.SEALED
        || invocation.manifestRoot().filter(proposed.systemIdentity()::equals).isEmpty()
        || !invocation.transactionId().equals(proposed.transactionId())
        || invocation.preparedOwnerEpoch() != proposed.ownerEpoch()
        || invocation.baseSequence() != proposed.baseSequence()
        || invocation.baseRevision() != proposed.baseRevision()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "commit preflight binding differs from the sealed invocation"));
    }
    return catalog.readRoot(proposed.systemIdentity(), scope).thenCompose(stored -> {
      ManifestRoot root = stored.orElseThrow(() ->
          new IllegalStateException("sealed manifest root is missing"));
      if (!root.digest().equals(proposed.digest())) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "durable manifest root differs from the proposed root"));
      }
      return catalog.forEachCandidate(root, scope, ignoredCandidate ->
              CompletableFuture.completedFuture(null))
          .thenCompose(candidates -> catalog.forEachEvent(root, scope,
              event -> CompletableFuture.completedFuture(null)))
          .thenApply(ignored -> root);
    });
  }
}
