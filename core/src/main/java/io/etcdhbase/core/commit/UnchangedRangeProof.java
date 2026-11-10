package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.ToIntFunction;

/** Bounded coverage proof; a positive result does not replace authorization or pin validation. */
public final class UnchangedRangeProof {
  public enum Verdict { UNCHANGED, CHANGED, INCONCLUSIVE }

  /** Only the bounded metadata needed from the earlier protected view survives cache admission. */
  public record Baseline(ByteKey profileFingerprint, ByteKey authorizationScopeDigest,
      long barrierSequence, long targetRevision, long compactRevision) {
    public Baseline {
      Objects.requireNonNull(profileFingerprint, "profileFingerprint");
      Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
      if (profileFingerprint.length() != 32 || authorizationScopeDigest.length() != 32
          || barrierSequence < 0 || targetRevision <= 0 || compactRevision < 0 || compactRevision > targetRevision) {
        throw new IllegalArgumentException("invalid cached proof baseline");
      }
    }

    public static Baseline capture(ReadView view) {
      Objects.requireNonNull(view, "view");
      return new Baseline(view.profileFingerprint(), view.authorizationScopeDigest(),
          view.barrierSequence(), view.targetRevision(), view.compactRevision());
    }
  }

  private static final ThreadLocal<ArrayDeque<Runnable>> CONTINUATIONS = new ThreadLocal<>();
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("coverage-proof-cleanup").start(task);
  private static final Executor CONTINUE = task -> {
    ArrayDeque<Runnable> queue = CONTINUATIONS.get();
    if (queue != null) { queue.addLast(task); return; }
    queue = new ArrayDeque<>();
    CONTINUATIONS.set(queue);
    try {
      queue.addLast(task);
      Runnable next;
      while ((next = queue.pollFirst()) != null) next.run();
    } finally {
      queue.clear();
      CONTINUATIONS.remove();
    }
  };

  private final CommitDecisionRepository decisions;
  private final ManifestCatalog manifests;
  private final int bucketCount;
  private final int maximumDecisions;
  private final long maximumCandidateBytes;
  private final ToIntFunction<ByteKey> bucket;

  public UnchangedRangeProof(CommitDecisionRepository decisions, ManifestCatalog manifests,
      int bucketCount, int maximumDecisions, long maximumCandidateBytes, ToIntFunction<ByteKey> bucket) {
    this.decisions = Objects.requireNonNull(decisions, "decisions");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    if (bucketCount < 1 || bucketCount > 4096 || maximumDecisions < 1 || maximumDecisions > 1024
        || maximumCandidateBytes < 1 || maximumCandidateBytes > 64L * 1024 * 1024) {
      throw new IllegalArgumentException("invalid coverage proof bounds");
    }
    this.bucketCount = bucketCount;
    this.maximumDecisions = maximumDecisions;
    this.maximumCandidateBytes = maximumCandidateBytes;
  }

  /**
   * Compares barriers from the same table-group owner. The caller must still verify the live
   * identity, hold the current pin through response preparation, and validate it before sending.
   * A missing, inconsistent, or over-budget proof must fall back to an independent direct read.
   * The proof budget and scope must be owned solely by this proof; cancellation closes that
   * scope, and proof read-limit exhaustion must not poison the direct-read account.
   */
  public CompletableFuture<Verdict> prove(long previousGroupId, long currentGroupId,
      Baseline previous, ReadView current,
      ByteKey previousStorageProfile, ByteKey currentStorageProfile, KeyRange range,
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(current, "current");
    Objects.requireNonNull(previousStorageProfile, "previousStorageProfile");
    Objects.requireNonNull(currentStorageProfile, "currentStorageProfile");
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(budget, "budget");
    RequestScope scope = budget.scope();
    if (scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(inactive(scope));
    if (previousGroupId == 0 || previousGroupId != currentGroupId
        || previousStorageProfile.length() != 32 || currentStorageProfile.length() != 32
        || !previousStorageProfile.equals(currentStorageProfile)
        || !previous.profileFingerprint().equals(current.profileFingerprint())
        || !previous.authorizationScopeDigest().equals(current.authorizationScopeDigest())
        || previous.compactRevision() != current.compactRevision()
        || range.kind() == KeyRange.Kind.POINT && range.lowerInclusive().orElseThrow().length() == 0
        || previous.barrierSequence() > current.barrierSequence()) {
      return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
    }
    long distance = current.barrierSequence() - previous.barrierSequence();
    if (distance == 0) {
      return CompletableFuture.completedFuture(previous.targetRevision() == current.targetRevision()
          ? Verdict.UNCHANGED : Verdict.INCONCLUSIVE);
    }
    if (distance > maximumDecisions) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
    var result = proveSequence(previous.barrierSequence() + 1, previous.targetRevision(), current, range, budget);
    var outcome = result.handle((value, problem) -> {
      if (scope.isClosed() || scope.isDeadlineExceeded()) throw new java.util.concurrent.CompletionException(inactive(scope));
      if (problem != null) {
        Throwable cause = io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(problem);
        if (cause instanceof Error fatal) throw fatal;
        return Verdict.INCONCLUSIVE;
      }
      return value;
    });
    var exposed = outcome.copy();
    exposed.whenComplete((ignored, failure) -> {
      if (exposed.isCancelled()) scope.cancelAsync(CLEANUP);
    });
    return exposed;
  }

  private CompletableFuture<Verdict> proveSequence(long sequence, long previousRevision,
      ReadView current, KeyRange range, ManifestCatalog.ProofReadBudget budget) {
    RequestScope scope = budget.scope();
    if (scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(inactive(scope));
    Optional<CommitDecision> selected = current.selectedDecision().filter(decision -> decision.sequence() == sequence);
    CompletableFuture<Optional<CommitDecision>> read = selected.isPresent()
        ? CompletableFuture.completedFuture(selected)
        : budget.read(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE, () -> decisions.read(sequence, scope));
    return read.thenComposeAsync(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
      CommitDecision decision = found.orElseThrow();
      if (decision.sequence() != sequence || decision.baseRevision() != previousRevision
          || !decision.containsKeyChanges()) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
      CompletableFuture<Optional<ManifestRoot>> rootRead = selected.isPresent()
          ? CompletableFuture.completedFuture(current.selectedRoot())
          : budget.read(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE, () -> manifests.readRoot(decision.manifestRoot(), scope));
      return rootRead.thenComposeAsync(rootValue -> {
        if (rootValue.isEmpty()) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
        ManifestRoot root = rootValue.orElseThrow();
        if (!root.transactionId().equals(decision.transactionId())
            || !root.systemIdentity().equals(decision.manifestRoot())
            || !root.digest().equals(decision.manifestDigest())
            || root.baseSequence() != sequence - 1 || root.baseRevision() != previousRevision
            || !root.containsKeyChanges()) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
        return proveBuckets(root, range, firstBucket(range), budget).thenComposeAsync(verdict -> {
          if (verdict != Verdict.UNCHANGED) return CompletableFuture.completedFuture(verdict);
          if (sequence == current.barrierSequence()) {
            return CompletableFuture.completedFuture(decision.revision() == current.targetRevision()
                ? Verdict.UNCHANGED : Verdict.INCONCLUSIVE);
          }
          return proveSequence(sequence + 1, decision.revision(), current, range, budget);
        }, CONTINUE);
      }, CONTINUE);
    }, CONTINUE);
  }

  private int firstBucket(KeyRange range) {
    if (range.kind() != KeyRange.Kind.POINT) return 0;
    int selected = bucket.applyAsInt(range.lowerInclusive().orElseThrow());
    if (selected < 0 || selected >= bucketCount) throw new IllegalArgumentException("invalid range bucket");
    return selected;
  }

  private CompletableFuture<Verdict> proveBuckets(ManifestRoot root, KeyRange range, int index,
      ManifestCatalog.ProofReadBudget budget) {
    RequestScope scope = budget.scope();
    if (scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(inactive(scope));
    if (range.isEmpty() || index >= bucketCount) return CompletableFuture.completedFuture(Verdict.UNCHANGED);
    return manifests.candidatesForProof(root, index, range, maximumCandidateBytes, budget).thenComposeAsync(slice -> {
      boolean changed;
      boolean complete;
      try (slice) {
        changed = !slice.candidates().isEmpty();
        complete = slice.exhausted();
      }
      if (changed) return CompletableFuture.completedFuture(Verdict.CHANGED);
      if (!complete) return CompletableFuture.completedFuture(Verdict.INCONCLUSIVE);
      if (range.kind() == KeyRange.Kind.POINT) return CompletableFuture.completedFuture(Verdict.UNCHANGED);
      return proveBuckets(root, range, index + 1, budget);
    }, CONTINUE);
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
