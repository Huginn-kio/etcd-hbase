package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Bounded merge of the first revision from each bucket; it does not prove selection or coverage. */
public final class EventRevisionIndexSeeker {
  private static final int MAX_BUCKETS_PER_STEP = 256;
  private static final long MAX_PAGE_BYTES = 8L << 20;
  private final EventRevisionIndexRepository indexes;
  private final int bucketCount;

  public EventRevisionIndexSeeker(EventRevisionIndexRepository indexes, int bucketCount) {
    this.indexes = Objects.requireNonNull(indexes, "indexes");
    if (bucketCount < 1 || bucketCount > 65_536) {
      throw new IllegalArgumentException("invalid event-index bucket count");
    }
    this.bucketCount = bucketCount;
  }

  public Cursor start(long minimumRevision, RequestScope scope) {
    return new Cursor(this, Objects.requireNonNull(scope, "scope"), minimumRevision, 0, OptionalLong.empty());
  }

  /**
   * Reads a bounded number of buckets. The caller must hold event-history protection for the
   * whole seek and verify the result against a captured control barrier and the sealed root.
   */
  public CompletableFuture<Cursor> step(Cursor cursor, int maximumBuckets, long maximumPageBytes,
      RequestScope scope, BudgetManager.Account account, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(cursor, "cursor");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(budget, "budget");
    if (cursor.owner != this || cursor.scope != scope || cursor.nextBucket() > bucketCount || maximumBuckets < 1
        || maximumBuckets > MAX_BUCKETS_PER_STEP || maximumPageBytes < 1
        || maximumPageBytes > MAX_PAGE_BYTES || !account.belongsTo(scope) || !budget.belongsTo(scope)
        || !budget.usesAccount(account)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event index seek budget"));
    }
    int stop = Math.min(bucketCount, cursor.nextBucket() + maximumBuckets);
    CompletableFuture<Cursor> next = CompletableFuture.completedFuture(cursor);
    for (int bucket = cursor.nextBucket(); bucket < stop; bucket++) {
      final int currentBucket = bucket;
      next = next.thenCompose(current -> budget.scanMeasured(maximumPageBytes,
          EventRevisionIndexPage::encodedBytes,
          () -> indexes.firstAtOrAfter(currentBucket, current.minimumRevision(), maximumPageBytes, scope))
          .thenApply(page -> advance(current, page, account)));
    }
    return next;
  }

  private static Cursor advance(Cursor cursor, EventRevisionIndexPage page, BudgetManager.Account account) {
    if (page.physicalRows() > 2) throw new IllegalStateException("event index seek exceeded its row budget");
    try {
      account.chargeInternalScanPage(page.physicalRows(), page.encodedBytes(), page.scanRpcCalls());
    } catch (BackendFailure failure) {
      throw new CompletionException(failure);
    }
    OptionalLong candidate = cursor.candidateRevision();
    if (page.index().isPresent()) {
      long revision = page.index().orElseThrow().decision().revision();
      if (revision < cursor.minimumRevision()) {
        throw new IllegalStateException("event index seek returned an earlier revision");
      }
      if (candidate.isEmpty() || revision < candidate.getAsLong()) candidate = OptionalLong.of(revision);
    }
    return new Cursor(cursor.owner, cursor.scope, cursor.minimumRevision(), cursor.nextBucket() + 1, candidate);
  }

  public boolean complete(Cursor cursor) {
    Objects.requireNonNull(cursor, "cursor");
    if (cursor.owner != this || cursor.nextBucket() > bucketCount) {
      throw new IllegalArgumentException("cursor does not belong to this seeker");
    }
    return cursor.nextBucket() == bucketCount;
  }

  /** Only a completed cursor supplies a candidate; an empty result is not proof of missing history. */
  public OptionalLong candidate(Cursor cursor) {
    if (!complete(cursor)) throw new IllegalStateException("event index seek is incomplete");
    return cursor.candidateRevision();
  }

  public static final class Cursor {
    private final EventRevisionIndexSeeker owner;
    private final RequestScope scope;
    private final long minimumRevision;
    private final int nextBucket;
    private final OptionalLong candidateRevision;

    private Cursor(EventRevisionIndexSeeker owner, RequestScope scope, long minimumRevision,
        int nextBucket, OptionalLong candidateRevision) {
      if (minimumRevision <= 0 || nextBucket < 0
          || candidateRevision.isPresent() && candidateRevision.getAsLong() < minimumRevision) {
        throw new IllegalArgumentException("invalid event index seek cursor");
      }
      this.owner = owner;
      this.scope = scope;
      this.minimumRevision = minimumRevision;
      this.nextBucket = nextBucket;
      this.candidateRevision = candidateRevision;
    }

    public long minimumRevision() { return minimumRevision; }
    public int nextBucket() { return nextBucket; }
    private OptionalLong candidateRevision() { return candidateRevision; }
  }
}
