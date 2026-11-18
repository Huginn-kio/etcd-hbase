package io.etcdhbase.core.commit;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/** Computes safe public KV revision watermarks from a stable snapshot of active watches. */
public final class ProgressFrontier {
  private ProgressFrontier() {}

  /**
   * A gap-free verified prefix of selected commit sequences and its corresponding KV revision.
   * Sequence may advance while revision remains unchanged for metadata-only commits.
   */
  public record VerifiedFrontier(long contiguousSequence, long kvRevision) {
    public VerifiedFrontier {
      if (contiguousSequence < 0 || kvRevision <= 0) {
        throw new IllegalArgumentException("invalid verified Watch progress frontier");
      }
    }
  }

  /** State needed to decide whether one watch may safely advertise progress. */
  public record WatcherState(
      long watchId,
      Optional<VerifiedFrontier> verifiedThrough,
      boolean active,
      boolean replayComplete,
      boolean futureStartReached,
      boolean hasPendingMatchingEventsThroughFrontier) {
    public WatcherState {
      Objects.requireNonNull(verifiedThrough, "verifiedThrough");
    }
  }

  /** Returns the watch's safe KV revision, or empty when its frontier is not ready. */
  public static OptionalLong forWatcher(Collection<WatcherState> watchers, long watchId) {
    for (WatcherState watcher : snapshot(watchers)) {
      if (watcher.watchId() == watchId) return safeKvRevision(watcher);
    }
    return OptionalLong.empty();
  }

  /**
   * Returns a common KV revision only when every active watch can safely report through it.
   * Closed watches are ignored; an empty active set does not produce a vacuous progress response.
   */
  public static OptionalLong forAllActive(Collection<WatcherState> watchers) {
    long minimumKvRevision = Long.MAX_VALUE;
    boolean hasActiveWatch = false;
    for (WatcherState watcher : snapshot(watchers)) {
      if (!watcher.active()) continue;
      hasActiveWatch = true;
      OptionalLong safeKvRevision = safeKvRevision(watcher);
      if (safeKvRevision.isEmpty()) return OptionalLong.empty();
      minimumKvRevision = Math.min(minimumKvRevision, safeKvRevision.getAsLong());
    }
    return hasActiveWatch ? OptionalLong.of(minimumKvRevision) : OptionalLong.empty();
  }

  private static OptionalLong safeKvRevision(WatcherState watcher) {
    if (!watcher.active()
        || !watcher.replayComplete()
        || !watcher.futureStartReached()
        || watcher.hasPendingMatchingEventsThroughFrontier()
        || watcher.verifiedThrough().isEmpty()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(watcher.verifiedThrough().orElseThrow().kvRevision());
  }

  private static List<WatcherState> snapshot(Collection<WatcherState> watchers) {
    List<WatcherState> snapshot = List.copyOf(Objects.requireNonNull(watchers, "watchers"));
    Set<Long> watchIds = new HashSet<>();
    for (WatcherState watcher : snapshot) {
      if (!watchIds.add(watcher.watchId())) {
        throw new IllegalArgumentException("duplicate watch ID in progress snapshot");
      }
    }
    return snapshot;
  }
}
