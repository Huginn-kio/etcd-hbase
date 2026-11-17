package io.etcdhbase.server.commit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Owns stream-scoped watch IDs, active subscriptions, and compacted-ID retention. */
final class WatchRegistry {
  static final int MAX_WATCHES_PER_STREAM = 1024;

  enum Rejection { NONE, DUPLICATE_ID, STREAM_LIMIT, RETENTION_LIMIT }

  record Registration(long watchId, Watcher watcher, Rejection rejection) {
    Registration {
      Objects.requireNonNull(rejection, "rejection");
      if (rejection != Rejection.NONE && (watchId != 0 || watcher != null)) {
        throw new IllegalArgumentException("rejected Watch registration contains an ID");
      }
    }
  }

  /** State for an active subscription; all subscription access is serialized with close. */
  static final class Watcher implements AutoCloseable {
    private final long id;
    private final long startRevision;
    private final long minimumEventRevision;
    private final boolean previousKv;
    private final boolean filterPut;
    private final boolean filterDelete;
    private final WatchStream.Subscription subscription;
    private WatchStream.Delivery pendingDelivery;
    private volatile boolean createdSent;
    private boolean closed;

    private Watcher(long id, long startRevision, long minimumEventRevision,
        boolean previousKv, boolean filterPut, boolean filterDelete,
        WatchStream.Subscription subscription) {
      this.id = id;
      this.startRevision = startRevision;
      this.minimumEventRevision = minimumEventRevision;
      this.previousKv = previousKv;
      this.filterPut = filterPut;
      this.filterDelete = filterDelete;
      this.subscription = Objects.requireNonNull(subscription, "subscription");
    }

    long id() { return id; }
    long startRevision() { return startRevision; }
    synchronized boolean progressReady(long revision) {
      return !closed && subscription.progressReady(revision);
    }
    long minimumEventRevision() { return minimumEventRevision; }
    boolean previousKv() { return previousKv; }
    boolean filterPut() { return filterPut; }
    boolean filterDelete() { return filterDelete; }

    boolean createdSent() { return createdSent; }

    void markCreatedSent() { createdSent = true; }

    void setWakeup(Runnable wakeup) { subscription.setWakeup(wakeup); }

    synchronized WatchStream.Delivery takeNext() {
      if (closed) return null;
      if (pendingDelivery != null) {
        WatchStream.Delivery delivery = pendingDelivery;
        pendingDelivery = null;
        return delivery;
      }
      WatchStream.Delivery delivery = subscription.poll().orElse(null);
      if (closed && delivery != null) {
        delivery.close();
        return null;
      }
      return delivery;
    }

    synchronized WatchStream.Delivery pollNext() {
      if (closed) return null;
      WatchStream.Delivery delivery = subscription.poll().orElse(null);
      if (closed && delivery != null) {
        delivery.close();
        return null;
      }
      return delivery;
    }

    synchronized void defer(WatchStream.Delivery delivery) {
      if (closed) delivery.close();
      else pendingDelivery = delivery;
    }

    synchronized boolean isClosed() { return closed; }

    synchronized Optional<String> terminationReason() {
      return closed ? Optional.empty() : subscription.terminationReason();
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      WatchStream.Delivery delivery = pendingDelivery;
      pendingDelivery = null;
      Throwable failure = null;
      if (delivery != null) failure = WatchCleanup.closeAndCollect(delivery::close, failure);
      failure = WatchCleanup.closeAndCollect(() -> subscription.setWakeup(null), failure);
      failure = WatchCleanup.closeAndCollect(subscription::close, failure);
      WatchCleanup.throwIfPresent(failure);
    }
  }

  private final LinkedHashMap<Long, Watcher> active = new LinkedHashMap<>();
  private final Set<Long> compactedIds = new HashSet<>();
  private final CompactedWatchIdBudget compactedIdBudget;
  private long nextAutoId;

  WatchRegistry(CompactedWatchIdBudget compactedIdBudget) {
    this.compactedIdBudget = Objects.requireNonNull(compactedIdBudget, "compactedIdBudget");
  }

  synchronized Rejection checkCreate(long requestedId) {
    if (requestedId != 0 && containsIdLocked(requestedId)) return Rejection.DUPLICATE_ID;
    if (watchIdCountLocked() >= MAX_WATCHES_PER_STREAM) return Rejection.STREAM_LIMIT;
    return Rejection.NONE;
  }

  synchronized Registration registerActive(long requestedId, long startRevision, long minimumEventRevision,
      boolean previousKv, boolean filterPut, boolean filterDelete,
      WatchStream.Subscription subscription) {
    Rejection rejection = checkCreate(requestedId);
    if (rejection != Rejection.NONE) return rejected(rejection);
    long id = requestedId == 0 ? allocateWatchIdLocked() : requestedId;
    Watcher watcher = new Watcher(id, startRevision, minimumEventRevision, previousKv,
        filterPut, filterDelete, subscription);
    active.put(id, watcher);
    return new Registration(id, watcher, Rejection.NONE);
  }

  synchronized Registration retainCompacted(long requestedId) {
    Rejection rejection = checkCreate(requestedId);
    if (rejection != Rejection.NONE) return rejected(rejection);
    if (!compactedIdBudget.tryAcquire()) return rejected(Rejection.RETENTION_LIMIT);
    long id;
    try {
      id = requestedId == 0 ? allocateWatchIdLocked() : requestedId;
      compactedIds.add(id);
    } catch (RuntimeException | Error failure) {
      compactedIdBudget.release(1);
      throw failure;
    }
    return new Registration(id, null, Rejection.NONE);
  }

  synchronized Watcher removeActive(long id) { return active.remove(id); }

  synchronized boolean removeCompacted(long id) {
    if (!compactedIds.remove(id)) return false;
    compactedIdBudget.release(1);
    return true;
  }

  synchronized boolean removeActive(long id, Watcher expected) {
    if (active.get(id) != expected) return false;
    active.remove(id);
    return true;
  }

  synchronized List<Watcher> activeSnapshot() { return List.copyOf(active.values()); }

  synchronized int activeCount() { return active.size(); }

  synchronized boolean isEmpty() { return active.isEmpty() && compactedIds.isEmpty(); }

  synchronized List<Watcher> detachAll() {
    List<Watcher> detached = new ArrayList<>(active.values());
    active.clear();
    int compactedCount = compactedIds.size();
    compactedIds.clear();
    compactedIdBudget.release(compactedCount);
    return detached;
  }

  private boolean containsIdLocked(long id) {
    return active.containsKey(id) || compactedIds.contains(id);
  }

  private int watchIdCountLocked() { return active.size() + compactedIds.size(); }

  private long allocateWatchIdLocked() {
    long id = nextAutoId;
    while (containsIdLocked(id)) id = Math.addExact(id, 1);
    nextAutoId = Math.addExact(id, 1);
    return id;
  }

  private static Registration rejected(Rejection rejection) {
    return new Registration(0, null, rejection);
  }
}
