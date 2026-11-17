package io.etcdhbase.server.commit;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import mvccpb.Kv;

/** One stream-scoped watch registry and single writer for control responses and event batches. */
final class WatchStream implements StreamObserver<Rpc.WatchRequest>, AutoCloseable {
  private static final long EVENT_POLL_DELAY_MILLIS = 25;
  private static final int MAX_FILTERED_REVISIONS_PER_DRAIN = 128;
  private static final long REJECTED_CREATE_RESPONSE_WATCH_ID = -1;
  private static final String HISTORICAL_REPLAY_UNSUPPORTED_REASON =
      "etcd-hbase: historical Watch replay is not implemented";
  private static final int MAX_WATCHES_PER_STREAM = 1024;
  static final int MAX_PENDING_EVENTS_PER_WATCH = 4096;

  interface SessionProvider {
    CompletionStage<Session> open();
  }

  interface Session {
    long completedRevision();
    CompletionStage<Long> currentRevision();
    CompletionStage<OpenedWatch> subscribe(KeyRange range, Rpc.WatchCreateRequest request)
        throws BackendFailure;
  }

  record OpenedWatch(long revision, long compactRevision, Subscription subscription) {
    OpenedWatch(long revision, Subscription subscription) { this(revision, -1, subscription); }

    OpenedWatch {
      if (revision < 0 || compactRevision < -1) {
        throw new IllegalArgumentException("invalid Watch revision checkpoint");
      }
      Objects.requireNonNull(subscription, "subscription");
    }
  }

  interface Subscription extends AutoCloseable {
    Optional<Delivery> poll();
    Optional<String> terminationReason();
    void closeLiveDelivery();
    @Override void close();
  }

  static final class Delivery implements AutoCloseable {
    private final long revision;
    private final PreparedEvent event;
    private final Runnable release;
    private final AtomicBoolean closed = new AtomicBoolean();

    Delivery(long revision, PreparedEvent event, Runnable release) {
      if (revision <= 0) throw new IllegalArgumentException("watch event revision must be positive");
      this.revision = revision;
      this.event = Objects.requireNonNull(event, "event");
      this.release = Objects.requireNonNull(release, "release");
    }

    long revision() { return revision; }
    PreparedEvent event() { return event; }

    @Override public void close() {
      if (closed.compareAndSet(false, true)) release.run();
    }
  }

  private record PendingResponse(Rpc.WatchResponse response, Runnable afterSend) {
    private PendingResponse {
      Objects.requireNonNull(response, "response");
      Objects.requireNonNull(afterSend, "afterSend");
    }
  }

  private record EventOutput(Rpc.WatchResponse response, List<Delivery> deliveries) {
    private EventOutput {
      Objects.requireNonNull(response, "response");
      deliveries = List.copyOf(deliveries);
    }

    private void release() { deliveries.forEach(Delivery::close); }
  }

  private record EventPoll(EventOutput output, boolean consumed) {}

  private enum PumpResult { RESPONSE_SENT, ADVANCED, IDLE }

  private final Object lock = new Object();
  private final StreamObserver<Rpc.WatchResponse> responseObserver;
  private final ServerCallStreamObserver<Rpc.WatchResponse> serverObserver;
  private final RequestScope streamScope;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final SessionProvider sessions;
  private final long clusterId;
  private final long memberId;
  private final int maximumResponseBytes;
  private final LinkedHashMap<Long, Watcher> watchers = new LinkedHashMap<>();
  private final ArrayDeque<PendingResponse> responses = new ArrayDeque<>();
  private final AtomicBoolean draining = new AtomicBoolean();
  private final AtomicBoolean drainRequested = new AtomicBoolean();

  private CompletionStage<Session> sessionFuture;
  private Session session;
  private Rpc.WatchRequest pendingRequest;
  private ScheduledFuture<?> pollTask;
  private int nextWatchIndex;
  private long nextAutoId;
  private boolean processingRequest;
  private boolean requestOutstanding;
  private boolean inboundClosed;
  private boolean terminal;
  private boolean peerCancelled;
  private Throwable terminalFailure;

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes) {
    this.responseObserver = Objects.requireNonNull(responseObserver, "responseObserver");
    this.streamScope = Objects.requireNonNull(streamScope, "streamScope");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    this.sessions = Objects.requireNonNull(sessions, "sessions");
    if (clusterId == 0 || memberId == 0 || maximumResponseBytes < 1) {
      throw new IllegalArgumentException("invalid Watch stream identity or response budget");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumResponseBytes = maximumResponseBytes;
    if (responseObserver instanceof ServerCallStreamObserver<?> serverCall) {
      @SuppressWarnings("unchecked")
      ServerCallStreamObserver<Rpc.WatchResponse> typed =
          (ServerCallStreamObserver<Rpc.WatchResponse>) serverCall;
      serverObserver = typed;
      serverObserver.disableAutoInboundFlowControl();
      serverObserver.setOnReadyHandler(this::scheduleDrain);
      serverObserver.setOnCancelHandler(this::cancelFromPeer);
    } else {
      serverObserver = null;
    }
    streamScope.registerCleanup(this);
    requestNext();
  }

  @Override
  public void onNext(Rpc.WatchRequest request) {
    Objects.requireNonNull(request, "request");
    boolean accepted;
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      accepted = requestOutstanding && pendingRequest == null;
      if (accepted) {
        requestOutstanding = false;
        pendingRequest = request;
      }
    }
    if (!accepted) {
      fail(new RejectedExecutionException("Watch stream exceeded its inbound request window"));
      return;
    }
    scheduleDrain();
  }

  @Override
  public void onError(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    cancelFromPeer();
  }

  @Override
  public void onCompleted() {
    // J47 must compare request half-close behavior with etcd before this lifecycle is treated as
    // contract.
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      inboundClosed = true;
      requestOutstanding = false;
    }
    scheduleDrain();
  }

  private void scheduleDrain() {
    drainRequested.set(true);
    if (!draining.compareAndSet(false, true)) return;
    try {
      worker.execute(() -> {
        try {
          drainRequested.getAndSet(false);
          drainLoop();
        } catch (RuntimeException | Error failure) {
          fail(failure);
          if (failure instanceof Error fatal) throw fatal;
        } finally {
          draining.set(false);
          if (drainRequested.get() || hasRunnableWork()) scheduleDrain();
        }
      });
    } catch (RejectedExecutionException rejected) {
      drainAfterRejectedExecution(rejected);
    } catch (Error fatal) {
      try {
        drainRequested.set(false);
        fail(fatal, false);
        drainLoop();
      } finally {
        draining.set(false);
        drainRequested.set(false);
      }
      throw fatal;
    }
  }

  /** The executor rejected the only writer; this caller owns the drain until it returns. */
  private void drainAfterRejectedExecution(RejectedExecutionException rejected) {
    Throwable cleanupFailure = null;
    try {
      drainRequested.set(false);
      fail(rejected, false);
    } catch (RuntimeException | Error failure) {
      cleanupFailure = failure;
    }
    try {
      drainLoop();
    } catch (RuntimeException | Error failure) {
      if (cleanupFailure == null) cleanupFailure = failure;
      else if (failure != cleanupFailure) cleanupFailure.addSuppressed(failure);
    } finally {
      draining.set(false);
      drainRequested.set(false);
    }
    if (cleanupFailure instanceof Error fatal) throw fatal;
    if (cleanupFailure instanceof RuntimeException failure) throw failure;
  }

  private void drainLoop() {
    int filteredRevisions = 0;
    for (;;) {
      PendingResponse response;
      Rpc.WatchRequest request;
      boolean complete;
      Throwable failureToSend;
      synchronized (lock) {
        if (peerCancelled) return;
        if (terminalFailure != null) {
          failureToSend = terminalFailure;
          terminalFailure = null;
          response = null;
          request = null;
          complete = false;
        } else if (terminal) {
          return;
        } else if (!responses.isEmpty()) {
          failureToSend = null;
          if (!isReady()) return;
          response = responses.removeFirst();
          request = null;
          complete = false;
        } else if (pendingRequest != null && !processingRequest) {
          failureToSend = null;
          request = pendingRequest;
          pendingRequest = null;
          processingRequest = true;
          response = null;
          complete = false;
        } else if (inboundClosed && pendingRequest == null && !processingRequest) {
          failureToSend = null;
          response = null;
          request = null;
          complete = true;
        } else if (!isReady()) {
          return;
        } else {
          failureToSend = null;
          response = null;
          request = null;
          complete = false;
        }
      }

      if (failureToSend != null) {
        Throwable cause = unwrap(failureToSend);
        try {
          responseObserver.onError(cause instanceof StatusRuntimeException statusFailure
              ? statusFailure : ErrorProjection.project(cause));
        } finally {
          streamScope.cancel();
        }
        return;
      }

      if (complete) {
        try {
          closeWatchers();
        } catch (RuntimeException | Error failure) {
          fail(failure);
          if (failure instanceof Error fatal) throw fatal;
          return;
        }
        synchronized (lock) {
          if (terminal || peerCancelled) return;
          terminal = true;
        }
        cancelPoll();
        responseObserver.onCompleted();
        return;
      }
      if (response != null) {
        try {
          responseObserver.onNext(response.response());
          response.afterSend().run();
        } catch (RuntimeException | Error failure) {
          fail(failure);
          if (failure instanceof Error fatal) throw fatal;
          return;
        }
        continue;
      }
      if (request != null) {
        processRequest(request);
        continue;
      }

      PumpResult pumpResult = pumpOneWatcher();
      if (pumpResult == PumpResult.RESPONSE_SENT) {
        filteredRevisions = 0;
        requestNext();
        continue;
      }
      if (pumpResult == PumpResult.ADVANCED) {
        requestNext();
        if (++filteredRevisions >= MAX_FILTERED_REVISIONS_PER_DRAIN) {
          schedulePoll(0);
          return;
        }
        continue;
      }
      requestNext();
      if (hasCreatedWatcher()) schedulePoll(EVENT_POLL_DELAY_MILLIS);
      return;
    }
  }

  private void processRequest(Rpc.WatchRequest request) {
    switch (request.getRequestUnionCase()) {
      case CREATE_REQUEST -> processCreate(request.getCreateRequest());
      case CANCEL_REQUEST -> processCancel(request.getCancelRequest());
      case PROGRESS_REQUEST, REQUESTUNION_NOT_SET -> finishRequestWithoutResponse();
    }
  }

  private void processCreate(Rpc.WatchCreateRequest request) {
    if (request == null) {
      finishRequestWithoutResponse();
      return;
    }
    CompletionStage<Session> opening = session();
    opening.whenComplete((openedSession, failure) -> executeCompletion(() -> {
      if (failure != null) {
        fail(failure);
        return;
      }
      completeCreate(openedSession, request);
    }));
  }

  private CompletionStage<Session> session() {
    synchronized (lock) {
      if (session != null) return java.util.concurrent.CompletableFuture.completedFuture(session);
      if (sessionFuture != null) return sessionFuture;
      try {
        sessionFuture = Objects.requireNonNull(sessions.open(), "Watch session future");
      } catch (RuntimeException | Error failure) {
        sessionFuture = java.util.concurrent.CompletableFuture.failedFuture(failure);
      }
      return sessionFuture;
    }
  }

  private void completeCreate(Session openedSession, Rpc.WatchCreateRequest request) {
    Objects.requireNonNull(openedSession, "openedSession");
    try {
      synchronized (lock) { session = openedSession; }
      KeyRange range;
      try {
        range = keyRange(request);
      } catch (InvalidRange invalidRange) {
        rejectCreateAtCurrentRevision(openedSession, invalidRange.getMessage());
        return;
      }
      boolean stopped;
      boolean duplicate;
      boolean atLimit;
      synchronized (lock) {
        stopped = terminal || peerCancelled || inboundClosed;
        duplicate = !stopped && request.getWatchId() != 0
            && watchers.containsKey(request.getWatchId());
        atLimit = !stopped && !duplicate && watchers.size() >= MAX_WATCHES_PER_STREAM;
        if (stopped) processingRequest = false;
      }
      if (stopped) {
        scheduleDrain();
        return;
      }
      if (duplicate) {
        rejectCreateAtCurrentRevision(openedSession,
            "mvcc: duplicate watch ID provided on the WatchStream");
        return;
      }
      if (atLimit) {
        rejectCreateAtCurrentRevision(openedSession,
            "etcd-hbase: Watch stream subscription limit exceeded");
        return;
      }

      CompletionStage<OpenedWatch> opening;
      try {
        opening = Objects.requireNonNull(
            openedSession.subscribe(range, request), "opened Watch subscription");
      } catch (BackendFailure failure) {
        if (failure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
          rejectCreateAtCurrentRevision(openedSession, "etcd-hbase: Watch resource limit exceeded");
        } else {
          fail(failure);
        }
        return;
      }
      opening.whenComplete((opened, failure) -> executeCompletion(() ->
          completeSubscribedCreate(openedSession, request, opened, failure)));
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void completeSubscribedCreate(Session openedSession, Rpc.WatchCreateRequest request,
      OpenedWatch opened, Throwable subscriptionFailure) {
    if (subscriptionFailure != null) {
      Throwable cause = unwrap(subscriptionFailure);
      if (cause instanceof BackendFailure backendFailure
          && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
        rejectCreateAtCurrentRevision(openedSession, "etcd-hbase: Watch resource limit exceeded");
      } else {
        fail(cause);
      }
      return;
    }
    try {
      Objects.requireNonNull(opened, "opened Watch subscription");
      if (request.getStartRevision() != 0
          && request.getStartRevision() < opened.compactRevision()) {
        completeCompactedCreate(opened, request);
        return;
      }

      if (request.getStartRevision() > 0 && request.getStartRevision() <= opened.revision()) {
        opened.subscription().close();
        rejectCreate(opened.revision(), HISTORICAL_REPLAY_UNSUPPORTED_REASON);
        return;
      }

      Watcher watcher = null;
      boolean stopped;
      boolean duplicate;
      boolean atLimit;
      synchronized (lock) {
        stopped = terminal || peerCancelled || inboundClosed;
        duplicate = !stopped && request.getWatchId() != 0
            && watchers.containsKey(request.getWatchId());
        atLimit = !stopped && !duplicate && watchers.size() >= MAX_WATCHES_PER_STREAM;
        if (!stopped && !duplicate && !atLimit) {
          long id = request.getWatchId() == 0 ? allocateWatchIdLocked() : request.getWatchId();
          long minimumEventRevision = request.getStartRevision() <= 0
              ? nextRevision(opened.revision()) : request.getStartRevision();
          Watcher createdWatcher = new Watcher(id, minimumEventRevision,
              request.getPrevKv(),
              request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NOPUT),
              request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NODELETE),
              opened.subscription(), false);
          watchers.put(id, createdWatcher);
          watcher = createdWatcher;
          enqueueResponse(createdResponse(id, opened.revision()), () -> {
            createdWatcher.createdSent = true;
            requestNext();
          });
        }
        processingRequest = false;
      }
      if (watcher == null) {
        opened.subscription().close();
        if (stopped) {
          scheduleDrain();
        } else if (duplicate) {
          rejectCreate(opened.revision(), "mvcc: duplicate watch ID provided on the WatchStream");
        } else if (atLimit) {
          rejectCreate(opened.revision(), "etcd-hbase: Watch stream subscription limit exceeded");
        }
        return;
      }
      scheduleDrain();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void rejectCreateAtCurrentRevision(Session openedSession, String reason) {
    CompletionStage<Long> currentRevision;
    try {
      currentRevision = Objects.requireNonNull(
          openedSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    currentRevision.whenComplete((revision, failure) -> executeCompletion(() -> {
      if (failure != null) {
        fail(unwrap(failure));
      } else if (revision == null || revision < 0) {
        fail(new IllegalStateException("Watch session revision is invalid"));
      } else {
        rejectCreate(revision, reason);
      }
    }));
  }

  private void completeCompactedCreate(OpenedWatch opened, Rpc.WatchCreateRequest request) {
    try {
      opened.subscription().closeLiveDelivery();
    } catch (RuntimeException | Error failure) {
      try {
        opened.subscription().close();
      } catch (RuntimeException | Error cleanupFailure) {
        if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
      }
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }

    Watcher watcher = null;
    boolean stopped;
    boolean duplicate;
    boolean atLimit;
    synchronized (lock) {
      stopped = terminal || peerCancelled || inboundClosed;
      duplicate = !stopped && request.getWatchId() != 0
          && watchers.containsKey(request.getWatchId());
      atLimit = !stopped && !duplicate && watchers.size() >= MAX_WATCHES_PER_STREAM;
      if (!stopped && !duplicate && !atLimit) {
        long id = request.getWatchId() == 0 ? allocateWatchIdLocked() : request.getWatchId();
        Watcher compacted = new Watcher(id, request.getStartRevision(), request.getPrevKv(),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NOPUT),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NODELETE),
            opened.subscription(), true);
        watchers.put(id, compacted);
        watcher = compacted;
        enqueueResponse(createdResponse(id, opened.revision()), () -> compacted.createdSent = true);
        enqueueResponse(compactedResponse(id, 0, opened.compactRevision()), this::requestNext);
      }
      processingRequest = false;
    }
    if (watcher == null) {
      try {
        opened.subscription().close();
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      if (stopped) {
        scheduleDrain();
      } else if (duplicate) {
        rejectCreate(opened.revision(), "mvcc: duplicate watch ID provided on the WatchStream");
      } else if (atLimit) {
        rejectCreate(opened.revision(), "etcd-hbase: Watch stream subscription limit exceeded");
      }
      return;
    }
    scheduleDrain();
  }

  private void processCancel(Rpc.WatchCancelRequest request) {
    if (request == null) {
      finishRequestWithoutResponse();
      return;
    }
    Watcher watcher;
    Session currentSession;
    synchronized (lock) {
      watcher = watchers.remove(request.getWatchId());
      currentSession = session;
      if (watcher == null) processingRequest = false;
    }
    if (watcher == null) {
      requestNext();
      return;
    }
    try {
      watcher.close();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    CompletionStage<Long> revision;
    try {
      revision = currentSession == null
          ? java.util.concurrent.CompletableFuture.completedFuture(0L)
          : Objects.requireNonNull(currentSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      synchronized (lock) { processingRequest = false; }
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    revision.whenComplete((currentRevision, failure) -> executeCompletion(() ->
        completeCancel(watcher, currentRevision, failure)));
  }

  private void completeCancel(Watcher watcher, Long revision, Throwable failure) {
    if (failure != null) {
      synchronized (lock) { processingRequest = false; }
      fail(unwrap(failure));
      return;
    }
    if (revision == null || revision < 0) {
      synchronized (lock) { processingRequest = false; }
      fail(new IllegalStateException("Watch session revision is invalid"));
      return;
    }
    synchronized (lock) {
      processingRequest = false;
      if (terminal || peerCancelled) return;
      enqueueResponse(cancelledResponse(watcher.id, revision), this::requestNext);
    }
    scheduleDrain();
    if (!hasCreatedWatcher()) cancelPoll();
  }

  private void rejectCreate(long revision, String reason) {
    enqueueResponse(Rpc.WatchResponse.newBuilder()
        .setHeader(header(revision))
        .setWatchId(REJECTED_CREATE_RESPONSE_WATCH_ID)
        .setCreated(true)
        .setCanceled(true)
        .setCancelReason(reason)
        .build(), this::requestNext);
    synchronized (lock) { processingRequest = false; }
    scheduleDrain();
  }

  private void finishRequestWithoutResponse() {
    synchronized (lock) { processingRequest = false; }
    requestNext();
    scheduleDrain();
  }

  private void enqueueResponse(Rpc.WatchResponse response, Runnable afterSend) {
    synchronized (lock) { responses.addLast(new PendingResponse(response, afterSend)); }
  }

  private PumpResult pumpOneWatcher() {
    List<Watcher> snapshot;
    int first;
    synchronized (lock) {
      snapshot = List.copyOf(watchers.values());
      if (snapshot.isEmpty()) return PumpResult.IDLE;
      first = Math.floorMod(nextWatchIndex, snapshot.size());
    }
    for (int offset = 0; offset < snapshot.size(); offset++) {
      int index = (first + offset) % snapshot.size();
      Watcher watcher = snapshot.get(index);
      if (!watcher.createdSent || watcher.compacted) continue;
      EventPoll poll = nextEventResponse(watcher);
      EventOutput output = poll.output();
      if (output != null) {
        advanceWatchIndex(index);
        try {
          if (watcher.isClosed() || terminalOrPeerCancelled()) return PumpResult.IDLE;
          responseObserver.onNext(output.response());
        } catch (RuntimeException | Error failure) {
          fail(failure);
          if (failure instanceof Error fatal) throw fatal;
          return PumpResult.IDLE;
        } finally {
          output.release();
        }
        return PumpResult.RESPONSE_SENT;
      }
      if (poll.consumed()) {
        advanceWatchIndex(index);
        return PumpResult.ADVANCED;
      }
    }
    return PumpResult.IDLE;
  }

  private void advanceWatchIndex(int index) {
    synchronized (lock) { nextWatchIndex = (index + 1) % Math.max(1, watchers.size()); }
  }

  private EventPoll nextEventResponse(Watcher watcher) {
    synchronized (watcher) {
      return nextEventResponseLocked(watcher);
    }
  }

  private EventPoll nextEventResponseLocked(Watcher watcher) {
    List<Delivery> deliveries = new ArrayList<>();
    boolean transferred = false;
    try {
      Delivery first = watcher.takeNext();
      if (first == null) {
        Optional<String> termination = watcher.subscription.terminationReason();
        termination.ifPresent(reason -> terminateWatcher(watcher, reason));
        return new EventPoll(null, termination.isPresent());
      }
      long revision = first.revision();
      deliveries.add(first);
      while (deliveries.size() < MAX_PENDING_EVENTS_PER_WATCH) {
        Delivery next = watcher.pollNext();
        if (next == null) break;
        if (next.revision() != revision) {
          watcher.defer(next);
          break;
        }
        deliveries.add(next);
      }

      var response = Rpc.WatchResponse.newBuilder()
          .setHeader(header(revision))
          .setWatchId(watcher.id);
      for (Delivery delivery : deliveries) {
        PreparedEvent event = delivery.event();
        if (delivery.revision() < watcher.minimumEventRevision
            || event.kind() == PreparedEvent.Kind.PUT && watcher.filterPut
            || event.kind() == PreparedEvent.Kind.DELETE && watcher.filterDelete) continue;
        if (event.current().metadata().modificationRevision() != revision) {
          throw new IllegalStateException("Watch event revision differs from its selected revision");
        }
        response.addEvents(project(event, watcher.previousKv));
      }
      if (response.getEventsCount() == 0) return new EventPoll(null, true);
      Rpc.WatchResponse built = response.build();
      if (built.getSerializedSize() > maximumResponseBytes) {
        terminateWatcher(watcher, "etcd-hbase: Watch response exceeds the configured message budget");
        return new EventPoll(null, true);
      }
      EventOutput output = new EventOutput(built, deliveries);
      transferred = true;
      return new EventPoll(output, true);
    } finally {
      if (!transferred) deliveries.forEach(Delivery::close);
    }
  }

  private void terminateWatcher(Watcher watcher, String reason) {
    synchronized (lock) {
      if (watchers.remove(watcher.id) != watcher) return;
      watcher.close();
      enqueueResponse(cancelledResponse(
          watcher.id, session == null ? 0 : session.completedRevision(), reason),
          this::requestNext);
    }
  }

  private void requestNext() {
    if (serverObserver == null) {
      synchronized (lock) {
        if (!terminal && !peerCancelled && !inboundClosed && !processingRequest
            && pendingRequest == null && responses.isEmpty()) requestOutstanding = true;
      }
      return;
    }
    boolean request;
    synchronized (lock) {
      request = !terminal && !peerCancelled && !inboundClosed && !processingRequest
          && pendingRequest == null && responses.isEmpty() && !requestOutstanding;
      if (request) requestOutstanding = true;
    }
    if (!request) return;
    try {
      serverObserver.request(1);
    } catch (RuntimeException failure) {
      fail(failure);
    }
  }

  private void schedulePoll(long delayMillis) {
    synchronized (lock) {
      if (terminal || peerCancelled || pollTask != null || !hasCreatedWatcherLocked()) return;
      try {
        pollTask = timers.schedule(() -> {
          synchronized (lock) { pollTask = null; }
          scheduleDrain();
        }, delayMillis, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException rejected) {
        // The stream cannot make progress without its scheduler, so fail and release all subscriptions.
        fail(rejected);
      }
    }
  }

  private void cancelPoll() {
    ScheduledFuture<?> scheduled;
    synchronized (lock) {
      scheduled = pollTask;
      pollTask = null;
    }
    if (scheduled != null) scheduled.cancel(false);
  }

  private boolean isReady() {
    return serverObserver == null || serverObserver.isReady();
  }

  private boolean hasCreatedWatcher() {
    synchronized (lock) { return hasCreatedWatcherLocked(); }
  }

  private boolean hasCreatedWatcherLocked() {
    return watchers.values().stream().anyMatch(watcher -> watcher.createdSent && !watcher.compacted);
  }

  private boolean hasRunnableWork() {
    synchronized (lock) {
      return !peerCancelled && (terminalFailure != null || !terminal && (pendingRequest != null
          || !responses.isEmpty() && isReady()
          || inboundClosed && pendingRequest == null && !processingRequest && responses.isEmpty()));
    }
  }

  private void executeCompletion(Runnable completion) {
    try {
      worker.execute(completion);
    } catch (RejectedExecutionException rejected) {
      fail(rejected);
    } catch (Error fatal) {
      fail(fatal);
      throw fatal;
    }
  }

  private long allocateWatchIdLocked() {
    long id = nextAutoId;
    while (watchers.containsKey(id)) id = Math.addExact(id, 1);
    nextAutoId = Math.addExact(id, 1);
    return id;
  }

  private static long nextRevision(long revision) {
    return revision == Long.MAX_VALUE ? Long.MAX_VALUE : revision + 1;
  }

  private static KeyRange keyRange(Rpc.WatchCreateRequest request) {
    byte[] keyBytes = request.getKey().toByteArray();
    if (keyBytes.length == 0) keyBytes = new byte[] {0};
    ByteKey key = ByteKey.copyOf(keyBytes);
    byte[] endBytes = request.getRangeEnd().toByteArray();
    if (endBytes.length == 0) return KeyRange.point(key);
    if (endBytes.length == 1 && endBytes[0] == 0) {
      return KeyRange.interval(Optional.of(key), Optional.empty());
    }
    ByteKey end = ByteKey.copyOf(endBytes);
    if (key.compareTo(end) >= 0) throw new InvalidRange();
    return KeyRange.between(key, end);
  }

  private Rpc.WatchResponse createdResponse(long id, long revision) {
    return Rpc.WatchResponse.newBuilder().setHeader(header(revision)).setWatchId(id)
        .setCreated(true).build();
  }

  private Rpc.WatchResponse cancelledResponse(long id, long revision) {
    return cancelledResponse(id, revision, "");
  }

  private Rpc.WatchResponse cancelledResponse(long id, long revision, String reason) {
    var response = Rpc.WatchResponse.newBuilder().setHeader(header(revision))
        .setWatchId(id).setCanceled(true);
    if (!reason.isEmpty()) response.setCancelReason(reason);
    return response.build();
  }

  private Rpc.WatchResponse compactedResponse(long id, long revision, long compactRevision) {
    return Rpc.WatchResponse.newBuilder().setHeader(header(revision))
        .setWatchId(id).setCanceled(true).setCompactRevision(compactRevision)
        .build();
  }

  private Rpc.ResponseHeader header(long revision) {
    return ResponseHeaderFactory.create(clusterId, memberId, revision, 0);
  }

  private static Kv.Event project(PreparedEvent event, boolean previousKv) {
    var builder = Kv.Event.newBuilder()
        .setType(event.kind() == PreparedEvent.Kind.PUT ? Kv.Event.EventType.PUT : Kv.Event.EventType.DELETE)
        .setKv(project(event.current()));
    if (previousKv) event.previous().ifPresent(previous -> builder.setPrevKv(project(previous)));
    return builder.build();
  }

  private static Kv.KeyValue project(StoredRecord record) {
    var metadata = record.metadata();
    var builder = Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(record.key().bytes()))
        .setCreateRevision(metadata.createRevision()).setModRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLease(metadata.leaseId());
    record.value().ifPresent(value -> builder.setValue(ByteString.copyFrom(value)));
    return builder.build();
  }

  private void cancelFromPeer() {
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      peerCancelled = true;
      requestOutstanding = false;
      pendingRequest = null;
      responses.clear();
    }
    streamScope.cancel();
    close();
  }

  private void fail(Throwable failure) {
    fail(failure, true);
  }

  private void fail(Throwable failure, boolean scheduleWriter) {
    Objects.requireNonNull(failure, "failure");
    synchronized (lock) {
      if (peerCancelled) return;
      if (terminal) {
        if (terminalFailure != null && terminalFailure != failure) {
          terminalFailure.addSuppressed(failure);
        }
        return;
      }
      terminal = true;
      terminalFailure = failure;
      requestOutstanding = false;
      pendingRequest = null;
      responses.clear();
    }
    cancelPoll();
    Throwable cleanupFailure = closeWatchersCollecting();
    if (cleanupFailure != null && cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    if (scheduleWriter) scheduleDrain();
    if (cleanupFailure instanceof Error fatal) throw fatal;
  }

  private boolean terminalOrPeerCancelled() {
    synchronized (lock) { return terminal || peerCancelled; }
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof java.util.concurrent.CompletionException
        || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  @Override
  public void close() {
    List<Watcher> closing;
    synchronized (lock) {
      if (terminal && watchers.isEmpty()) return;
      terminal = true;
      requestOutstanding = false;
      pendingRequest = null;
      responses.clear();
      closing = new ArrayList<>(watchers.values());
      watchers.clear();
    }
    cancelPoll();
    closeWatchers(closing);
  }

  private Throwable closeWatchersCollecting() {
    List<Watcher> closing;
    synchronized (lock) {
      closing = new ArrayList<>(watchers.values());
      watchers.clear();
    }
    return closeWatchersCollecting(closing);
  }

  private void closeWatchers() {
    Throwable failure = closeWatchersCollecting();
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException cleanup) throw cleanup;
  }

  private static void closeWatchers(List<Watcher> closing) {
    Throwable failure = closeWatchersCollecting(closing);
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException cleanup) throw cleanup;
  }

  private static Throwable closeWatchersCollecting(List<Watcher> closing) {
    Throwable failure = null;
    for (Watcher watcher : closing) {
      try {
        watcher.close();
      } catch (RuntimeException | Error cleanup) {
        if (failure == null) failure = cleanup;
        else if (cleanup != failure) {
          if (cleanup instanceof Error && !(failure instanceof Error)) {
            cleanup.addSuppressed(failure);
            failure = cleanup;
          } else {
            failure.addSuppressed(cleanup);
          }
        }
      }
    }
    return failure;
  }

  private static final class InvalidRange extends RuntimeException {
    private InvalidRange() { super("mvcc: watcher range is empty"); }
  }

  private final class Watcher implements AutoCloseable {
    private final long id;
    private final long minimumEventRevision;
    private final boolean previousKv;
    private final boolean filterPut;
    private final boolean filterDelete;
    private final Subscription subscription;
    private final boolean compacted;
    private Delivery pendingDelivery;
    private boolean createdSent;
    private boolean closed;

    private Watcher(long id, long minimumEventRevision,
        boolean previousKv, boolean filterPut, boolean filterDelete, Subscription subscription,
        boolean compacted) {
      this.id = id;
      this.minimumEventRevision = minimumEventRevision;
      this.previousKv = previousKv;
      this.filterPut = filterPut;
      this.filterDelete = filterDelete;
      this.subscription = Objects.requireNonNull(subscription, "subscription");
      this.compacted = compacted;
    }

    private synchronized Delivery takeNext() {
      if (closed) return null;
      if (pendingDelivery != null) {
        Delivery delivery = pendingDelivery;
        pendingDelivery = null;
        return delivery;
      }
      Delivery delivery = subscription.poll().orElse(null);
      if (closed && delivery != null) {
        delivery.close();
        return null;
      }
      return delivery;
    }

    private synchronized Delivery pollNext() {
      if (closed) return null;
      Delivery delivery = subscription.poll().orElse(null);
      if (closed && delivery != null) {
        delivery.close();
        return null;
      }
      return delivery;
    }

    private synchronized void defer(Delivery delivery) {
      if (closed) delivery.close();
      else pendingDelivery = delivery;
    }

    private synchronized boolean isClosed() { return closed; }

    @Override public synchronized void close() {
      if (closed) return;
      closed = true;
      RuntimeException failure = null;
      if (pendingDelivery != null) {
        try { pendingDelivery.close(); }
        catch (RuntimeException cleanup) { failure = cleanup; }
        pendingDelivery = null;
      }
      try { subscription.close(); }
      catch (RuntimeException cleanup) {
        if (failure == null) failure = cleanup;
        else if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      if (failure != null) throw failure;
    }
  }
}
