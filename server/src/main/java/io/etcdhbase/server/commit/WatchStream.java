package io.etcdhbase.server.commit;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
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
import java.util.function.Consumer;

/** One stream-scoped watch registry and single writer for control responses and event batches. */
final class WatchStream implements StreamObserver<Rpc.WatchRequest>, AutoCloseable {
  private static final int MAX_FILTERED_REVISIONS_PER_DRAIN = 128;
  private static final long REJECTED_CREATE_RESPONSE_WATCH_ID = -1;
  private static final String HISTORICAL_REPLAY_UNSUPPORTED_REASON =
      "etcd-hbase: historical Watch replay is not implemented";
  interface SessionProvider {
    CompletionStage<Session> open();
  }

  interface Session {
    long completedRevision();
    CompletionStage<Long> currentRevision();
    CompletionStage<OpenedWatch> subscribe(KeyRange range, Rpc.WatchCreateRequest request)
        throws BackendFailure;
  }

  record OpenedWatch(long revision, long barrierSequence, long compactRevision,
      Subscription subscription, boolean historicalReplaySupported) {
    OpenedWatch(long revision, Subscription subscription) {
      this(revision, 0, -1, subscription, false);
    }

    OpenedWatch(long revision, long compactRevision, Subscription subscription) {
      this(revision, 0, compactRevision, subscription, false);
    }

    OpenedWatch(long revision, long barrierSequence, long compactRevision,
        Subscription subscription) {
      this(revision, barrierSequence, compactRevision, subscription, false);
    }

    OpenedWatch {
      if (revision < 0 || barrierSequence < 0 || compactRevision < -1) {
        throw new IllegalArgumentException("invalid Watch revision checkpoint");
      }
      Objects.requireNonNull(subscription, "subscription");
    }
  }

  interface Subscription extends AutoCloseable {
    Optional<Delivery> poll();
    Optional<String> terminationReason();
    void setWakeup(Runnable wakeup);
    default boolean progressReady(long revision) { return true; }
    @Override void close();
  }

  static final class Delivery implements AutoCloseable {
    private final long sequence;
    private final long revision;
    private final PreparedEvent event;
    private final Runnable release;
    private final AtomicBoolean closed = new AtomicBoolean();

    Delivery(long revision, PreparedEvent event, Runnable release) {
      this(Math.addExact(Objects.requireNonNull(event, "event").baseSequence(), 1),
          revision, event, release);
    }

    Delivery(long sequence, long revision, PreparedEvent event, Runnable release) {
      if (sequence <= 0) throw new IllegalArgumentException("watch event sequence must be positive");
      if (revision <= 0) throw new IllegalArgumentException("watch event revision must be positive");
      this.sequence = sequence;
      this.revision = revision;
      this.event = Objects.requireNonNull(event, "event");
      this.release = Objects.requireNonNull(release, "release");
    }

    long sequence() { return sequence; }
    long revision() { return revision; }
    long ordinal() { return event.ordinal(); }
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

  private enum DrainKind { STOP, SEND_FAILURE, COMPLETE, SEND_RESPONSE, PROCESS_REQUEST, PUMP }

  private record DrainAction(
      DrainKind kind, PendingResponse response, Rpc.WatchRequest request, Throwable failure) {}
  private record ProgressBarrier(long revision) {}

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
  private final WatchRegistry watchRegistry;
  private final WatchEventBatchEncoder eventBatchEncoder;
  private final ArrayDeque<PendingResponse> responses = new ArrayDeque<>();
  private final AtomicBoolean draining = new AtomicBoolean();
  private final AtomicBoolean drainRequested = new AtomicBoolean();

  private CompletionStage<Session> sessionFuture;
  private Session session;
  private Rpc.WatchRequest pendingRequest;
  private ScheduledFuture<?> drainYieldTask;
  private int nextWatchIndex;
  private int pendingTerminationResponses;
  private boolean processingRequest;
  private boolean requestOutstanding;
  private boolean inboundClosed;
  private boolean terminal;
  private boolean peerCancelled;
  private Throwable terminalFailure;
  private ProgressBarrier pendingProgress;

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes) {
    this(responseObserver, streamScope, worker, timers, sessions, clusterId, memberId,
        maximumResponseBytes, CompactedWatchIdBudget.processWide());
  }

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes,
      CompactedWatchIdBudget compactedWatchIdBudget) {
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
    this.watchRegistry = new WatchRegistry(Objects.requireNonNull(
        compactedWatchIdBudget, "compactedWatchIdBudget"));
    this.eventBatchEncoder = new WatchEventBatchEncoder(clusterId, memberId, maximumResponseBytes);
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
      DrainAction action = nextDrainAction();
      switch (action.kind()) {
        case STOP -> { return; }
        case SEND_FAILURE -> {
          sendTerminalFailure(action.failure());
          return;
        }
        case COMPLETE -> {
          completeInboundStream();
          return;
        }
        case SEND_RESPONSE -> {
          sendResponse(action.response());
          continue;
        }
        case PROCESS_REQUEST -> {
          processRequest(action.request());
          continue;
        }
        case PUMP -> { }
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
          scheduleDrainYield();
          return;
        }
        continue;
      }
      if (enqueuePendingProgress()) continue;
      requestNext();
      return;
    }
  }

  private DrainAction nextDrainAction() {
    synchronized (lock) {
      if (peerCancelled) return new DrainAction(DrainKind.STOP, null, null, null);
      if (terminalFailure != null) {
        Throwable failure = terminalFailure;
        terminalFailure = null;
        return new DrainAction(DrainKind.SEND_FAILURE, null, null, failure);
      }
      if (terminal) return new DrainAction(DrainKind.STOP, null, null, null);
      if (pendingTerminationResponses > 0) return new DrainAction(DrainKind.STOP, null, null, null);
      if (!responses.isEmpty()) {
        if (!isReady()) return new DrainAction(DrainKind.STOP, null, null, null);
        return new DrainAction(DrainKind.SEND_RESPONSE, responses.removeFirst(), null, null);
      }
      if (pendingRequest != null && !processingRequest) {
        Rpc.WatchRequest request = pendingRequest;
        pendingRequest = null;
        processingRequest = true;
        return new DrainAction(DrainKind.PROCESS_REQUEST, null, request, null);
      }
      if (inboundClosed && pendingRequest == null && !processingRequest) {
        return new DrainAction(DrainKind.COMPLETE, null, null, null);
      }
      if (!isReady()) return new DrainAction(DrainKind.STOP, null, null, null);
      return new DrainAction(DrainKind.PUMP, null, null, null);
    }
  }

  private void sendTerminalFailure(Throwable failure) {
    Throwable cause = unwrap(failure);
    try {
      responseObserver.onError(cause instanceof StatusRuntimeException statusFailure
          ? statusFailure : ErrorProjection.project(cause));
    } finally {
      streamScope.cancel();
    }
  }

  private void completeInboundStream() {
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
    cancelDrainYield();
    responseObserver.onCompleted();
  }

  private void sendResponse(PendingResponse response) {
    try {
      responseObserver.onNext(response.response());
      response.afterSend().run();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void processRequest(Rpc.WatchRequest request) {
    switch (request.getRequestUnionCase()) {
      case CREATE_REQUEST -> processCreate(request.getCreateRequest());
      case CANCEL_REQUEST -> processCancel(request.getCancelRequest());
      case PROGRESS_REQUEST -> processProgressRequest();
      case REQUESTUNION_NOT_SET -> finishRequestWithoutResponse();
    }
  }

  private void processProgressRequest() {
    boolean hasActiveWatch;
    synchronized (lock) {
      hasActiveWatch = watchRegistry.activeSnapshot().stream()
          .anyMatch(watcher -> watcher.createdSent() && !watcher.isClosed());
    }
    if (!hasActiveWatch) {
      finishRequestWithoutResponse();
      return;
    }
    session().whenComplete((openedSession, sessionFailure) -> executeCompletion(() -> {
      if (sessionFailure != null) {
        fail(sessionFailure);
        return;
      }
      CompletionStage<Long> revision;
      try {
        revision = Objects.requireNonNull(openedSession.currentRevision(),
            "current Watch revision future");
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      revision.whenComplete((currentRevision, revisionFailure) -> executeCompletion(() ->
          completeProgressRequest(openedSession, currentRevision, revisionFailure)));
    }));
  }

  private void completeProgressRequest(Session openedSession, Long revision, Throwable failure) {
    if (failure != null) {
      fail(unwrap(failure));
      return;
    }
    if (revision == null || revision < 0) {
      fail(new IllegalStateException("Watch session revision is invalid"));
      return;
    }
    boolean ready;
    try {
      synchronized (lock) {
        if (terminal || peerCancelled) return;
        List<WatchRegistry.Watcher> active = watchRegistry.activeSnapshot().stream()
            .filter(watcher -> watcher.createdSent() && !watcher.isClosed())
            .toList();
        // etcd v3.5.23 drops the request while a stream watcher is unsynced or starts in the future.
        ready = !active.isEmpty() && openedSession.completedRevision() >= revision
            && active.stream().allMatch(watcher -> watcher.startRevision() <= 0
                || watcher.startRevision() <= revision)
            && active.stream().allMatch(watcher -> watcher.progressReady(revision));
        if (ready) pendingProgress = new ProgressBarrier(revision);
        else processingRequest = false;
      }
    } catch (RuntimeException | Error failureToCheck) {
      fail(failureToCheck);
      if (failureToCheck instanceof Error fatal) throw fatal;
      return;
    }
    if (!ready) requestNext();
    scheduleDrain();
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
      if (rejectUnadmittedCreate(openedSession, request)) return;
      subscribe(openedSession, request, range);
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private boolean rejectUnadmittedCreate(Session openedSession, Rpc.WatchCreateRequest request) {
    boolean stopped;
    WatchRegistry.Rejection rejection = WatchRegistry.Rejection.NONE;
    synchronized (lock) {
      stopped = terminal || peerCancelled || inboundClosed;
      if (!stopped) rejection = watchRegistry.checkCreate(request.getWatchId());
      if (stopped) processingRequest = false;
    }
    if (stopped) {
      scheduleDrain();
      return true;
    }
    if (rejection == WatchRegistry.Rejection.NONE) return false;
    rejectCreateAtCurrentRevision(openedSession, rejectionReason(rejection));
    return true;
  }

  private void subscribe(Session openedSession, Rpc.WatchCreateRequest request, KeyRange range) {
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
    opening.whenComplete((opened, failure) -> executeCompletion(
        () -> completeSubscribedCreate(openedSession, request, opened, failure),
        schedulingFailure -> closeUnregisteredWatch(opened, failure, schedulingFailure)));
  }

  private void completeSubscribedCreate(Session openedSession, Rpc.WatchCreateRequest request,
      OpenedWatch opened, Throwable subscriptionFailure) {
    if (subscriptionFailure != null) {
      handleSubscriptionFailure(openedSession, subscriptionFailure);
      return;
    }
    try {
      Objects.requireNonNull(opened, "opened Watch subscription");
      if (request.getStartRevision() != 0
          && request.getStartRevision() < opened.compactRevision()) {
        completeCompactedCreate(opened, request);
        return;
      }
      if (request.getStartRevision() > 0 && request.getStartRevision() <= opened.revision()
          && !opened.historicalReplaySupported()) {
        rejectUnsupportedHistoricalReplay(opened);
        return;
      }
      registerSubscribedWatch(opened, request);
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void handleSubscriptionFailure(Session openedSession, Throwable failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof BackendFailure backendFailure
        && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
      rejectCreateAtCurrentRevision(openedSession, "etcd-hbase: Watch resource limit exceeded");
    } else {
      fail(cause);
    }
  }

  private void rejectUnsupportedHistoricalReplay(OpenedWatch opened) {
    opened.subscription().close();
    rejectCreate(opened.revision(), HISTORICAL_REPLAY_UNSUPPORTED_REASON);
  }

  private void registerSubscribedWatch(OpenedWatch opened, Rpc.WatchCreateRequest request) {
    WatchRegistry.Registration registration = null;
    boolean stopped;
    synchronized (lock) {
      stopped = terminal || peerCancelled || inboundClosed;
      if (!stopped) {
        long minimumEventRevision = request.getStartRevision() <= 0
            ? nextRevision(opened.revision()) : request.getStartRevision();
        registration = watchRegistry.registerActive(request.getWatchId(), request.getStartRevision(), minimumEventRevision,
            request.getPrevKv(),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NOPUT),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NODELETE),
            opened.subscription());
        if (registration.rejection() == WatchRegistry.Rejection.NONE) {
          WatchRegistry.Watcher watcher = registration.watcher();
          enqueueResponse(createdResponse(registration.watchId(), opened.revision()), () -> {
            watcher.markCreatedSent();
            requestNext();
          });
        }
      }
      processingRequest = false;
    }
    if (!stopped && registration != null
        && registration.rejection() == WatchRegistry.Rejection.NONE) {
      try {
        registration.watcher().setWakeup(this::scheduleDrain);
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
    }
    if (stopped || registration == null
        || registration.rejection() != WatchRegistry.Rejection.NONE) {
      opened.subscription().close();
      if (stopped) {
        scheduleDrain();
      } else {
        rejectCreate(opened.revision(), rejectionReason(registration.rejection()));
      }
      return;
    }
    scheduleDrain();
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
      opened.subscription().close();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }

    boolean stopped;
    WatchRegistry.Registration registration = null;
    synchronized (lock) {
      stopped = terminal || peerCancelled || inboundClosed;
      if (!stopped) {
        registration = watchRegistry.retainCompacted(request.getWatchId());
        if (registration.rejection() == WatchRegistry.Rejection.NONE) {
          try {
            enqueueResponse(createdResponse(registration.watchId(), opened.revision()), () -> {});
            enqueueResponse(compactedResponse(
                registration.watchId(), 0, opened.compactRevision()), this::requestNext);
          } catch (RuntimeException | Error failure) {
            watchRegistry.removeCompacted(registration.watchId());
            throw failure;
          }
        }
      }
      processingRequest = false;
    }
    if (stopped) {
      scheduleDrain();
    } else if (registration.rejection() != WatchRegistry.Rejection.NONE) {
      rejectCreate(opened.revision(), rejectionReason(registration.rejection()));
    } else {
      scheduleDrain();
    }
  }

  private static String rejectionReason(WatchRegistry.Rejection rejection) {
    return switch (rejection) {
      case DUPLICATE_ID -> "mvcc: duplicate watch ID provided on the WatchStream";
      case STREAM_LIMIT -> "etcd-hbase: Watch stream subscription limit exceeded";
      case RETENTION_LIMIT -> "etcd-hbase: compacted Watch ID retention limit exceeded";
      case NONE -> throw new IllegalArgumentException("successful Watch registration has no rejection");
    };
  }

  private void processCancel(Rpc.WatchCancelRequest request) {
    if (request == null) {
      finishRequestWithoutResponse();
      return;
    }
    WatchRegistry.Watcher watcher;
    boolean compactedTombstone;
    Session currentSession;
    synchronized (lock) {
      watcher = watchRegistry.removeActive(request.getWatchId());
      compactedTombstone = watcher == null && watchRegistry.removeCompacted(request.getWatchId());
      currentSession = session;
      if (watcher == null) processingRequest = false;
    }
    if (watcher == null && !compactedTombstone) {
      requestNext();
      return;
    }
    if (watcher != null) {
      try {
        watcher.close();
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
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
        completeCancel(request.getWatchId(), currentRevision, failure)));
  }

  private void completeCancel(long watchId, Long revision, Throwable failure) {
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
      enqueueResponse(cancelledResponse(watchId, revision), this::requestNext);
    }
    scheduleDrain();
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
    List<WatchRegistry.Watcher> snapshot;
    Long progressRevision;
    int first;
    synchronized (lock) {
      snapshot = watchRegistry.activeSnapshot();
      if (snapshot.isEmpty()) return PumpResult.IDLE;
      progressRevision = pendingProgress == null ? null : pendingProgress.revision();
      first = Math.floorMod(nextWatchIndex, snapshot.size());
    }
    for (int offset = 0; offset < snapshot.size(); offset++) {
      int index = (first + offset) % snapshot.size();
      WatchRegistry.Watcher watcher = snapshot.get(index);
      if (!watcher.createdSent()) continue;
      WatchEventBatchEncoder.Poll poll;
      synchronized (watcher) { poll = eventBatchEncoder.nextBatch(watcher, progressRevision); }
      if (poll.terminationReason() != null) {
        terminateWatcher(watcher, poll.terminationReason());
        advanceWatchIndex(index);
        return PumpResult.ADVANCED;
      }
      WatchEventBatchEncoder.Output output = poll.output();
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
    synchronized (lock) {
      nextWatchIndex = (index + 1) % Math.max(1, watchRegistry.activeCount());
    }
  }

  private boolean enqueuePendingProgress() {
    boolean noActiveWatch;
    synchronized (lock) {
      if (pendingProgress == null) return false;
      noActiveWatch = watchRegistry.activeSnapshot().stream()
          .noneMatch(watcher -> watcher.createdSent() && !watcher.isClosed());
      long revision = pendingProgress.revision();
      pendingProgress = null;
      if (noActiveWatch) {
        processingRequest = false;
      } else {
        responses.addLast(new PendingResponse(
            Rpc.WatchResponse.newBuilder().setHeader(header(revision)).setWatchId(-1).build(), () -> {
              synchronized (lock) { processingRequest = false; }
              requestNext();
            }));
      }
    }
    if (noActiveWatch) requestNext();
    return true;
  }

  private void terminateWatcher(WatchRegistry.Watcher watcher, String reason) {
    Session currentSession;
    synchronized (lock) {
      if (!watchRegistry.removeActive(watcher.id(), watcher)) return;
      pendingTerminationResponses++;
      currentSession = session;
    }
    try {
      watcher.close();
    } catch (RuntimeException | Error failure) {
      completeTerminationResponse(watcher.id(), reason, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }

    CompletionStage<Long> revision;
    try {
      revision = currentSession == null
          ? java.util.concurrent.CompletableFuture.completedFuture(0L)
          : Objects.requireNonNull(currentSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      completeTerminationResponse(watcher.id(), reason, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    revision.whenComplete((currentRevision, failure) -> executeCompletion(
        () -> completeTerminationResponse(watcher.id(), reason, currentRevision, failure),
        ignored -> releasePendingTerminationResponse()));
  }

  private void completeTerminationResponse(long watchId, String reason, Long revision, Throwable failure) {
    Throwable cause = failure == null ? null : unwrap(failure);
    if (cause == null && (revision == null || revision < 0)) {
      cause = new IllegalStateException("Watch session revision is invalid");
    }
    synchronized (lock) {
      releasePendingTerminationResponseLocked();
      if (terminal || peerCancelled) return;
      if (cause == null) enqueueResponse(cancelledResponse(watchId, revision, reason), this::requestNext);
    }
    if (cause != null) {
      fail(cause);
      return;
    }
    scheduleDrain();
  }

  private void releasePendingTerminationResponse() {
    synchronized (lock) { releasePendingTerminationResponseLocked(); }
  }

  private void releasePendingTerminationResponseLocked() {
    if (pendingTerminationResponses <= 0) {
      throw new IllegalStateException("Watch termination response count is unbalanced");
    }
    pendingTerminationResponses--;
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

  private void scheduleDrainYield() {
    synchronized (lock) {
      if (terminal || peerCancelled || drainYieldTask != null) return;
      try {
        drainYieldTask = timers.schedule(() -> {
          synchronized (lock) { drainYieldTask = null; }
          scheduleDrain();
        }, 0, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException rejected) {
        // The bounded drain must yield to avoid monopolizing the worker on filtered revisions.
        fail(rejected);
      }
    }
  }

  private void cancelDrainYield() {
    ScheduledFuture<?> scheduled;
    synchronized (lock) {
      scheduled = drainYieldTask;
      drainYieldTask = null;
    }
    if (scheduled != null) scheduled.cancel(false);
  }

  private boolean isReady() {
    return serverObserver == null || serverObserver.isReady();
  }

  private boolean hasRunnableWork() {
    synchronized (lock) {
      return !peerCancelled && (terminalFailure != null || !terminal && pendingTerminationResponses == 0
          && (pendingRequest != null || !responses.isEmpty() && isReady()
          || inboundClosed && pendingRequest == null && !processingRequest && responses.isEmpty()));
    }
  }

  private void executeCompletion(Runnable completion) {
    executeCompletion(completion, ignored -> {});
  }

  private void executeCompletion(Runnable completion, Consumer<Throwable> schedulingFailureCleanup) {
    try {
      worker.execute(completion);
    } catch (RejectedExecutionException rejected) {
      failCompletion(rejected, schedulingFailureCleanup);
    } catch (Error fatal) {
      failCompletion(fatal, schedulingFailureCleanup);
      throw fatal;
    }
  }

  private void failCompletion(Throwable failure, Consumer<Throwable> schedulingFailureCleanup) {
    try {
      schedulingFailureCleanup.accept(failure);
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    }
    fail(failure);
  }

  private static void closeUnregisteredWatch(OpenedWatch opened, Throwable openingFailure,
      Throwable schedulingFailure) {
    if (openingFailure != null || opened == null) return;
    try {
      opened.subscription().close();
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != schedulingFailure) schedulingFailure.addSuppressed(cleanupFailure);
    }
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

  private void cancelFromPeer() {
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      peerCancelled = true;
      requestOutstanding = false;
      pendingRequest = null;
      pendingProgress = null;
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
      pendingProgress = null;
      responses.clear();
    }
    cancelDrainYield();
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
    List<WatchRegistry.Watcher> closing;
    synchronized (lock) {
      if (terminal && watchRegistry.isEmpty()) return;
      terminal = true;
      requestOutstanding = false;
      pendingRequest = null;
      pendingProgress = null;
      responses.clear();
      closing = watchRegistry.detachAll();
    }
    cancelDrainYield();
    closeWatchers(closing);
  }

  private Throwable closeWatchersCollecting() {
    List<WatchRegistry.Watcher> closing;
    synchronized (lock) {
      closing = watchRegistry.detachAll();
    }
    return closeWatchersCollecting(closing);
  }

  private void closeWatchers() {
    Throwable failure = closeWatchersCollecting();
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException cleanup) throw cleanup;
  }

  private static void closeWatchers(List<WatchRegistry.Watcher> closing) {
    Throwable failure = closeWatchersCollecting(closing);
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException cleanup) throw cleanup;
  }

  private static Throwable closeWatchersCollecting(List<WatchRegistry.Watcher> closing) {
    Throwable failure = null;
    for (WatchRegistry.Watcher watcher : closing) {
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
}
