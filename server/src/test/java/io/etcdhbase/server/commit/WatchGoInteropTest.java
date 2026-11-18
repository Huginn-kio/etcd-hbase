package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.service.WatchService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WatchGoInteropTest {
  private static final String TABLE_GROUP = "watch-go-interop";

  @Test
  void pinnedGoClientMatchesStartAndLifecycleCasesAgainstJavaWatchService()
      throws Exception {
    Path root = repositoryRoot();
    Path artifactRoot = artifactRoot(root);
    Path artifacts = artifactRoot.resolve("j45/local-service");
    Path lifecycleArtifacts = artifactRoot.resolve("j45/local-lifecycle");
    Files.createDirectories(artifacts);
    Files.createDirectories(lifecycleArtifacts);
    Path driver = artifactRoot.resolve("j45/m4driver");
    runProcess(new ProcessBuilder(root.resolve("scripts/dev/go-m0.sh").toString(), "build",
        "-trimpath", "-o", driver.toString(), "./cmd/m4driver").directory(root.toFile()),
        artifacts.resolve("go-build.log"), Duration.ofMinutes(3));

    String runId = runId(artifactRoot);
    Path scenario = root.resolve("testkit/scenarios/m4-watch-starts.yaml");
    runLocalPhase(root, driver, scenario, artifacts, runId, "initial", 2, -1, false);
    runLocalPhase(root, driver, scenario, artifacts, runId, "compacted", 8, 4, false);
    runLocalPhase(root, driver, scenario, lifecycleArtifacts, runId, "initial", 1, -1, true);
  }

  private static void runLocalPhase(Path root, Path driver, Path scenario, Path artifacts,
      String runId, String phase, long revision, long compactRevision, boolean runLifecycle)
      throws Exception {
    String prefix = "etcd-hbase-m4-test/" + runId + "/watch/" + phase + "/";
    Path trace = artifacts.resolve(phase + "-trace.jsonl");
    Path metadata = artifacts.resolve(phase + "-metadata.json");
    Path log = artifacts.resolve(phase + ".log");
    var fixture = new LocalWatchServer(revision, compactRevision, prefix);
    try (fixture) {
      List<String> command = new ArrayList<>(List.of(driver.toString(), "run", "--scenario", scenario.toString(),
          "--endpoint", "127.0.0.1:" + fixture.port(), "--scope", "local-service",
          "--prefix", prefix, "--trace", trace.toString(), "--metadata", metadata.toString(),
          "--watch-phase", phase, "--watch-revision", Long.toString(revision),
          "--watch-compact-revision", Long.toString(compactRevision), "--timeout", "90s"));
      if (runLifecycle) command.add("--watch-lifecycle");
      runProcess(new ProcessBuilder(command).directory(root.toFile()), log, Duration.ofSeconds(110));
    }
    assertEquals(0, fixture.budgets.snapshot().requests(), "Go Watch requests leaked admission");
    assertEquals(0, fixture.budgets.snapshot().allocatedBytes(), "Go Watch state memory leaked");
    assertEquals(0, fixture.activeSubscriptions.get(), "Go Watch subscriptions leaked tailer ownership");
    assertTrue(Files.readString(log).contains("M4 Watch start scenario passed"),
        "Go Watch start scenario did not report success");
    if (runLifecycle) {
      assertTrue(Files.readString(log).contains("M4 Watch lifecycle interop passed"),
          "Go Watch lifecycle interop did not report success");
    }
    var document = new ObjectMapper().readTree(Files.readString(metadata));
    assertEquals("local-service", document.path("evidence_scope").asText());
    assertEquals(phase, document.path("watch_phase").asText());
    assertEquals(revision, document.path("watch_revision").asLong());
    assertEquals(compactRevision, document.path("watch_compact_revision").asLong());
    assertEquals("passed", document.path("status").asText());
  }

  private static final class LocalWatchServer implements AutoCloseable {
    private final RequestScope startup = RequestScope.withoutDeadline();
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1);
    private final ScheduledThreadPoolExecutor workers = new ScheduledThreadPoolExecutor(2);
    private final RequestScopeInterceptor requests = new RequestScopeInterceptor(
        deadlines, 16, Duration.ofMinutes(2), 1 << 20, RequestCompletionRecorder.NOOP);
    private final BudgetManager budgets;
    private final AtomicInteger activeSubscriptions = new AtomicInteger();
    private final Server server;

    private LocalWatchServer(long revision, long compactRevision, String lifecyclePrefix)
        throws IOException, BackendFailure {
      var limits = new BudgetManager.Limits(16, 16, 16, 16, 16, 1L << 20, 4L << 20);
      budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
      var tailer = mock(SharedEventTailer.class);
      List<PreparedEvent> lifecycleEvents = watchLifecycleEvents(lifecyclePrefix, revision > 1);
      when(tailer.isClosed()).thenReturn(false);
      when(tailer.completedRevision()).thenReturn(revision);
      long storedCompactRevision = compactRevision == -1 ? 0 : compactRevision;
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong())).thenAnswer(invocation -> {
            KeyRange range = invocation.getArgument(0);
            var pending = new ConcurrentLinkedQueue<SharedEventTailer.Delivery>();
            lifecycleEvents.stream()
                .filter(event -> range.contains(event.current().key()))
                .map(LocalWatchServer::delivery)
                .forEach(pending::add);
            var subscription = mock(SharedEventTailer.Subscription.class);
            when(subscription.poll()).thenAnswer(ignored -> Optional.ofNullable(pending.poll()));
            when(subscription.termination()).thenReturn(Optional.empty());
            AtomicBoolean closed = new AtomicBoolean();
            activeSubscriptions.incrementAndGet();
            doAnswer(ignored -> {
              if (closed.compareAndSet(false, true)) activeSubscriptions.decrementAndGet();
              return null;
            }).when(subscription).close();
            return new SharedEventTailer.SubscriptionStart(subscription, 0, revision);
          });

      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      pending.installEventTailer(tailer);
      var control = mock(CommitControlStore.class);
      var state = CommitState.restore(CommitState.FORMAT_VERSION, ByteKey.copyOf(new byte[32]),
          Optional.empty(), 0, 0, 1, 0, revision, 0, revision, Optional.empty(), storedCompactRevision);
      when(control.read(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(state, new byte[] {1}))));
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, budgets, workers, workers,
          11, 12, 64 * 1024);
      deadlines.setRemoveOnCancelPolicy(true);
      workers.setRemoveOnCancelPolicy(true);
      Server candidate = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
          .intercept(requests).addService(new WatchService(gateway.handler())).build();
      try {
        server = candidate.start();
      } catch (IOException | RuntimeException | Error failure) {
        candidate.shutdownNow();
        requests.close();
        workers.shutdownNow();
        deadlines.shutdownNow();
        startup.close();
        throw failure;
      }
    }

    int port() { return server.getPort(); }

    private static SharedEventTailer.Delivery delivery(PreparedEvent event) {
      var delivery = mock(SharedEventTailer.Delivery.class);
      // A default Mockito sequence of zero would be filtered as being behind J46's barrier.
      when(delivery.sequence()).thenReturn(Math.addExact(event.baseSequence(), 1));
      when(delivery.revision()).thenReturn(event.current().metadata().modificationRevision());
      when(delivery.event()).thenReturn(event);
      return delivery;
    }

    @Override
    public void close() throws Exception {
      try {
        server.shutdownNow();
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS), "local Watch server did not stop");
      } finally {
        try {
          requests.close();
        } finally {
          workers.shutdownNow();
          deadlines.shutdownNow();
          startup.close();
        }
      }
    }
  }

  private static Path repositoryRoot() {
    for (Path candidate = Path.of("").toAbsolutePath(); candidate != null;
        candidate = candidate.getParent()) {
      if (Files.isRegularFile(candidate.resolve("scripts/dev/go-m0.sh"))) return candidate;
    }
    throw new IllegalStateException("could not locate the M4 repository root");
  }

  private static List<PreparedEvent> watchLifecycleEvents(String prefix, boolean includeWatchHistory) {
    String base = prefix + "interop/";
    String filter = base + "filter/";
    var events = new ArrayList<PreparedEvent>();
    if (includeWatchHistory) {
      events.add(watchPut(2, 0, (prefix + "initial/minus-one").getBytes(),
          "before-negative-watch".getBytes(), null));
    }
    events.addAll(List.of(
        watchPut(2, includeWatchHistory ? 1 : 0,
            (base + "multi/a").getBytes(), "multi-a".getBytes(), null),
        watchPut(3, (base + "multi/b").getBytes(), "multi-b".getBytes(), null),
        watchPut(4, new byte[] {0, (byte) 0xff, 0x0f}, "binary-before".getBytes(), null),
        watchPut(5, new byte[] {0, (byte) 0xff, 0x10}, "binary-key".getBytes(), null),
        watchPut(6, new byte[] {0, (byte) 0xff, 0x11}, "binary-after".getBytes(), null),
        watchPut(7, new byte[] {(byte) 0xff, 0}, "binary-high".getBytes(), null),
        watchPut(8, new byte[] {0}, "empty-key".getBytes(), null),
        watchPut(9, (filter + "put").getBytes(), "put-after".getBytes(), "put-before".getBytes()),
        watchDelete(10, (filter + "delete").getBytes(), "delete-before".getBytes()),
        watchPut(11, (filter + "put-marker").getBytes(), "put-marker".getBytes(), null),
        watchDelete(12, (filter + "delete-marker").getBytes(), "delete-marker-before".getBytes())));
    return List.copyOf(events);
  }

  private static PreparedEvent watchPut(long revision, byte[] key, byte[] value, byte[] previous) {
    return watchPut(revision, 0, key, value, previous);
  }

  private static PreparedEvent watchPut(long revision, long ordinal, byte[] key, byte[] value,
      byte[] previous) {
    long createRevision = previous == null ? revision : 1;
    long version = previous == null ? 1 : 2;
    var metadata = new StorageMetadata(createRevision, revision, version, 0,
        StorageMetadata.State.LIVE, value.length);
    var current = StoredRecord.live(ByteKey.copyOf(key), metadata, value);
    Optional<StoredRecord> previousRecord = Optional.empty();
    if (previous != null) {
      previousRecord = Optional.of(StoredRecord.live(ByteKey.copyOf(key),
          new StorageMetadata(1, revision - 1, 1, 0, StorageMetadata.State.LIVE, previous.length),
          previous));
    }
    // This fixture starts from initialized revision 1 and advances one selected sequence per KV revision.
    return new PreparedEvent(new TransactionId(new UUID(0, revision)), 1, revision - 2,
        revision - 1, ordinal, PreparedEvent.Kind.PUT, current, previousRecord);
  }

  private static PreparedEvent watchDelete(long revision, byte[] key, byte[] previous) {
    var current = StoredRecord.metadataOnly(ByteKey.copyOf(key), new StorageMetadata(1, revision,
        1, 0, StorageMetadata.State.TOMBSTONE, 0));
    var previousRecord = StoredRecord.live(ByteKey.copyOf(key), new StorageMetadata(1,
        revision - 1, 1, 0, StorageMetadata.State.LIVE, previous.length), previous);
    return new PreparedEvent(new TransactionId(new UUID(0, revision)), 1, revision - 2,
        revision - 1, 0, PreparedEvent.Kind.DELETE, current, Optional.of(previousRecord));
  }

  private static Path artifactRoot(Path root) {
    String configured = System.getenv("M4_ARTIFACT_ROOT");
    if (configured != null && !configured.isBlank()) return Path.of(configured).toAbsolutePath();
    return root.resolve(".artifacts/m4/j45-local-" + UUID.randomUUID());
  }

  private static String runId(Path artifactRoot) {
    String configured = System.getenv("M4_RUN_ID");
    if (configured != null && configured.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      return configured;
    }
    return artifactRoot.getFileName().toString();
  }

  private static void runProcess(ProcessBuilder builder, Path log, Duration timeout)
      throws IOException, InterruptedException {
    Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS),
          "Go Watch client process exceeded its deadline");
      assertEquals(0, process.exitValue(), () -> readLog(log));
    } finally {
      if (process.isAlive()) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Go Watch process did not stop");
      }
    }
  }

  private static String readLog(Path path) {
    try {
      return Files.readString(path);
    } catch (IOException failure) {
      return "Go Watch client log is unavailable";
    }
  }
}
