package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.grpc.RequestCompletionRecorder;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.server.fixture.FailureFixture;
import io.etcdhbase.server.fixture.WatchCancellationFixture;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GeneralGoInteropIT {
  @Test
  void observesRealSkeletonAndIsolatedErrorFixtureFromThePinnedGoDriver() throws Exception {
    Path root = WireProcesses.repositoryRoot();
    Path artifacts = WireProcesses.artifacts(root);
    Path trace = artifacts.resolve("transport.jsonl");
    Path driver = WireProcesses.driver(root, artifacts);
    try (Bootstrap skeleton =
            Bootstrap.start(new Bootstrap.ListenerBinding("127.0.0.1", 0, "j07-wire"));
        FixtureServer fixture = new FixtureServer()) {
      WireProcesses.run(
          new ProcessBuilder(
                  driver.toString(),
                  "wire",
                  "--skeleton-endpoint",
                  "127.0.0.1:" + skeleton.boundListener().port(),
                  "--fixture-endpoint",
                  "127.0.0.1:" + fixture.server.getPort(),
                  "--trace",
                  trace.toString(),
                  "--timeout",
                  "3s")
              .directory(root.toFile()),
          artifacts.resolve("m0driver.log"),
          Duration.ofSeconds(30));
      assertTrue(
          fixture.failure.awaitDeadlineScopeCleanup(Duration.ofSeconds(2)),
          "Range deadline did not finish scope cleanup");
      assertTrue(fixture.watch.awaitRequestReceived(Duration.ofSeconds(2)));
      assertTrue(fixture.watch.requestWasValid(), "Go/Java Watch bytes or int64 changed");
      assertTrue(
          fixture.watch.awaitScopeClosed(Duration.ofSeconds(2)),
          "Watch deadline did not finish scope cleanup");
      assertTrue(fixture.failure.rawUnavailableRequestCount() >= 1);
      assertTrue(
          fixture.failure.clientV3RetryRequestCount() >= 2,
          "clientv3 did not retry UNAVAILABLE before its caller deadline");
      WireTraceAssertions.assertWireTrace(trace);
    }
  }

  /** The fixture owns its deadline thread and cleanup pool, just like a production listener. */
  private static final class FixtureServer implements AutoCloseable {
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1);
    private final RequestScopeInterceptor requests =
        new RequestScopeInterceptor(
            deadlines, 16, Duration.ofSeconds(5), 1 << 20, RequestCompletionRecorder.NOOP);
    private final FailureFixture failure = new FailureFixture();
    private final WatchCancellationFixture watch = new WatchCancellationFixture();
    private final Server server;

    private FixtureServer() throws Exception {
      deadlines.setRemoveOnCancelPolicy(true);
      Server candidate =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
              .intercept(requests)
              .addService(failure)
              .addService(watch)
              .build();
      try {
        server = candidate.start();
      } catch (Exception | Error problem) {
        candidate.shutdownNow();
        requests.close();
        deadlines.shutdownNow();
        throw problem;
      }
    }

    @Override
    public void close() throws Exception {
      try {
        server.shutdownNow();
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS), "fixture did not stop");
      } finally {
        try {
          requests.close();
        } finally {
          deadlines.shutdownNow();
        }
      }
    }
  }
}
