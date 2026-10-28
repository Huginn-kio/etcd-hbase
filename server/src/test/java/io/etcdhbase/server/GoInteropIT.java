package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.AuthGrpc;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import etcdserverpb.WatchGrpc;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GoInteropIT {
  private static final byte[] KEY = {0x00, (byte) 0xff, 0x61};
  private static final byte[] RANGE_END = {0x00, (byte) 0xff, 0x62};
  private static final long REVISION = 1L << 54;

  @Test
  void fixedGoClientRecognizesJavaMethodsBytesIntegersErrorsAndStreamTermination() throws Exception {
    Path root = repositoryRoot();
    Path artifacts =
        root.resolve(".artifacts/m0/j03/go-interop-" + UUID.randomUUID());
    Files.createDirectories(artifacts);
    Path binary = artifacts.resolve("j03-smoke");
    runProcess(
        new ProcessBuilder(
            root.resolve("scripts/dev/go-j03.sh").toString(),
            "build", "-trimpath", "-o", binary.toString(), ".")
            .directory(root.toFile()),
        artifacts.resolve("go-build.log"),
        Duration.ofSeconds(120));

    ErrorFixture fixture = new ErrorFixture();
    Server fixtureServer =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(fixture.kv())
            .addService(fixture.auth())
            .addService(fixture.watch())
            .build()
            .start();
    try (Bootstrap skeleton =
        Bootstrap.start(new Bootstrap.ListenerBinding("127.0.0.1", 0, "j03-wire"))) {
      Path log = artifacts.resolve("go-smoke.log");
      runProcess(
          new ProcessBuilder(
              binary.toString(),
              "--endpoint", "127.0.0.1:" + skeleton.boundListener().port(),
              "--fixture-endpoint", "127.0.0.1:" + fixtureServer.getPort(),
              "--timeout", "3s")
              .directory(root.toFile()),
          log,
          Duration.ofSeconds(30));
      assertTrue(Files.readString(log).contains("J03 Go/Java smoke passed"));
      Rpc.RangeRequest range = fixture.range.get();
      assertNotNull(range, "Java did not receive the Go binary Range request");
      assertArrayEquals(KEY, range.getKey().toByteArray());
      assertArrayEquals(RANGE_END, range.getRangeEnd().toByteArray());
      assertEquals(REVISION, range.getRevision());
      assertEquals(1, range.getLimit());
      assertTrue(fixture.watchReceived.await(2, TimeUnit.SECONDS));
      Rpc.WatchCreateRequest watch = fixture.watchRequest.get().getCreateRequest();
      assertArrayEquals(KEY, watch.getKey().toByteArray());
      assertArrayEquals(RANGE_END, watch.getRangeEnd().toByteArray());
      assertEquals(REVISION, watch.getStartRevision());
      assertTrue(
          fixture.watchCancelled.await(2, TimeUnit.SECONDS),
          "Go deadline did not cancel the Java streaming context");
    } finally {
      fixtureServer.shutdownNow();
      assertTrue(fixtureServer.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static Path repositoryRoot() {
    for (Path candidate = Path.of("").toAbsolutePath(); candidate != null;
        candidate = candidate.getParent()) {
      if (Files.isRegularFile(candidate.resolve("scripts/dev/go-j03.sh"))) {
        return candidate;
      }
    }
    throw new IllegalStateException("could not locate the J03 repository root");
  }

  private static void runProcess(ProcessBuilder builder, Path log, Duration timeout)
      throws IOException, InterruptedException {
    Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(process.waitFor(timeout.toSeconds(), TimeUnit.SECONDS), "process exceeded deadline");
      assertEquals(0, process.exitValue(), () -> readLog(log));
    } finally {
      if (process.isAlive()) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "test process did not stop");
      }
    }
  }

  private static String readLog(Path log) {
    try {
      return Files.readString(log);
    } catch (IOException failure) {
      return "process log is unavailable";
    }
  }

  /** Transport-only fixtures: no backend access, successful KV values, headers or auth tokens. */
  private static final class ErrorFixture {
    private final AtomicReference<Rpc.RangeRequest> range = new AtomicReference<>();
    private final AtomicReference<Rpc.WatchRequest> watchRequest = new AtomicReference<>();
    private final CountDownLatch watchReceived = new CountDownLatch(1);
    private final CountDownLatch watchCancelled = new CountDownLatch(1);

    KVGrpc.KVImplBase kv() {
      return new KVGrpc.KVImplBase() {
        @Override
        public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> responses) {
          BackendFailure.Category category;
          if (request.getKey().equals(ByteString.copyFromUtf8("permission"))) {
            category = BackendFailure.Category.PERMISSION;
          } else if (request.getKey().equals(ByteString.copyFromUtf8("timeout"))) {
            category = BackendFailure.Category.TIMEOUT;
          } else {
            range.set(request);
            category = BackendFailure.Category.UNAVAILABLE;
          }
          responses.onError(ErrorProjection.project(new BackendFailure(category)));
        }
      };
    }

    AuthGrpc.AuthImplBase auth() {
      return new AuthGrpc.AuthImplBase() {
        @Override
        public void authenticate(
            Rpc.AuthenticateRequest request, StreamObserver<Rpc.AuthenticateResponse> responses) {
          responses.onError(
              ErrorProjection.project(new BackendFailure(BackendFailure.Category.AUTHENTICATION)));
        }
      };
    }

    WatchGrpc.WatchImplBase watch() {
      return new WatchGrpc.WatchImplBase() {
        @Override
        public StreamObserver<Rpc.WatchRequest> watch(StreamObserver<Rpc.WatchResponse> responses) {
          Context.current().addListener(ignored -> watchCancelled.countDown(), Runnable::run);
          return new StreamObserver<>() {
            @Override
            public void onNext(Rpc.WatchRequest request) {
              watchRequest.set(request);
              watchReceived.countDown();
            }

            @Override
            public void onError(Throwable failure) {}

            @Override
            public void onCompleted() {}
          };
        }
      };
    }
  }
}
