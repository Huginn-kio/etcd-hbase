package io.etcdhbase.server;

import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.ClusterService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.MaintenanceService;
import io.etcdhbase.grpc.service.WatchService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Starts the J03 protocol skeleton without loading storage or runtime-configuration providers. */
public final class Bootstrap implements AutoCloseable {
  private static final Set<String> OPTIONS = Set.of("--bind-host", "--port", "--table-group");
  private static final String USAGE =
      "usage: j03-server.sh --bind-host <loopback> --port <0..65535> --table-group <id>";

  private final Server server;
  private final BoundListener boundListener;
  private final AtomicBoolean closed = new AtomicBoolean();

  private Bootstrap(Server server, ListenerBinding binding) {
    this.server = server;
    this.boundListener =
        new BoundListener(binding.bindHost(), server.getPort(), binding.tableGroupId());
  }

  public static Bootstrap start(ListenerBinding binding) throws IOException {
    Objects.requireNonNull(binding, "binding");
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress(binding.bindHost(), binding.port()))
            .addService(new KvService())
            .addService(new WatchService())
            .addService(new LeaseService())
            .addService(new AuthService())
            .addService(new ClusterService())
            .addService(new MaintenanceService())
            .build();
    try {
      server.start();
      return new Bootstrap(server, binding);
    } catch (IOException | RuntimeException failure) {
      server.shutdownNow();
      throw failure;
    }
  }

  public BoundListener boundListener() {
    return boundListener;
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long nanos;
    try {
      nanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      nanos = Long.MAX_VALUE;
    }
    return server.awaitTermination(nanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    server.shutdown();
    try {
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow();
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("gRPC skeleton did not terminate");
        }
      }
    } catch (InterruptedException interrupted) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  static ListenerBinding parseArguments(String[] arguments) {
    if (arguments.length != 6) {
      throw new IllegalArgumentException(USAGE);
    }
    Map<String, String> values = new HashMap<>();
    for (int index = 0; index < arguments.length; index += 2) {
      String option = arguments[index];
      if (!OPTIONS.contains(option) || values.putIfAbsent(option, arguments[index + 1]) != null) {
        throw new IllegalArgumentException(USAGE);
      }
    }
    try {
      return new ListenerBinding(
          values.get("--bind-host"),
          Integer.parseInt(values.get("--port")),
          values.get("--table-group"));
    } catch (NumberFormatException invalidPort) {
      throw new IllegalArgumentException("port must be an integer");
    }
  }

  static int run(String[] arguments) {
    ListenerBinding binding;
    try {
      binding = parseArguments(arguments);
    } catch (IllegalArgumentException invalidArguments) {
      System.err.println(USAGE);
      return 2;
    }
    try (Bootstrap bootstrap = start(binding)) {
      Thread shutdown = new Thread(bootstrap::close, "j03-grpc-shutdown");
      Runtime.getRuntime().addShutdownHook(shutdown);
      try {
        BoundListener listener = bootstrap.boundListener();
        System.out.printf(
            "listener bind=%s port=%d table_group=%s data_readiness=UNIMPLEMENTED%n",
            listener.bindHost(), listener.port(), listener.tableGroupId());
        bootstrap.awaitTermination();
        return 0;
      } finally {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdown);
        } catch (IllegalStateException shuttingDown) {
          // The JVM shutdown hook owns cleanup after shutdown has begun.
        }
      }
    } catch (IOException | RuntimeException | InterruptedException failure) {
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.err.println("J03 gRPC skeleton failed: " + failure.getClass().getSimpleName());
      return 1;
    }
  }

  public static void main(String[] arguments) {
    int exitCode = run(arguments);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /** A declared binding only; it does not assert that a backend table group exists or is ready. */
  public record ListenerBinding(String bindHost, int port, String tableGroupId) {
    public ListenerBinding {
      if (bindHost == null || !Set.of("127.0.0.1", "::1", "localhost").contains(bindHost)) {
        throw new IllegalArgumentException("the J03 skeleton requires a loopback binding");
      }
      if (port < 0 || port > 65535) {
        throw new IllegalArgumentException("port must be between 0 and 65535");
      }
      if (tableGroupId == null || !tableGroupId.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")) {
        throw new IllegalArgumentException("table-group id must be a bounded identifier");
      }
      if ("localhost".equals(bindHost)) {
        bindHost = "127.0.0.1";
      }
    }
  }

  public record BoundListener(String bindHost, int port, String tableGroupId) {}
}
