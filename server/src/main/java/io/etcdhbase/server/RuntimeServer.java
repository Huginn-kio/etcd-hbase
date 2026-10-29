package io.etcdhbase.server;

import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfigurationValidator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns all configured listeners and rolls back earlier binds when startup partially fails. */
public final class RuntimeServer implements AutoCloseable {
  private final List<Bootstrap> listeners;
  private final AtomicBoolean closed = new AtomicBoolean();

  private RuntimeServer(List<Bootstrap> listeners) {
    this.listeners = List.copyOf(listeners);
  }

  public static RuntimeServer start(RuntimeConfiguration configuration) throws IOException {
    RuntimeConfigurationValidator.validate(configuration, 1);
    requireDevelopmentActivation(configuration);
    List<Bootstrap> opened = new ArrayList<>();
    try {
      for (var listener : configuration.listeners()) {
        opened.add(Bootstrap.startConfigured(listener, configuration.resources()));
      }
      return new RuntimeServer(opened);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        closeAll(opened);
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static void requireDevelopmentActivation(RuntimeConfiguration configuration) {
    for (var listener : configuration.listeners()) {
      var group =
          configuration.tableGroups().stream()
              .filter(value -> value.id().equals(listener.tableGroupId()))
              .findFirst()
              .orElseThrow();
      var backend =
          configuration.hbaseProfiles().stream()
              .filter(value -> value.id().equals(group.hbaseProfileId()))
              .findFirst()
              .orElseThrow();
      if (backend.authenticationMode() != AuthenticationMode.DEV_NO_AUTH) {
        throw new IllegalArgumentException(
            "target identity and TLS activation are not configured in this skeleton");
      }
    }
  }

  public List<Bootstrap.BoundListener> boundListeners() {
    return listeners.stream().map(Bootstrap::boundListener).toList();
  }

  public void awaitTermination() throws InterruptedException {
    listeners.getFirst().awaitTermination();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) closeAll(listeners);
  }

  private static void closeAll(List<Bootstrap> opened) {
    Throwable first = null;
    for (var listener : opened.reversed()) {
      try {
        listener.close();
      } catch (RuntimeException | Error failure) {
        if (first == null) first = failure;
        else first.addSuppressed(failure);
      }
    }
    if (first instanceof Error fatal) throw fatal;
    if (first instanceof RuntimeException failure) throw failure;
  }

  static int run(String[] arguments) {
    if (arguments.length != 2 || !arguments[0].equals("--config")) {
      System.err.println("usage: runtime-server --config <json-file>");
      return 2;
    }
    try (var input = Files.newInputStream(Path.of(arguments[1]));
        RuntimeServer server = start(new RuntimeConfigurationLoader().load(input))) {
      Thread hook = new Thread(server::close, "runtime-server-shutdown");
      Runtime.getRuntime().addShutdownHook(hook);
      try {
        System.out.println(
            "listeners=" + server.boundListeners().size() + " data_readiness=UNIMPLEMENTED");
        server.awaitTermination();
        return 0;
      } finally {
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException shuttingDown) {
          /* The shutdown hook owns cleanup. */
        }
      }
    } catch (IllegalArgumentException failure) {
      System.err.println("runtime configuration or activation is invalid");
      return 2;
    } catch (IOException | InterruptedException | RuntimeException failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      System.err.println("runtime server failed");
      return 1;
    }
  }

  public static void main(String[] arguments) {
    int result = run(arguments);
    if (result != 0) System.exit(result);
  }
}
