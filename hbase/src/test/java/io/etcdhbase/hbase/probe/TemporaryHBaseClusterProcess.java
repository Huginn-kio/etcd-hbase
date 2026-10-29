package io.etcdhbase.hbase.probe;

import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/** Starts the unshaded HBase testing server in its own JVM, isolated from the shaded client. */
public final class TemporaryHBaseClusterProcess implements AutoCloseable {
  private static final String READY = "M0_HBASE_CLUSTER_READY";
  private static final long START_TIMEOUT_SECONDS = 240;
  private static final long CLIENT_READY_TIMEOUT_SECONDS = 60;
  private static final long CLIENT_READY_ATTEMPT_TIMEOUT_SECONDS = 15;
  private static final long STOP_TIMEOUT_SECONDS = 60;

  private final Process process;
  private final Path directory;
  private final Path configurationFile;
  private final Path logFile;
  private final String hbaseVersion;
  private final String jvmVersion;
  private final List<CompletableFuture<Void>> abandonedCreations = new ArrayList<>();
  private String previousKerberosConfiguration;
  private boolean ownsKerberosConfiguration;

  private TemporaryHBaseClusterProcess(
      Process process,
      Path directory,
      Path configurationFile,
      Path logFile,
      String hbaseVersion,
      String jvmVersion) {
    this.process = process;
    this.directory = directory;
    this.configurationFile = configurationFile;
    this.logFile = logFile;
    this.hbaseVersion = hbaseVersion;
    this.jvmVersion = jvmVersion;
  }

  public static TemporaryHBaseClusterProcess start() throws Exception {
    return start(false);
  }

  public static TemporaryHBaseClusterProcess startSecure() throws Exception {
    return start(true);
  }

  private static TemporaryHBaseClusterProcess start(boolean secure) throws Exception {
    Path directory = Files.createTempDirectory(Path.of("target"), "m0-hbase-cluster-");
    Path configurationFile = directory.resolve("hbase-site.xml");
    Path readinessFile = directory.resolve("ready.txt");
    Path failureFile = directory.resolve("startup-failure.txt");
    Path logFile = directory.resolve("server.log");
    List<String> command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                unshadedServerClasspath(),
                HBaseMiniClusterMain.class.getName(),
                configurationFile.toString(),
                readinessFile.toString(),
                failureFile.toString()));
    if (secure) {
      command.add("kerberos");
    }
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(logFile.toFile())
            .start();

    String readyLine;
    try {
      readyLine = waitForReadiness(process, readinessFile, failureFile, logFile);
    } catch (Exception startupFailure) {
      process.destroyForcibly();
      process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      throw new IOException(
          "temporary HBase cluster did not start; server log is " + logFile, startupFailure);
    }

    String[] fields = readyLine.split("\t", -1);
    if (fields.length != 4 || !fields[0].equals("M0_HBASE_CLUSTER_READY")) {
      process.destroyForcibly();
      throw new IOException("temporary HBase cluster returned an invalid readiness record");
    }
    TemporaryHBaseClusterProcess cluster =
        new TemporaryHBaseClusterProcess(
            process, directory, configurationFile, logFile, fields[2], fields[3]);
    try {
      // cluster.start() completing does not prove that the first client registry RPC is ready.
      if (secure) {
        cluster.previousKerberosConfiguration = System.getProperty("java.security.krb5.conf");
        cluster.ownsKerberosConfiguration = true;
        Configuration client = cluster.clientConfiguration();
        System.setProperty(
            "java.security.krb5.conf", client.get(TemporaryKerberosSecurity.PREFIX + "krb5"));
        try (var user =
            KerberosUserContext.fromKeytab(
                "admin@" + client.get(TemporaryKerberosSecurity.PREFIX + "realm"),
                Path.of(client.get(TemporaryKerberosSecurity.PREFIX + "admin.keytab")))) {
          cluster.waitForClientReadiness(user);
        }
      } else {
        cluster.waitForClientReadiness(null);
      }
      return cluster;
    } catch (Exception readinessFailure) {
      cluster.restoreKerberosConfiguration();
      process.destroyForcibly();
      try {
        process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        readinessFailure.addSuppressed(interrupted);
        Thread.currentThread().interrupt();
      }
      throw new IOException(
          "temporary HBase cluster did not become ready to clients; server log is " + logFile,
          readinessFailure);
    }
  }

  public Configuration clientConfiguration() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(configurationFile.toUri()));
    return configuration;
  }

  private void waitForClientReadiness(HBaseUserContext user)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLIENT_READY_TIMEOUT_SECONDS);
    Throwable lastFailure = null;
    while (System.nanoTime() < deadline) {
      CompletableFuture<AsyncConnection> creation =
          user == null
              ? ConnectionFactory.createAsyncConnection(clientConfiguration())
              : ConnectionFactory.createAsyncConnection(clientConfiguration(), user.user());
      try {
        try (AsyncConnection connection =
            creation.get(CLIENT_READY_ATTEMPT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          connection
              .getAdmin()
              .listNamespaces()
              .get(CLIENT_READY_ATTEMPT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          return;
        }
      } catch (InterruptedException interrupted) {
        closeUnclaimedConnection(creation);
        Thread.currentThread().interrupt();
        throw interrupted;
      } catch (ExecutionException | TimeoutException | IOException readinessFailure) {
        lastFailure = readinessFailure;
        if (readinessFailure instanceof TimeoutException) {
          closeUnclaimedConnection(creation);
        }
      }

      if (!process.isAlive()) {
        throw new IOException(
            "temporary HBase cluster exited before client readiness with status "
                + process.exitValue()
                + "; server log is "
                + logFile,
            lastFailure);
      }
      Thread.sleep(250);
    }
    throw new IOException(
        "timed out waiting for HBase client readiness; server log is " + logFile, lastFailure);
  }

  String hbaseVersion() {
    return hbaseVersion;
  }

  private void closeUnclaimedConnection(CompletableFuture<AsyncConnection> creation) {
    // Cancelling HBase's creation future can discard a late connection without closing it.
    abandonedCreations.add(
        creation.handle(
            (connection, failure) -> {
              if (connection == null) {
                return null;
              }
              try {
                connection.close();
              } catch (IOException closeFailure) {
                throw new java.io.UncheckedIOException(
                    "failed to close a late probe connection", closeFailure);
              }
              return null;
            }));
  }

  String jvmVersion() {
    return jvmVersion;
  }

  @Override
  public void close() throws Exception {
    try {
      CompletableFuture.allOf(abandonedCreations.toArray(CompletableFuture[]::new))
          .get(10, TimeUnit.SECONDS);
      process.getOutputStream().write("STOP\n".getBytes(StandardCharsets.US_ASCII));
      process.getOutputStream().flush();
      if (!process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
        throw new IOException("temporary HBase cluster did not stop; server log is " + logFile);
      }
      if (process.exitValue() != 0) {
        throw new IOException(
            "temporary HBase cluster exited with status "
                + process.exitValue()
                + "; server log is "
                + logFile);
      }
    } finally {
      restoreKerberosConfiguration();
      if (process.isAlive()) {
        process.destroyForcibly();
      }
      try (var paths = Files.walk(directory)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  private void restoreKerberosConfiguration() {
    if (ownsKerberosConfiguration) {
      if (previousKerberosConfiguration == null) {
        System.clearProperty("java.security.krb5.conf");
      } else {
        System.setProperty("java.security.krb5.conf", previousKerberosConfiguration);
      }
      ownsKerberosConfiguration = false;
    }
  }

  private static String unshadedServerClasspath() {
    String[] entries = System.getProperty("java.class.path", "").split(java.io.File.pathSeparator);
    List<String> isolated = new ArrayList<>(entries.length);
    for (String entry : entries) {
      Path path = Path.of(entry);
      String name = path.getFileName() == null ? "" : path.getFileName().toString();
      if (!name.startsWith("hbase-shaded-client-byo-hadoop-")) {
        isolated.add(entry);
      }
    }
    return String.join(java.io.File.pathSeparator, isolated);
  }

  private static String waitForReadiness(
      Process process, Path readinessFile, Path failureFile, Path logFile)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(START_TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      if (Files.exists(readinessFile)) {
        return Files.readString(readinessFile, StandardCharsets.UTF_8).strip();
      }
      if (Files.exists(failureFile)) {
        throw new IOException(
            "temporary HBase cluster startup failed: "
                + Files.readString(failureFile, StandardCharsets.UTF_8)
                + "; server log is "
                + logFile);
      }
      if (!process.isAlive()) {
        throw new IOException(
            "temporary HBase cluster exited before readiness with status "
                + process.exitValue()
                + "; server log is "
                + logFile);
      }
      Thread.sleep(100);
    }
    throw new IOException(
        "timed out waiting for temporary HBase cluster; server log is " + logFile);
  }
}
