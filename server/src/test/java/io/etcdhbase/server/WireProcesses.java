package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

final class WireProcesses {
  private WireProcesses() {}

  static Path repositoryRoot() throws IOException {
    for (Path path = Path.of("").toAbsolutePath(); path != null; path = path.getParent()) {
      if (Files.isRegularFile(path.resolve("scripts/dev/go-m0.sh"))) return path;
    }
    throw new IOException("testkit repository root is unavailable");
  }

  static Path artifacts(Path root) throws IOException {
    String configured = System.getenv("M0_WIRE_ARTIFACT_ROOT");
    Path path =
        configured == null
            ? root.resolve(".artifacts/m0/wire/" + UUID.randomUUID())
            : Path.of(configured).toAbsolutePath();
    Files.createDirectories(path);
    return path;
  }

  static Path driver(Path root, Path artifacts) throws Exception {
    String configured = System.getenv("M0_WIRE_DRIVER");
    if (configured != null) {
      Path driver = Path.of(configured).toAbsolutePath();
      assertTrue(Files.isExecutable(driver), "configured driver is not executable");
      return driver;
    }
    Path driver = artifacts.resolve("m0driver");
    run(
        new ProcessBuilder(
                root.resolve("scripts/dev/go-m0.sh").toString(),
                "build",
                "-trimpath",
                "-o",
                driver.toString(),
                "./cmd/m0driver")
            .directory(root.toFile()),
        artifacts.resolve("go-build.log"),
        Duration.ofMinutes(5));
    return driver;
  }

  static void run(ProcessBuilder builder, Path log, Duration timeout) throws Exception {
    Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(
          process.waitFor(timeout.toSeconds(), TimeUnit.SECONDS), "test process exceeded deadline");
      assertEquals(0, process.exitValue(), () -> readLog(log));
    } finally {
      if (process.isAlive()) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "owned test process did not stop");
      }
    }
  }

  private static String readLog(Path path) {
    try {
      return Files.readString(path);
    } catch (IOException failure) {
      return "test process log is unavailable";
    }
  }
}
