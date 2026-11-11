package io.etcdhbase.hbase.probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HBaseMiniClusterReadinessTest {
  @Test
  void publishesOnlyCompleteRecords(@TempDir Path directory) throws Exception {
    String record = "M0_HBASE_CLUSTER_READY\t" + "x".repeat(1024 * 1024) + "\tversion\tjvm";
    try (var executor = Executors.newSingleThreadExecutor()) {
      for (int iteration = 0; iteration < 20; iteration++) {
        Path ready = directory.resolve("ready-" + iteration);
        var published = executor.submit(() -> {
          HBaseMiniClusterMain.publishReadiness(ready, record);
          return null;
        });
        observeCompleteRecordsUntilPublished(ready, record, published);
        published.get(10, TimeUnit.SECONDS);
        assertEquals(record, Files.readString(ready, StandardCharsets.UTF_8));
      }
    }
    try (var files = Files.list(directory)) {
      assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".pending")));
    }
  }

  private static void observeCompleteRecordsUntilPublished(Path ready, String record,
      Future<?> published) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!published.isDone() && System.nanoTime() < deadline) {
      if (Files.exists(ready)) {
        assertEquals(record, Files.readString(ready, StandardCharsets.UTF_8));
      }
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
    }
  }
}
