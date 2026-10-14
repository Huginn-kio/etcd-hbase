package io.etcdhbase.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolSourceLockTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void vendoredProtoSourcesMatchTheLockedHashesAndInventory() throws Exception {
    Path sourceLockPath = Path.of(System.getProperty("protocol.source.lock", "upstream-lock.json"));
    Path protoRoot =
        Path.of(System.getProperty("protocol.proto.root", "src/main/proto"))
            .toAbsolutePath()
            .normalize();
    assertTrue(Files.isRegularFile(sourceLockPath), "missing protocol/upstream-lock.json");
    assertTrue(Files.isDirectory(protoRoot), "missing protocol/src/main/proto");

    JsonNode sourceLock = JSON.readTree(sourceLockPath.toFile());
    Set<String> lockedPaths = new TreeSet<>();
    for (JsonNode source : sourceLock.path("sources")) {
      for (JsonNode fileEntry : source.path("files")) {
        String relativeName = fileEntry.path("path").asText();
        Path relativePath = Path.of(relativeName);
        assertFalse(relativeName.isBlank() || relativePath.isAbsolute(), "invalid locked proto path");
        assertEquals(relativePath, relativePath.normalize(), "non-normalized locked proto path");
        Path sourcePath = protoRoot.resolve(relativePath).normalize();
        assertTrue(sourcePath.startsWith(protoRoot), "locked proto path escapes the source root");
        assertTrue(Files.isRegularFile(sourcePath), "missing locked proto source " + relativeName);
        assertTrue(lockedPaths.add(relativeName), "duplicate locked proto source " + relativeName);

        byte[] sourceBytes = Files.readAllBytes(sourcePath);
        String actualSha256 =
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(sourceBytes));
        assertEquals(fileEntry.path("sha256").asText(), actualSha256,
            "vendored proto source differs from its lock: " + relativeName);
      }
    }

    Set<String> actualPaths;
    try (Stream<Path> paths = Files.walk(protoRoot)) {
      actualPaths = paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".proto"))
          .map(protoRoot::relativize)
          .map(path -> path.toString().replace(path.getFileSystem().getSeparator(), "/"))
          .collect(Collectors.toCollection(TreeSet::new));
    }
    assertEquals(lockedPaths, actualPaths, "vendored proto inventory differs from the source lock");
  }
}
