package io.etcdhbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/** Persists explicitly requested local fixture evidence; it is not a target acceptance report. */
final class M3HistoryEvidence {
  private M3HistoryEvidence() {}

  static void save(Path history, String name, Map<String, ?> details) throws Exception {
    String configured = System.getenv("M3_HISTORY_ARTIFACT_ROOT");
    if (configured == null || configured.isBlank()) return;
    if (!name.matches("[a-zA-Z0-9._-]{1,100}")) throw new IllegalArgumentException("invalid evidence name");
    Path allowed = WireProcesses.repositoryRoot().resolve(".artifacts").toRealPath();
    Path directory = Path.of(configured).toRealPath();
    if (directory.equals(allowed) || !directory.startsWith(allowed)) {
      throw new IllegalArgumentException("history evidence must be under the local artifacts directory");
    }
    String source = System.getenv("M3_SOURCE_COMMIT");
    if (source != null && !source.matches("[0-9a-f]{40}")) throw new IllegalArgumentException("invalid source commit");
    var metadata = new LinkedHashMap<String, Object>();
    metadata.put("scope", "public-local-minicluster");
    metadata.put("authentication", "simple-development");
    metadata.put("source_commit", source == null ? "unbound" : source);
    metadata.putAll(details);
    Files.copy(history, directory.resolve(name + "-history.json"));
    Files.writeString(directory.resolve(name + "-fixture.json"), new ObjectMapper().writeValueAsString(metadata) + "\n",
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
  }
}
