package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3InitializeTablesCommandTest {
  @TempDir Path directory;

  @Test
  void confirmationCoversBothPhysicalPlanAndPinRegistry() throws Exception {
    Path configuration = configuration();
    var calls = new AtomicInteger();
    M3InitializeTablesCommand.ApplyOperation apply = (plan, pins) -> {
      calls.incrementAndGet();
      assertEquals(2, pins.shardCount());
      return new M3TableGroupInitializer.InitializationResult(List.of(), plan.source().fingerprint(), 1);
    };
    var planned = run(new String[] {"init", "plan", "--config", configuration.toString(),
        "--table-group", "local", "--pin-registry", "2:16:60000"}, apply);
    assertEquals(0, planned.status());
    assertTrue(planned.output().contains("backend_contacted: false"));
    assertTrue(planned.output().contains("salt_strategy_id: crc32-bucket-v1"));
    assertTrue(planned.output().contains("split_keys_hex:"));
    assertTrue(planned.output().contains("family:"));
    String confirmation = planned.output().lines()
        .filter(line -> line.startsWith("confirmation_fingerprint: "))
        .findFirst().orElseThrow().substring("confirmation_fingerprint: ".length());

    var altered = run(new String[] {"init", "apply", "--config", configuration.toString(),
        "--confirm-table-group", "local", "--confirm-plan", confirmation,
        "--pin-registry", "3:16:60000"}, apply);
    assertEquals(2, altered.status());
    assertEquals(0, calls.get());
    assertFalse(altered.error().contains(configuration.toString()));

    var accepted = run(new String[] {"init", "apply", "--config", configuration.toString(),
        "--confirm-table-group", "local", "--confirm-plan", confirmation,
        "--pin-registry", "2:16:60000"}, apply);
    assertEquals(0, accepted.status());
    assertEquals(1, calls.get());
  }

  @Test
  void rejectsMissingOrInvalidPinRegistryBeforeBackendContact() throws Exception {
    Path configuration = configuration();
    M3InitializeTablesCommand.ApplyOperation forbidden = (plan, pins) -> {
      throw new AssertionError("backend must not be contacted");
    };
    assertEquals(2, run(new String[] {"init", "plan", "--config", configuration.toString()}, forbidden).status());
    assertEquals(2, run(new String[] {"init", "plan", "--config", configuration.toString(),
        "--pin-registry", "2:16:0"}, forbidden).status());
    assertEquals(2, run(new String[] {"init", "plan", "--config", configuration.toString(),
        "--pin-registry", "2:16:60000", "--pin-registry", "2:16:60000"}, forbidden).status());
  }

  private Path configuration() throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "j05", "native-cell-versions.json")));
    ((ObjectNode) tree.withArray("table_groups").get(0).get("system_table"))
        .withArray("record_categories").add("RESULT");
    Path path = directory.resolve("m3-init.json");
    Files.write(path, mapper.writeValueAsBytes(tree));
    return path;
  }

  private static Result run(String[] args, M3InitializeTablesCommand.ApplyOperation apply) {
    var output = new ByteArrayOutputStream();
    var error = new ByteArrayOutputStream();
    int status = M3InitializeTablesCommand.run(args, new PrintStream(output), new PrintStream(error), apply);
    return new Result(status, output.toString(), error.toString());
  }

  private record Result(int status, String output, String error) {}
}
