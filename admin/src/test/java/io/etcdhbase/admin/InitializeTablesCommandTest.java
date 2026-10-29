package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InitializeTablesCommandTest {
  @TempDir private Path directory;

  @Test
  void printsBothOfflinePlansAndNoConnectionOrCredentialMaterial() throws Exception {
    var separate = run(planArguments(write("history-table.json")), forbiddenApply());
    var nativeVersions = run(planArguments(write("native-cell-versions.json")), forbiddenApply());
    assertEquals(0, separate.exitCode(), separate.error());
    assertEquals(0, nativeVersions.exitCode(), nativeVersions.error());
    assertTrue(separate.output().contains("history_layout: SEPARATE_HISTORY_TABLE"));
    assertTrue(separate.output().contains("m0_cli:latest"));
    assertTrue(nativeVersions.output().contains("m0_cli:kv"));
    assertTrue(separate.output().contains("backend_contacted: false"));
    assertNotEquals(fingerprint(separate.output()), fingerprint(nativeVersions.output()));
    assertFalse(separate.output().contains("127.0.0.1"));
    assertFalse(separate.output().contains("credential_reference"));
  }

  @Test
  void rejectsDuplicateUnknownMissingAndConflictingOptionsWithoutEchoingInputs() throws Exception {
    String config = write("history-table.json");
    for (String[] arguments :
        List.of(
            new String[] {"init", "plan", "--config", config, "--config", "private-marker"},
            new String[] {"init", "plan", "--private-marker", "value"},
            new String[] {"init", "plan"},
            new String[] {"init", "plan", "--config", config, "--history-layout", "private-marker"},
            new String[] {
              "init", "plan", "--config", config, "--history-layout", "NATIVE_CELL_VERSIONS"
            },
            new String[] {"init", "apply", "--config", config})) {
      var result = run(arguments, forbiddenApply());
      assertEquals(2, result.exitCode(), result.error());
      assertTrue(result.output().isEmpty());
      assertFalse(result.error().contains("private-marker"));
    }
  }

  @Test
  void rejectsAnIncorrectFingerprintBeforeTheApplyOperation() throws Exception {
    var result =
        run(
            applyArguments(write("history-table.json"), "sha256:" + "0".repeat(64)),
            forbiddenApply());
    assertEquals(2, result.exitCode());
    assertTrue(result.error().contains("--confirm-plan"));
    assertTrue(result.output().isEmpty());
  }

  @Test
  void confirmedApplyExecutesOnceAndReportsOnlyTheResultOfThatOperation() throws Exception {
    String config = write("history-table.json");
    String confirmation = fingerprint(run(planArguments(config), forbiddenApply()).output());
    var calls = new AtomicInteger();
    var result =
        run(
            applyArguments(config, confirmation),
            plan -> {
              calls.incrementAndGet();
              return new TableGroupInitializer.InitializationResult(
                  plan.namespace(),
                  plan.tables().stream()
                      .map(table -> TableName.valueOf(plan.namespace(), table.tableName()))
                      .toList(),
                  true,
                  false,
                  plan.fingerprint());
            });
    assertEquals(0, result.exitCode(), result.error());
    assertEquals(1, calls.get());
    assertTrue(result.output().contains("status=created"));
    assertTrue(result.output().contains(confirmation));
  }

  @Test
  void separatesConfigurationErrorsFromBackendFailuresWithoutPrintingBackendDiagnostics()
      throws Exception {
    var missing =
        run(planArguments(directory.resolve("private-marker.json").toString()), forbiddenApply());
    assertEquals(2, missing.exitCode());
    assertFalse(missing.error().contains("private-marker"));
    String config = write("history-table.json");
    String confirmation = fingerprint(run(planArguments(config), forbiddenApply()).output());
    var unavailable =
        run(
            applyArguments(config, confirmation),
            plan -> {
              throw new IOException("private-marker backend address and credential");
            });
    assertEquals(4, unavailable.exitCode());
    assertTrue(unavailable.error().contains("UNAVAILABLE"));
    assertFalse(unavailable.error().contains("private-marker"));
    assertTrue(unavailable.output().isEmpty());
  }

  @Test
  void leavesTargetCredentialActivationExplicitlyUnconfiguredInThePublicCli() throws Exception {
    ObjectNode tree = InitializationFixtures.example("history-table.json", "m0_cli");
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("authentication_mode", "CREDENTIAL_REFERENCE");
    profile.put("credential_reference", "secret://test/private-marker");
    profile.put("quorum", "private-marker.invalid");
    ((ObjectNode) tree.withArray("table_groups").get(0)).put("identity_provider_id", "custom-sasl");
    String config = write(tree, "secure.json");
    String confirmation = fingerprint(run(planArguments(config), forbiddenApply()).output());
    var output = new ByteArrayOutputStream();
    var error = new ByteArrayOutputStream();
    int exitCode =
        InitializeTablesCommand.run(
            applyArguments(config, confirmation), new PrintStream(output), new PrintStream(error));
    assertEquals(3, exitCode);
    assertTrue(output.toString(StandardCharsets.UTF_8).isEmpty());
    assertTrue(error.toString(StandardCharsets.UTF_8).contains("missing-input"));
    assertFalse(error.toString(StandardCharsets.UTF_8).contains("private-marker"));
  }

  private String write(String name) throws Exception {
    return write(InitializationFixtures.example(name, "m0_cli"), name);
  }

  private String write(ObjectNode tree, String name) throws Exception {
    Path path = directory.resolve(name);
    Files.write(path, new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(tree));
    return path.toString();
  }

  private static String[] planArguments(String config) {
    return new String[] {"init", "plan", "--config", config};
  }

  private static String[] applyArguments(String config, String fingerprint) {
    return new String[] {
      "init",
      "apply",
      "--config",
      config,
      "--confirm-table-group",
      "local",
      "--confirm-plan",
      fingerprint
    };
  }

  private static String fingerprint(String output) {
    return output
        .lines()
        .filter(line -> line.startsWith("confirmation_fingerprint: "))
        .findFirst()
        .orElseThrow()
        .substring("confirmation_fingerprint: ".length());
  }

  private static InitializeTablesCommand.ApplyOperation forbiddenApply() {
    return plan -> {
      throw new AssertionError("offline or invalid input must not contact HBase");
    };
  }

  private static CommandResult run(String[] args, InitializeTablesCommand.ApplyOperation apply) {
    var output = new ByteArrayOutputStream();
    var error = new ByteArrayOutputStream();
    int exitCode =
        InitializeTablesCommand.run(args, new PrintStream(output), new PrintStream(error), apply);
    return new CommandResult(
        exitCode, output.toString(StandardCharsets.UTF_8), error.toString(StandardCharsets.UTF_8));
  }

  private record CommandResult(int exitCode, String output, String error) {}
}
