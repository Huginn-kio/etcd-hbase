package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;
import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.config.DevelopmentHBaseConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Objects;
import org.apache.hadoop.security.UserGroupInformation;

/** Explicitly plans or initializes a fresh local-development M3 table group. */
public final class M3InitializeTablesCommand {
  private M3InitializeTablesCommand() {}

  public static void main(String[] args) {
    int result = run(args, System.out, System.err);
    if (result != 0) System.exit(result);
  }

  static int run(String[] args, PrintStream output, PrintStream error) {
    return run(args, output, error, M3InitializeTablesCommand::applyLocalDevelopment);
  }

  @FunctionalInterface
  interface ApplyOperation {
    M3TableGroupInitializer.InitializationResult apply(M3TableGroupPlan plan,
        SnapshotPinConfiguration pins) throws Exception;
  }

  static int run(String[] args, PrintStream output, PrintStream error, ApplyOperation operation) {
    Objects.requireNonNull(output, "output");
    Objects.requireNonNull(error, "error");
    Objects.requireNonNull(operation, "operation");
    PreparedPlan prepared;
    try {
      prepared = loadAndConfirmPlan(args);
    } catch (IOException unreadable) {
      error.println("m3-admin error: configuration cannot be read");
      return 2;
    } catch (IllegalArgumentException invalid) {
      error.println("m3-admin error: " + invalid.getMessage());
      return 2;
    }
    if (prepared.command().options().action() == InitializationOptions.Action.PLAN) {
      renderPlan(prepared, output);
      return 0;
    }
    return applyPlan(prepared, operation, output, error);
  }

  private static PreparedPlan loadAndConfirmPlan(String[] args) throws IOException {
    CommandOptions command;
    M3TableGroupPlan plan;
    command = parse(args);
    try (var input = Files.newInputStream(command.options().config())) {
      var configuration = new InitializationConfigurationLoader().load(input);
      plan = M3TableGroupPlan.from(TableGroupPlanFactory.create(configuration,
          command.options().group(), command.options().historyLayout()));
    }
    String fingerprint = fingerprint(plan, command.pins());
    if (command.options().action() == InitializationOptions.Action.APPLY
        && !fingerprint.equals(command.options().confirmation())) {
      throw new IllegalArgumentException("confirmation does not match the M3 plan and pin registry");
    }
    return new PreparedPlan(command, plan, fingerprint);
  }

  private static void renderPlan(PreparedPlan prepared, PrintStream output) {
    var plan = prepared.plan();
    var pins = prepared.command().pins();
    output.println("M3 fresh table-group initialization");
    output.println("table_group_id: " + plan.source().tableGroupId());
    output.println("identity_provider_id: " + plan.source().identityProviderId());
    output.println("hbase_profile_id: " + plan.source().hbaseProfile().id());
    output.println("namespace: " + plan.source().namespace());
    output.println("authentication_mode: " + plan.source().hbaseProfile().authenticationMode());
    output.println("schema_version: " + plan.source().schemaVersion());
    output.println("salt_profile_id: " + plan.source().saltProfile().id());
    output.println("salt_strategy_id: " + plan.source().saltProfile().strategyId());
    output.println("history_layout: " + plan.source().historyLayout());
    output.println("configured_namespace_tables: " + plan.source().configuredNamespaceTableNames());
    for (var table : plan.source().tables()) {
      InitializeTablesCommand.printTable(plan.source().namespace(), table, output);
    }
    output.printf("pin_registry: %d:%d:%d%n", pins.shardCount(),
        pins.slotsPerShard(), pins.leaseMillis());
    output.println("confirmation_fingerprint: " + prepared.fingerprint());
    output.println("backend_contacted: false");
  }

  private static int applyPlan(PreparedPlan prepared, ApplyOperation operation,
      PrintStream output, PrintStream error) {
    var plan = prepared.plan();
    var pins = prepared.command().pins();
    try {
      var result = operation.apply(plan, pins);
      output.printf("m3-admin created table_group=%s tables=%s cluster_id=%s%n",
          plan.source().tableGroupId(), result.tables(), Long.toUnsignedString(result.clusterId()));
      return 0;
    } catch (MissingIdentityInputException missing) {
      error.println("m3-admin missing-input: target identity activation is not configured");
      return 3;
    } catch (M3TableGroupInitializer.TableCreationException partial) {
      error.printf("m3-admin partial: observed_created_tables=%s; outcome may be unknown; "
          + "inspect before retry%n", partial.observedCreated());
      return 4;
    } catch (M3TableGroupInitializer.MetadataInitializationException partial) {
      error.printf("m3-admin partial: created_tables=%s; metadata outcome may be unknown; "
          + "inspect before retry%n", partial.createdTables());
      return 4;
    } catch (IllegalArgumentException conflict) {
      error.println("m3-admin conflict: fresh table preconditions failed; inspect before retry");
      return 4;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      error.println("m3-admin backend: interrupted; outcome may be unknown; inspect before retry");
      return 4;
    } catch (Exception failure) {
      var classified = HBaseFailures.classify(failure);
      error.println("m3-admin backend: " + classified.category() + ": " + classified.safeMessage());
      return 4;
    }
  }

  private static CommandOptions parse(String[] args) {
    if (args == null) throw new IllegalArgumentException("missing initialization options");
    var filtered = new ArrayList<String>();
    String pinRegistry = null;
    for (int index = 0; index < args.length; index++) {
      if ("--pin-registry".equals(args[index])) {
        if (pinRegistry != null || ++index >= args.length || args[index] == null) {
          throw new IllegalArgumentException("provide exactly one pin registry");
        }
        pinRegistry = args[index];
      } else {
        filtered.add(args[index]);
      }
    }
    var options = InitializationOptions.parse(filtered.toArray(String[]::new));
    if (pinRegistry == null || !pinRegistry.matches("[0-9]{1,3}:[0-9]{1,3}:[0-9]{1,6}")) {
      throw new IllegalArgumentException("provide --pin-registry SHARDS:SLOTS:LEASE_MS");
    }
    String[] values = pinRegistry.split(":", -1);
    var pins = new SnapshotPinConfiguration(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
        Duration.ofMillis(Integer.parseInt(values[2])));
    return new CommandOptions(options, pins);
  }

  private static String fingerprint(M3TableGroupPlan plan, SnapshotPinConfiguration pins) {
    String canonical = "etcd-hbase/m3-initialize/v1\n" + plan.source().fingerprint() + "\n"
        + pins.shardCount() + ":" + pins.slotsPerShard() + ":" + pins.leaseMillis() + "\n";
    try {
      return "sha256:" + HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static M3TableGroupInitializer.InitializationResult applyLocalDevelopment(
      M3TableGroupPlan plan, SnapshotPinConfiguration pins) throws Exception {
    var source = plan.source();
    if (source.hbaseProfile().authenticationMode() != AuthenticationMode.DEV_NO_AUTH
        || !source.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID)) {
      throw new MissingIdentityInputException();
    }
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(source.identityProviderId(), scope,
            DevelopmentHBaseConfiguration.create(source.hbaseProfile()), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, OPERATION_TIMEOUT);
         var credentials = new BackendCredentials(UserGroupInformation.getCurrentUser().getShortUserName(),
             new char[0])) {
      var identity = await(provider.authenticate(credentials, scope).toCompletableFuture(), OPERATION_TIMEOUT);
      return M3TableGroupInitializer.initializeFresh(plan, "sha256:" + source.fingerprint(),
          identity, provider, pins);
    }
  }

  private record CommandOptions(InitializationOptions options, SnapshotPinConfiguration pins) {}

  private record PreparedPlan(CommandOptions command, M3TableGroupPlan plan, String fingerprint) {}

  private static final class MissingIdentityInputException extends Exception {
    private static final long serialVersionUID = 1L;
  }
}
