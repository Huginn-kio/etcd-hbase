package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;
import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

/** Offline plan and explicitly confirmed table provisioning; not etcd runtime bootstrap. */
public final class InitializeTablesCommand {
  private InitializeTablesCommand() {}

  public static void main(String[] args) {
    int result = run(args, System.out, System.err);
    if (result != 0) {
      System.exit(result);
    }
  }

  static int run(String[] args, PrintStream output, PrintStream error) {
    return run(args, output, error, InitializeTablesCommand::applyLocalDevelopment);
  }

  static int run(String[] args, PrintStream output, PrintStream error, ApplyOperation operation) {
    InitializationOptions options;
    TableGroupInitializationPlan plan;
    try {
      options = InitializationOptions.parse(args);
      try (var input = Files.newInputStream(options.config())) {
        var configuration = new InitializationConfigurationLoader().load(input);
        plan =
            TableGroupPlanFactory.create(configuration, options.group(), options.historyLayout());
      }
      if (options.action() == InitializationOptions.Action.APPLY) {
        TableGroupInitializer.confirm(plan, options.confirmation());
      }
    } catch (IOException unreadable) {
      error.println("m0-admin error: configuration cannot be read");
      return 2;
    } catch (IllegalArgumentException invalid) {
      error.println("m0-admin error: " + invalid.getMessage());
      return 2;
    }
    if (options.action() == InitializationOptions.Action.PLAN) {
      printPlan(plan, output);
      return 0;
    }
    return runApply(plan, operation, output, error);
  }

  @FunctionalInterface
  interface ApplyOperation {
    TableGroupInitializer.InitializationResult apply(TableGroupInitializationPlan plan)
        throws Exception;
  }

  private static int runApply(
      TableGroupInitializationPlan plan,
      ApplyOperation operation,
      PrintStream output,
      PrintStream error) {
    try {
      var result = operation.apply(plan);
      output.printf(
          "m0-admin apply namespace=%s table_group=%s history_layout=%s status=%s "
              + "created_tables=%s confirmation_fingerprint=sha256:%s%n",
          result.namespace(),
          plan.tableGroupId(),
          plan.historyLayout(),
          result.alreadyProvisioned() ? "already-provisioned" : "created",
          tableNames(result.createdTables()),
          result.planFingerprint());
      return 0;
    } catch (MissingIdentityInputException missing) {
      error.println("m0-admin missing-input: " + missing.getMessage());
      return 3;
    } catch (TableGroupInitializer.PartialInitializationException partial) {
      error.printf(
          "m0-admin partial: namespace=%s observed_created_tables=%s "
              + "confirmation_fingerprint=sha256:%s; inspect and rerun the same plan to continue%n",
          partial.namespace(), tableNames(partial.createdTables()), partial.planFingerprint());
      return 4;
    } catch (TableGroupInitializer.ConflictException conflict) {
      error.println("m0-admin conflict: " + conflict.getMessage());
      return 4;
    } catch (Exception failure) {
      var classified = HBaseFailures.classify(failure);
      error.println("m0-admin backend: " + classified.category() + ": " + classified.safeMessage());
      return 4;
    }
  }

  private static TableGroupInitializer.InitializationResult applyLocalDevelopment(
      TableGroupInitializationPlan plan) throws Exception {
    if (plan.hbaseProfile().authenticationMode() != AuthenticationMode.DEV_NO_AUTH
        || !plan.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID)) {
      throw new MissingIdentityInputException();
    }
    var configuration = localDevelopmentConfiguration(plan);
    var scope = new AuthenticationScope(plan.tableGroupId(), plan.hbaseProfile().id());
    // This explicit SIMPLE profile verifies reachability; secure callers supply a real J04 identity
    // to TableGroupInitializer.initialize. No credential mechanism is hardcoded into that API.
    try (var provider =
            new HBaseBackendIdentityProvider(
                plan.identityProviderId(),
                scope,
                configuration,
                HBaseAuthenticators.simpleDevelopment(),
                (connection, ignored) ->
                    connection.getAdmin().listNamespaces().thenApply(namespaces -> null),
                1,
                1,
                OPERATION_TIMEOUT);
        var credentials = new BackendCredentials("m0-initializer", new char[0])) {
      var identity =
          await(provider.authenticate(credentials, scope).toCompletableFuture(), OPERATION_TIMEOUT);
      return TableGroupInitializer.initialize(
          plan, "sha256:" + plan.fingerprint(), identity, provider);
    }
  }

  static Configuration localDevelopmentConfiguration(TableGroupInitializationPlan plan) {
    var configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", plan.hbaseProfile().quorum());
    configuration.set("hbase.client.zookeeper.quorum", plan.hbaseProfile().quorum());
    configuration.setInt("hbase.zookeeper.property.clientPort", plan.hbaseProfile().clientPort());
    configuration.setInt(
        "hbase.client.zookeeper.property.clientPort", plan.hbaseProfile().clientPort());
    configuration.set(
        "hbase.client.registry.impl", "org.apache.hadoop.hbase.client.ZKConnectionRegistry");
    configuration.set("hbase.security.authentication", "simple");
    configuration.set("hadoop.security.authentication", "simple");
    configuration.setBoolean("hbase.security.authorization", false);
    return configuration;
  }

  private static void printPlan(TableGroupInitializationPlan plan, PrintStream output) {
    output.println("M0 table provisioning candidate");
    output.println("table_group_id: " + plan.tableGroupId());
    output.println("identity_provider_id: " + plan.identityProviderId());
    output.println("hbase_profile_id: " + plan.hbaseProfile().id());
    output.println("namespace: " + plan.namespace());
    output.println("authentication_mode: " + plan.hbaseProfile().authenticationMode());
    output.println("schema_version: " + plan.schemaVersion());
    output.println("salt_profile_id: " + plan.saltProfile().id());
    output.println("salt_strategy_id: " + plan.saltProfile().strategyId());
    output.println("history_layout: " + plan.historyLayout());
    output.println("confirmation_fingerprint: sha256:" + plan.fingerprint());
    output.println("configured_namespace_tables: " + plan.configuredNamespaceTableNames());
    for (TableSchemaCandidate table : plan.tables()) {
      printTable(plan.namespace(), table, output);
    }
    output.println("backend_contacted: false");
  }

  private static void printTable(String namespace, TableSchemaCandidate table, PrintStream output) {
    output.println("table: " + namespace + ":" + table.tableName());
    output.println("  record_categories: " + table.recordCategories());
    for (var family : table.columnFamilies()) {
      output.printf(
          "  family: %s min_versions=%d max_versions=%d ttl_seconds=%d keep_deleted_cells=%s%n",
          family.name(),
          family.minVersions(),
          family.maxVersions(),
          family.ttlSeconds(),
          family.keepDeletedCells());
    }
    output.println("  split_keys_hex: " + table.splitKeysHex());
    output.println("  permission_intents: " + table.identityPermissions());
  }

  private static List<String> tableNames(List<TableName> tables) {
    return tables.stream().map(TableName::getNameAsString).toList();
  }

  private static final class MissingIdentityInputException extends Exception {
    private static final long serialVersionUID = 1L;

    private MissingIdentityInputException() {
      super(
          "public CLI activates explicit local development only; use an operator-supplied J04"
              + " identity for secure apply");
    }
  }
}
