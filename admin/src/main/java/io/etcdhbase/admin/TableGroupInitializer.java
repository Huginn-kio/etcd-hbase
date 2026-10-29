package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;

import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;

/** Provisions only confirmed schemas. It never writes bootstrap rows, alters or deletes tables. */
public final class TableGroupInitializer {
  private TableGroupInitializer() {}

  /** Uses any authorized J04 identity. The caller owns the provider and its borrowed connection. */
  public static InitializationResult initialize(
      TableGroupInitializationPlan plan,
      String confirmation,
      AuthorizedBackendIdentity identity,
      HBaseConnectionFactory connections)
      throws Exception {
    Objects.requireNonNull(plan, "plan");
    confirm(plan, confirmation);
    Objects.requireNonNull(identity, "identity");
    var scope = new AuthenticationScope(plan.tableGroupId(), plan.hbaseProfile().id());
    if (!scope.equals(identity.scope())
        || !plan.identityProviderId().equals(identity.providerId())) {
      throw new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION);
    }
    var connection =
        await(connections.connectionFor(identity).toCompletableFuture(), OPERATION_TIMEOUT);
    return initialize(plan, new AsyncTableGroupAdmin(connection));
  }

  static void confirm(TableGroupInitializationPlan plan, String confirmation) {
    if (!("sha256:" + plan.fingerprint()).equals(confirmation)) {
      throw new IllegalArgumentException(
          "--confirm-plan does not match the current plan fingerprint");
    }
  }

  static InitializationResult initialize(TableGroupInitializationPlan plan, TableGroupAdmin admin)
      throws Exception {
    String clusterId = admin.clusterId();
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalStateException("backend cluster identity is unavailable");
    }
    List<PlannedHBaseTable> planned =
        plan.tables().stream()
            .map(table -> PlannedHBaseTable.from(plan, table, clusterId))
            .toList();
    boolean namespaceCreated = ensureNamespace(plan.namespace(), admin);
    List<TableName> existingNames = admin.listTableNames(plan.namespace());
    verifyNamespaceScope(plan, existingNames);
    List<PlannedHBaseTable> existing =
        planned.stream()
            .filter(table -> existingNames.contains(table.descriptor().getTableName()))
            .toList();
    for (PlannedHBaseTable table : existing) {
      verifyDescriptor(table, admin);
    }
    if (existing.size() == planned.size()) {
      // No writes: a completed group may contain runtime data and operational region splits.
      return new InitializationResult(
          plan.namespace(), List.of(), namespaceCreated, true, plan.fingerprint());
    }
    for (PlannedHBaseTable table : existing) {
      verifyResumable(table, admin);
    }
    List<TableName> created = createMissingTables(plan, planned, existingNames, admin);
    return new InitializationResult(
        plan.namespace(), created, namespaceCreated, created.isEmpty(), plan.fingerprint());
  }

  private static boolean ensureNamespace(String namespace, TableGroupAdmin admin) throws Exception {
    if (admin.listNamespaces().contains(namespace)) {
      return false;
    }
    try {
      admin.createNamespace(namespace);
      return true;
    } catch (NamespaceExistException concurrentCreation) {
      return false;
    }
  }

  private static void verifyNamespaceScope(
      TableGroupInitializationPlan plan, List<TableName> actual) {
    Set<TableName> permitted =
        plan.configuredNamespaceTableNames().stream()
            .map(name -> TableName.valueOf(plan.namespace(), name))
            .collect(Collectors.toUnmodifiableSet());
    if (actual.stream().anyMatch(name -> !permitted.contains(name))) {
      throw new IllegalArgumentException(
          "namespace contains tables outside the configured table scope");
    }
  }

  private static void verifyDescriptor(PlannedHBaseTable table, TableGroupAdmin admin)
      throws Exception {
    if (!table.matches(admin.getDescriptor(table.descriptor().getTableName()))) {
      throw new IllegalArgumentException(
          "an existing table does not match the confirmed schema and profile");
    }
  }

  private static void verifyResumable(PlannedHBaseTable table, TableGroupAdmin admin)
      throws Exception {
    TableName name = table.descriptor().getTableName();
    if (!table.matchesInitialRegions(admin.getRegions(name))) {
      throw new IllegalArgumentException("an existing table has different region split boundaries");
    }
    if (!admin.isEmpty(name)) {
      throw new IllegalArgumentException(
          "a partial table group contains data; refusing to add tables");
    }
  }

  private static List<TableName> createMissingTables(
      TableGroupInitializationPlan plan,
      List<PlannedHBaseTable> tables,
      List<TableName> existing,
      TableGroupAdmin admin)
      throws Exception {
    List<TableName> created = new ArrayList<>();
    try {
      for (PlannedHBaseTable table : tables) {
        TableName name = table.descriptor().getTableName();
        if (existing.contains(name)) {
          continue;
        }
        try {
          admin.createTable(table.descriptor(), table.splitKeys());
          created.add(name);
        } catch (TableExistsException concurrentCreation) {
          verifyDescriptor(table, admin);
          verifyResumable(table, admin);
        }
      }
    } catch (Exception failed) {
      // A create procedure can finish after a timeout; this is the observed list, not a rollback.
      throw new PartialInitializationException(
          plan.namespace(), created, plan.fingerprint(), failed);
    }
    return List.copyOf(created);
  }

  public record InitializationResult(
      String namespace,
      List<TableName> createdTables,
      boolean namespaceCreated,
      boolean alreadyProvisioned,
      String planFingerprint) {
    public InitializationResult {
      createdTables = List.copyOf(createdTables);
    }
  }

  public static final class PartialInitializationException extends Exception {
    private static final long serialVersionUID = 1L;
    private final String namespace;
    private final List<TableName> createdTables;
    private final String planFingerprint;

    private PartialInitializationException(
        String namespace, List<TableName> createdTables, String planFingerprint, Throwable cause) {
      super("table creation failed; rerun the same confirmed plan to continue", cause);
      this.namespace = namespace;
      this.createdTables = List.copyOf(createdTables);
      this.planFingerprint = planFingerprint;
    }

    public String namespace() {
      return namespace;
    }

    public List<TableName> createdTables() {
      return createdTables;
    }

    public String planFingerprint() {
      return planFingerprint;
    }
  }
}
