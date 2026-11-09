package io.etcdhbase.admin;

import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/** Creates only new M3 tables; an existing or ambiguous table is never adopted. */
final class M3FreshTableCreator {
  private static final String STORAGE_ATTRIBUTE_PREFIX = "etcdhbase.storage.";

  private M3FreshTableCreator() {}

  static FreshTables create(M3TableGroupPlan plan, TableGroupAdmin admin) throws Exception {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(admin, "admin");
    String clusterId = admin.clusterId();
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalStateException("backend cluster identity is unavailable");
    }
    List<PlannedHBaseTable> planned = plan.source().tables().stream()
        .map(table -> planned(plan, table, clusterId)).toList();
    ensureNamespace(plan.source().namespace(), admin);
    List<TableName> existing = admin.listTableNames(plan.source().namespace());
    Set<TableName> permitted = plan.source().configuredNamespaceTableNames().stream()
        .map(name -> TableName.valueOf(plan.source().namespace(), name)).collect(Collectors.toUnmodifiableSet());
    if (existing.stream().anyMatch(table -> !permitted.contains(table))) {
      throw new IllegalArgumentException("namespace contains a table outside the configured scope");
    }
    Set<TableName> target = planned.stream().map(table -> table.descriptor().getTableName())
        .collect(Collectors.toUnmodifiableSet());
    if (existing.stream().anyMatch(target::contains)) {
      throw new IllegalArgumentException("M3 initialization requires fresh tables");
    }
    List<TableName> created = new ArrayList<>();
    try {
      for (PlannedHBaseTable table : planned) {
        admin.createTable(table.descriptor(), table.splitKeys());
        created.add(table.descriptor().getTableName());
      }
      for (PlannedHBaseTable table : planned) {
        TableName name = table.descriptor().getTableName();
        if (!table.matches(admin.getDescriptor(name))
            || !table.matchesInitialRegions(admin.getRegions(name)) || !admin.isEmpty(name)) {
          throw new IllegalStateException("new M3 table failed its schema, split, or emptiness check");
        }
      }
      return new FreshTables(created, plan.source().fingerprint());
    } catch (Exception failure) {
      // The last create may have succeeded despite a lost acknowledgement. Never roll back or
      // infer freshness from the observed list; a subsequent attempt must refuse existing tables.
      throw new PartialCreationException(created, failure);
    }
  }

  private static void ensureNamespace(String namespace, TableGroupAdmin admin) throws Exception {
    if (admin.listNamespaces().contains(namespace)) return;
    try {
      admin.createNamespace(namespace);
    } catch (NamespaceExistException concurrent) {
      // The table-scope and freshness checks below still run after concurrent namespace creation.
    }
  }

  private static PlannedHBaseTable planned(M3TableGroupPlan plan,
      TableSchemaCandidate table, String clusterId) {
    PlannedHBaseTable base = PlannedHBaseTable.from(plan.source(), table, clusterId);
    TableName name = base.descriptor().getTableName();
    StorageTableBinding binding = name.equals(plan.primary().table()) ? plan.primary()
        : plan.current().filter(value -> value.table().equals(name)).orElse(null);
    if (binding == null) {
      if (!name.equals(plan.system().table())) throw new IllegalArgumentException("unexpected M3 table");
      return base;
    }
    var descriptor = TableDescriptorBuilder.newBuilder(base.descriptor());
    binding.descriptor().getValues().forEach((key, value) -> {
      if (Bytes.toString(key.copyBytes()).startsWith(STORAGE_ATTRIBUTE_PREFIX)) {
        descriptor.setValue(key, value);
      }
    });
    return new PlannedHBaseTable(descriptor.build(), base.splitKeys());
  }

  record FreshTables(List<TableName> tables, String planFingerprint) {
    FreshTables {
      tables = List.copyOf(tables);
      Objects.requireNonNull(planFingerprint, "planFingerprint");
    }
  }

  static final class PartialCreationException extends Exception {
    private static final long serialVersionUID = 1L;
    private final List<TableName> observedCreated;

    private PartialCreationException(List<TableName> observedCreated, Throwable cause) {
      super("M3 table creation is incomplete or its outcome is unknown", cause);
      this.observedCreated = List.copyOf(observedCreated);
    }

    List<TableName> observedCreated() { return observedCreated; }
  }
}
