package io.etcdhbase.admin;

import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import io.etcdhbase.admin.config.InitializationConfiguration.RecordCategory;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;

/** Resolves a validated, explicitly selected M3 candidate into its physical table bindings. */
public final class M3TableGroupPlan {
  private final TableGroupInitializationPlan source;
  private final SystemTableBinding system;
  private final StorageTableBinding primary;
  private final Optional<StorageTableBinding> current;

  private M3TableGroupPlan(TableGroupInitializationPlan source, SystemTableBinding system,
      StorageTableBinding primary, Optional<StorageTableBinding> current) {
    this.source = source;
    this.system = system;
    this.primary = primary;
    this.current = current;
  }

  public TableGroupInitializationPlan source() { return source; }
  public SystemTableBinding system() { return system; }
  public StorageTableBinding primary() { return primary; }
  public Optional<StorageTableBinding> current() { return current; }

  /** M0 candidates remain valid on their own; M3 needs explicit result and retention capability. */
  public static M3TableGroupPlan from(TableGroupInitializationPlan plan) {
    Objects.requireNonNull(plan, "plan");
    var profile = plan.saltProfile();
    var salt = new HashSaltStrategy(new SaltProfile(profile.strategyId(), profile.encodingVersion(),
        profile.bucketCount(), profile.saltWidthBytes()));
    TableSchemaCandidate systemTable = plan.tables().getFirst();
    if (!systemTable.recordCategories().contains(RecordCategory.RESULT)) {
      throw new IllegalArgumentException("M3 requires the system result-record capability");
    }
    var system = new SystemTableBinding(name(plan, systemTable), family(systemTable, true, false),
        plan.tableGroupId(), profile.id(), salt);
    List<TableSchemaCandidate> data = plan.tables().subList(1, plan.tables().size());
    if (plan.historyLayout() == HistoryLayout.SEPARATE_HISTORY_TABLE) {
      var historyTable = only(data, Set.of(RecordCategory.HISTORY));
      var currentTable = only(data, Set.of(RecordCategory.CURRENT));
      var history = new StorageTableBinding(name(plan, historyTable), family(historyTable, false, false),
          StorageTableBinding.Layout.HISTORY, salt);
      var current = new StorageTableBinding(name(plan, currentTable), family(currentTable, false, false),
          StorageTableBinding.Layout.CURRENT, salt);
      return new M3TableGroupPlan(plan, system, history, Optional.of(current));
    }
    if (plan.historyLayout() == HistoryLayout.NATIVE_CELL_VERSIONS) {
      var table = only(data, Set.of(RecordCategory.CURRENT, RecordCategory.HISTORY));
      var nativeLayout = new StorageTableBinding(name(plan, table), family(table, false, true),
          StorageTableBinding.Layout.NATIVE, salt);
      return new M3TableGroupPlan(plan, system, nativeLayout, Optional.empty());
    }
    throw new IllegalArgumentException("M3 requires an explicitly selected history layout");
  }

  private static TableSchemaCandidate only(List<TableSchemaCandidate> tables,
      Set<RecordCategory> categories) {
    List<TableSchemaCandidate> matches = tables.stream()
        .filter(table -> Set.copyOf(table.recordCategories()).equals(categories)).toList();
    if (matches.size() != 1) throw new IllegalArgumentException("M3 table categories are ambiguous");
    return matches.getFirst();
  }

  private static TableName name(TableGroupInitializationPlan plan, TableSchemaCandidate table) {
    return TableName.valueOf(plan.namespace(), table.tableName());
  }

  private static byte[] family(TableSchemaCandidate table, boolean system, boolean nativeVersions) {
    if (table.columnFamilies().size() != 1) {
      throw new IllegalArgumentException("M3 bindings require exactly one column family per table");
    }
    var family = table.columnFamilies().getFirst();
    if (family.ttlSeconds() != -1 || system && family.maxVersions() != 1
        || nativeVersions && family.maxVersions() < 2) {
      throw new IllegalArgumentException("M3 table retention does not protect its records");
    }
    return family.name().getBytes(StandardCharsets.UTF_8);
  }
}
