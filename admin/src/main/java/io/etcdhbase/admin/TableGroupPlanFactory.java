package io.etcdhbase.admin;

import io.etcdhbase.admin.config.HBaseBackendScope;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.TableGroup;
import io.etcdhbase.admin.config.InitializationConfiguration.TableSchemaCandidate;
import io.etcdhbase.admin.config.InitializationConfigurationValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Resolves one table group and an explicitly configured or requested physical-layout candidate. */
public final class TableGroupPlanFactory {
  private TableGroupPlanFactory() {}

  public static TableGroupInitializationPlan create(
      InitializationConfiguration configuration,
      String requestedGroup,
      HistoryLayout requestedLayout) {
    InitializationConfigurationValidator.validate(configuration);
    TableGroup group = selectGroup(configuration.tableGroups(), requestedGroup);
    HistoryLayout layout = selectLayout(group.historyLayout(), requestedLayout);
    HBaseProfile backend =
        configuration.hbaseProfiles().stream()
            .filter(profile -> profile.id().equals(group.hbaseProfileId()))
            .findFirst()
            .orElseThrow();
    SaltProfile salt =
        configuration.saltProfiles().stream()
            .filter(profile -> profile.id().equals(group.saltProfileId()))
            .findFirst()
            .orElseThrow();
    return new TableGroupInitializationPlan(
        group,
        backend,
        salt,
        layout,
        selectedTables(group, layout),
        namespaceTables(configuration, group, backend, layout));
  }

  private static TableGroup selectGroup(List<TableGroup> groups, String requested) {
    if (requested == null) {
      if (groups.size() != 1) {
        throw new IllegalArgumentException(
            "provide --table-group when multiple groups are configured");
      }
      return groups.getFirst();
    }
    return groups.stream()
        .filter(group -> group.id().equals(requested))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("requested table group is not configured"));
  }

  private static HistoryLayout selectLayout(HistoryLayout configured, HistoryLayout requested) {
    if (configured == HistoryLayout.UNDECIDED) {
      if (requested == null || requested == HistoryLayout.UNDECIDED) {
        throw new IllegalArgumentException("history layout is undecided; provide --history-layout");
      }
      return requested;
    }
    if (requested != null && requested != configured) {
      throw new IllegalArgumentException("requested history layout conflicts with configuration");
    }
    return configured;
  }

  private static List<TableSchemaCandidate> selectedTables(TableGroup group, HistoryLayout layout) {
    var tables = new ArrayList<TableSchemaCandidate>();
    tables.add(group.systemTable());
    group.historyCandidates().stream()
        .filter(candidate -> candidate.historyLayout() == layout)
        .findFirst()
        .orElseThrow()
        .tables()
        .forEach(tables::add);
    return List.copyOf(tables);
  }

  private static List<String> namespaceTables(
      InitializationConfiguration configuration,
      TableGroup selectedGroup,
      HBaseProfile selectedBackend,
      HistoryLayout selectedLayout) {
    Map<String, HBaseProfile> backends =
        configuration.hbaseProfiles().stream()
            .collect(Collectors.toUnmodifiableMap(HBaseProfile::id, Function.identity()));
    HBaseBackendScope scope = HBaseBackendScope.of(selectedBackend);
    Set<String> names = new TreeSet<>();
    for (TableGroup group : configuration.tableGroups()) {
      if (!scope.equals(HBaseBackendScope.of(backends.get(group.hbaseProfileId())))) {
        continue;
      }
      names.add(group.systemTable().tableName());
      HistoryLayout layout =
          group.id().equals(selectedGroup.id()) ? selectedLayout : group.historyLayout();
      group.historyCandidates().stream()
          .filter(
              candidate -> layout == HistoryLayout.UNDECIDED || candidate.historyLayout() == layout)
          .flatMap(candidate -> candidate.tables().stream())
          .map(TableSchemaCandidate::tableName)
          .forEach(names::add);
    }
    return List.copyOf(names);
  }
}
