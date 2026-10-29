package io.etcdhbase.admin;

import io.etcdhbase.admin.config.InitializationConfiguration.HistoryLayout;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Strict, credential-free CLI input. Errors never echo unknown option names or values. */
record InitializationOptions(
    Action action, Path config, String group, HistoryLayout historyLayout, String confirmation) {
  enum Action {
    PLAN,
    APPLY
  }

  private static final Set<String> ALL_OPTIONS =
      Set.of(
          "--config",
          "--table-group",
          "--history-layout",
          "--confirm-table-group",
          "--confirm-plan");

  static InitializationOptions parse(String[] args) {
    if (args == null || args.length < 2 || !"init".equals(args[0])) {
      throw new IllegalArgumentException("usage: init plan|apply --config PATH [options]");
    }
    Action action = action(args[1]);
    Map<String, String> values = options(args);
    Path config = configurationPath(values.get("--config"));
    HistoryLayout layout = historyLayout(values.get("--history-layout"));
    Set<String> permitted =
        action == Action.PLAN
            ? Set.of("--config", "--table-group", "--history-layout")
            : Set.of("--config", "--confirm-table-group", "--confirm-plan", "--history-layout");
    if (!permitted.containsAll(values.keySet())) {
      throw new IllegalArgumentException("option does not apply to this init action");
    }
    if (action == Action.PLAN) {
      return new InitializationOptions(action, config, values.get("--table-group"), layout, null);
    }
    String group = values.get("--confirm-table-group");
    String confirmation = values.get("--confirm-plan");
    if (group == null
        || !group.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")
        || confirmation == null
        || !confirmation.matches("sha256:[0-9a-f]{64}")) {
      throw new IllegalArgumentException(
          "apply requires --confirm-table-group and --confirm-plan sha256:HEX");
    }
    return new InitializationOptions(action, config, group, layout, confirmation);
  }

  private static Action action(String value) {
    if ("plan".equals(value)) {
      return Action.PLAN;
    }
    if ("apply".equals(value)) {
      return Action.APPLY;
    }
    throw new IllegalArgumentException("init action must be plan or apply");
  }

  private static Map<String, String> options(String[] args) {
    Map<String, String> values = new HashMap<>();
    for (int index = 2; index < args.length; index += 2) {
      if (index + 1 >= args.length
          || args[index] == null
          || !ALL_OPTIONS.contains(args[index])
          || args[index + 1] == null) {
        throw new IllegalArgumentException("each supported option requires one value");
      }
      if (values.putIfAbsent(args[index], args[index + 1]) != null) {
        throw new IllegalArgumentException("duplicate option");
      }
    }
    return values;
  }

  private static Path configurationPath(String value) {
    if (value == null || value.isBlank() || value.length() > 4096) {
      throw new IllegalArgumentException("provide --config PATH");
    }
    try {
      return Path.of(value);
    } catch (InvalidPathException invalid) {
      throw new IllegalArgumentException("invalid configuration path");
    }
  }

  private static HistoryLayout historyLayout(String value) {
    if (value == null) {
      return null;
    }
    try {
      return HistoryLayout.valueOf(value);
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("unsupported history layout");
    }
  }
}
