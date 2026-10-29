package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;

/** Non-mutating backend authorization checks using the authenticated connection's current user. */
public final class HBaseAccessVerifiers {
  private HBaseAccessVerifiers() {}

  public record TableAccess(TableName table, Set<Action> actions) {
    public TableAccess {
      Objects.requireNonNull(table, "table");
      actions = Set.copyOf(actions);
      if (actions.isEmpty()) {
        throw new IllegalArgumentException("backend table access must require actions");
      }
    }
  }

  /**
   * Checks every required action on every configured table. A backend without this ACL RPC must
   * supply its own actual permission verifier; unsupported or unavailable checks fail closed.
   */
  public static HBaseAccessVerifier tablePermissions(List<TableAccess> requiredAccess) {
    var access = List.copyOf(requiredAccess);
    if (access.isEmpty() || access.size() > 64) {
      throw new IllegalArgumentException("backend table access requires between 1 and 64 tables");
    }
    return (connection, scope) -> {
      var permissions =
          access.stream()
              .map(
                  required ->
                      Permission.newBuilder(required.table())
                          .withActions(required.actions().toArray(Action[]::new))
                          .build())
              .toList();
      return connection
          .getAdmin()
          .hasUserPermissions(permissions)
          .thenCompose(
              allowed -> {
                if (allowed == null
                    || allowed.size() != permissions.size()
                    || !allowed.stream().allMatch(Boolean.TRUE::equals)) {
                  return CompletableFuture.failedFuture(
                      new BackendIdentityException(
                          BackendIdentityException.Reason.INSUFFICIENT_PERMISSION));
                }
                return CompletableFuture.completedFuture(null);
              });
    };
  }
}
