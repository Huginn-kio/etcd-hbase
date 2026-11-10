package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.junit.jupiter.api.Test;

class TableGroupStoresTest {
  @Test
  void rejectsWrongProvisioningPlanAndBackendClusterBeforeOpeningData() throws Exception {
    var plan = plan();
    for (var candidate : new String[][] {
        {"0".repeat(64), "backend-a", "backend-a"},
        {plan.source().fingerprint(), "backend-a", "backend-b"},
        {plan.source().fingerprint(), "backend-a", ""}}) {
      try (var scope = RequestScope.withoutDeadline();
           var connections = connection(candidate[0], candidate[1], candidate[2])) {
        var failure = assertThrows(CompletionException.class, () -> TableGroupStores.openConfigured(
            connections, identity(), plan, new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60)),
            scope).join());
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
      }
    }
  }

  private static M3TableGroupPlan plan() throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "j05", "history-table.json")));
    ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
    ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
    var config = new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
    return M3TableGroupPlan.from(TableGroupPlanFactory.create(config, "local", null));
  }

  private static AuthorizedBackendIdentity identity() {
    return new AuthorizedBackendIdentity("dev-no-auth", "fixture", "fixture-context",
        new AuthenticationScope("local", "local-hbase"), Set.of("local"), 1);
  }

  private static HBaseConnectionFactory connection(String plan, String descriptorCluster, String actualCluster) {
    TableDescriptor descriptor = proxy(TableDescriptor.class, (method, arguments) -> {
      if (method.equals("getValue") && arguments[0] instanceof String key) {
        return switch (key) {
          case "etcdhbase.m0.plan-fingerprint" -> plan;
          case "etcdhbase.m0.backend-cluster-id" -> descriptorCluster;
          default -> null;
        };
      }
      throw new AssertionError("unexpected descriptor access");
    });
    ClusterMetrics metrics = proxy(ClusterMetrics.class, (method, arguments) -> {
      if (method.equals("getClusterId")) return actualCluster;
      throw new AssertionError("unexpected cluster metrics access");
    });
    AsyncAdmin admin = proxy(AsyncAdmin.class, (method, arguments) -> switch (method) {
      case "getDescriptor" -> CompletableFuture.completedFuture(descriptor);
      case "getClusterMetrics" -> CompletableFuture.completedFuture(metrics);
      default -> throw new AssertionError("unexpected admin access");
    });
    AsyncConnection connection = proxy(AsyncConnection.class, (method, arguments) -> {
      if (method.equals("getAdmin")) return admin;
      throw new AssertionError("unexpected connection access");
    });
    return new HBaseConnectionFactory() {
      @Override public CompletableFuture<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
        return CompletableFuture.completedFuture(connection);
      }
      @Override public void close() {}
    };
  }

  private static <T> T proxy(Class<T> type, BiFunction<String, Object[], Object> invocation) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
        (ignored, method, arguments) -> invocation.apply(method.getName(), arguments)));
  }
}
