package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.server.commit.M3CacheRuntimeTestAccess;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Real RPC through a service-owned cache; normal configuration remains disabled. */
@Timeout(600)
class M3CacheRuntimeIT {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path temporaryDirectory;
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 8, 0);

  @Test
  void serviceOwnsCacheAndPreservesCurrentBarriersOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        var plan = M3TableGroupRuntimeIT.plan(file,
            cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
        var authentication = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
        try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), authentication,
                 cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
                 (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
                 1, 1, Duration.ofSeconds(30));
             var credentials = new BackendCredentials("m3-cache-runtime-fixture", new char[0])) {
          var identity = provider.authenticate(credentials, authentication).toCompletableFuture().get(30, TimeUnit.SECONDS);
          M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider,
              new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60)));
          var limit = new BudgetManager.Limits(10, 10, 16, 8, 16, 4L << 20, 512L << 20);
          var budgets = new BudgetManager(limit, Map.of(plan.source().tableGroupId(), limit), limit);
          try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
            io.etcdhbase.server.commit.M3TableGroupRuntime runtime;
            try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
              runtime = M3CacheRuntimeTestAccess.open(plan, M3TableGroupRuntimeIT.activation("1"),
                  RESOURCES, provider, identity, budgets, scans, startup);
            }
            try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
                "m3-cache", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
              assertSecondRuntimeIsRejected(cluster, file, plan.source().namespace());
              var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port()).usePlaintext().build();
              try {
                exerciseCache(KVGrpc.newBlockingStub(channel).withDeadlineAfter(90, TimeUnit.SECONDS), runtime);
              } finally {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
              }
            } finally {
              runtime.close();
              runtime.drained().get(30, TimeUnit.SECONDS);
            }
          }
          assertEquals(0, budgets.snapshot().reservedBytes());
          assertEquals(0, budgets.snapshot().allocatedBytes());
        }
      }
    }
  }

  private void assertSecondRuntimeIsRejected(TemporaryHBaseClusterProcess cluster,
      String file, String namespace) throws Exception {
    Path directory = Files.createDirectory(temporaryDirectory.resolve(namespace));
    Path configuration = directory.resolve("hbase-site.xml");
    Path hosts = directory.resolve("hosts");
    Path log = directory.resolve("second-replica.log");
    try (var output = Files.newOutputStream(configuration)) {
      cluster.clientConfiguration().writeXml(output);
    }
    Files.writeString(hosts, "127.0.0.1 localhost\n::1 localhost\n");
    int port = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
    var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djdk.net.hosts.file=" + hosts.toAbsolutePath(), "-cp", System.getProperty("java.class.path"),
        M3SecondReplicaProcess.class.getName(), configuration.toString(), file, Integer.toString(port), namespace)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.pid() > 0 && child.pid() != ProcessHandle.current().pid());
      assertTrue(child.waitFor(90, TimeUnit.SECONDS), "second replica did not finish; log: " + log);
      assertEquals(0, child.exitValue(), "second replica failed for an unexpected reason; log: " + log);
      assertTrue(Files.readString(log).contains("m3-second-replica: active owner rejected; resources drained"));
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }

  private static void exerciseCache(KVGrpc.KVBlockingStub client,
      io.etcdhbase.server.commit.M3TableGroupRuntime runtime) {
    ByteString key = ByteString.copyFromUtf8("cache-key");
    ByteString unrelated = ByteString.copyFromUtf8("other-key");
    client.put(Rpc.PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8("before")).build());
    var range = Rpc.RangeRequest.newBuilder().setKey(key).build();
    assertEquals("before", client.range(range).getKvs(0).getValue().toStringUtf8());
    assertEquals(0, M3CacheRuntimeTestAccess.hits(runtime));
    assertEquals("before", client.range(range).getKvs(0).getValue().toStringUtf8());
    assertEquals(1, M3CacheRuntimeTestAccess.hits(runtime));
    client.put(Rpc.PutRequest.newBuilder().setKey(unrelated).setValue(ByteString.copyFromUtf8("other")).build());
    var afterUnrelated = client.range(range);
    assertEquals("before", afterUnrelated.getKvs(0).getValue().toStringUtf8());
    assertEquals(3, afterUnrelated.getHeader().getRevision());
    assertEquals(2, M3CacheRuntimeTestAccess.hits(runtime));
    client.put(Rpc.PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8("after")).build());
    assertEquals("after", client.range(range).getKvs(0).getValue().toStringUtf8());
    assertEquals(2, M3CacheRuntimeTestAccess.hits(runtime));
  }
}
