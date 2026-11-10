package io.etcdhbase.server;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.kv.M3CacheTestProbe;
import io.etcdhbase.core.kv.RangeCommand;
import io.etcdhbase.core.kv.RangeReadCache;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.server.commit.TableGroupStores;
import io.etcdhbase.server.commit.TxnExecutionLimits;
import io.etcdhbase.server.commit.TxnReadExecutor;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

/** Separate JVM with a private cache and the real HBase read executor. */
public final class M3CacheReaderProcess {
  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private M3CacheReaderProcess() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 8) throw new IllegalArgumentException("invalid cache fixture arguments");
    var configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    var authentication = new AuthenticationScope(arguments[1], "public");
    var salt = new HashSaltStrategy();
    var system = new SystemTableBinding(TableName.valueOf(arguments[2]), new byte[] {'s'},
        authentication.tableGroupId(), arguments[3], salt);
    var primary = new StorageTableBinding(TableName.valueOf(arguments[5]), new byte[] {'d'},
        StorageTableBinding.Layout.valueOf(arguments[4]), salt);
    var current = arguments[6].equals("-") ? Optional.<StorageTableBinding>empty()
        : Optional.of(new StorageTableBinding(TableName.valueOf(arguments[6]), new byte[] {'d'},
            StorageTableBinding.Layout.CURRENT, salt));
    try (var provider = new HBaseBackendIdentityProvider("m3-cache-child", authentication,
            configuration, HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-cache-child", new char[0])) {
      var identity = provider.authenticate(credentials, authentication).toCompletableFuture().get(30, TimeUnit.SECONDS);
      TableGroupStores stores;
      try (var opening = RequestScope.withTimeout(TIMEOUT)) {
        stores = TableGroupStores.open(provider, identity, system, primary, current,
            new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60)), opening).get(30, TimeUnit.SECONDS);
      }
      var limit = new BudgetManager.Limits(8, 8, 8, 8, 8, 1_000_000, 512L << 20);
      var budget = new BudgetManager(limit, Map.of(authentication.tableGroupId(), limit), limit);
      var proof = new UnchangedRangeProof(stores.decisions(), stores.manifests(), salt.profile().bucketCount(),
          8, 8L << 20, primary.codec()::bucket);
      var proofOptions = new RangeReadCache.ProofOptions(proof,
          new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192),
          new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000), 8, 512L << 20, 8, TIMEOUT.toNanos());
      try (var owner = RequestScope.withoutDeadline();
           var cache = new RangeReadCache(stores.clusterId(), system.profileFingerprint(), 1L << 20,
               8, 8, budget, authentication.tableGroupId(), owner, Optional.of(proofOptions));
           var scans = new ScanScheduler(budget, Duration.ofSeconds(10));
           var worker = Executors.newVirtualThreadPerTaskExecutor();
           var socket = new Socket(InetAddress.getByName("127.0.0.1"), Integer.parseInt(arguments[7]));
           var input = new DataInputStream(socket.getInputStream());
           var output = new DataOutputStream(socket.getOutputStream())) {
        var executor = new TxnReadExecutor(authentication.tableGroupId(), stores.views(), stores.manifests(),
            provider, primary, worker, scans, new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64),
            new RequestMetrics(), Optional.of(cache));
        output.writeUTF("ready");
        output.flush();
        while (true) {
          String command = input.readUTF();
          if (command.equals("stop")) break;
          respond(command, executor, identity, authentication.tableGroupId(), budget, cache, input, output);
        }
      }
      if (budget.snapshot().allocatedBytes() != 0 || budget.snapshot().reservedBytes() != 0) {
        throw new IllegalStateException("cache subprocess did not release its budgets");
      }
    }
  }

  private static void respond(String command, TxnReadExecutor executor,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, String group, BudgetManager budget,
      RangeReadCache cache, DataInputStream input, DataOutputStream output) throws Exception {
    if (!command.equals("read")) throw new IllegalArgumentException("unknown fixture operation");
    ByteKey key = ByteKey.copyOf(HexFormat.of().parseHex(input.readUTF()));
    var range = new RangeCommand(KeyRange.point(key), RangeCommand.Context.STANDALONE, 0, 0,
        new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false, false);
    try (var scope = RequestScope.withTimeout(TIMEOUT);
         var account = budget.admit(group, scope,
             new BudgetManager.Reservation(64L << 10, 64L << 10, 1L << 20, 4L << 20, 1L << 20))) {
      var request = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range)), List.of());
      var result = executor.execute(request, M3PutCommitIT.AUTHORIZATION, identity, scope, account)
          .get(60, TimeUnit.SECONDS);
      var root = (ResultNode.Txn) result.result().root();
      var response = (ResultNode.Range) root.responses().getFirst();
      output.writeLong(response.header().revision());
      output.writeBoolean(!response.records().isEmpty());
      if (!response.records().isEmpty()) {
        output.writeUTF(new String(response.records().getFirst().value().orElseThrow(), StandardCharsets.UTF_8));
      }
      output.writeLong(M3CacheTestProbe.hits(cache));
      output.flush();
    }
  }
}
