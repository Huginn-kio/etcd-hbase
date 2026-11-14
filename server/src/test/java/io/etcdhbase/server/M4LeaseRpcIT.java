package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.grpc.internal.v1.ForwardMutationRequest;
import io.etcdhbase.grpc.internal.v1.ForwardMutationResponse;
import io.etcdhbase.grpc.internal.v1.InternalOwnerServiceGrpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.commit.ServiceAuthorizationContextReference;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.io.TempDir;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies M4 Lease RPCs against native etcd, a disposable HBase cluster, and runtime restart. */
@Timeout(600)
class M4LeaseRpcIT {
  private static final int MAX_DRIVER_DIAGNOSTIC_BYTES = 8 * 1024;
  private static final int TEST_OWNER_LEASE_MILLIS = 6_000;
  private static final int TEST_HEARTBEAT_MILLIS = 1_000;
  private static final List<String> LAYOUT_FILES =
      List.of("history-table.json", "native-cell-versions.json");
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000,
          4 << 20, 4 << 20, 16, 0);
  @TempDir Path testDirectory;

  @Test
  void realGoLeaseCallsMatchNativeAndSurviveRestartAcrossLayouts() throws Exception {
    RunContext run = prepareRun();
    prepareNativeEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compare(run, "HISTORY");
    compare(run, "NATIVE");
  }

  static void verifyDualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner(
      Path testDirectory) throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      var runtimeForwardingIdentity = runtimeForwardingIdentity(source.identityProviderId(), authScope);
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-owner-forwarding", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, activation("1").pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-owner-forwarding", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        try (TestTlsCertificate ownerACertificate = TestTlsCertificate.create(testDirectory, "owner-a");
             TestTlsCertificate ownerBCertificate = TestTlsCertificate.create(testDirectory, "owner-b");
             TestTlsCertificate unregisteredPeerCertificate =
                 TestTlsCertificate.create(testDirectory, "unregistered-peer")) {
            Path trustedPeers = testDirectory.resolve("trusted-owner-peers.pem");
            Files.writeString(trustedPeers,
                Files.readString(ownerACertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(ownerBCertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(unregisteredPeerCertificate.certificate()));
            int ownerAPort = availablePort();
            int ownerBPort = availablePort();
            var peers = List.of(
                new M3RuntimeConfiguration.OwnerPeer("owner-a", "127.0.0.1", ownerAPort, "owner-a"),
                new M3RuntimeConfiguration.OwnerPeer("owner-b", "127.0.0.1", ownerBPort, "owner-b"));
            var ownerAForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerAPort, ownerACertificate.certificate().toString(),
                ownerACertificate.privateKey().toString(), trustedPeers.toString(), peers);
            var ownerBForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerBPort, ownerBCertificate.certificate().toString(),
                ownerBCertificate.privateKey().toString(), trustedPeers.toString(), peers);

            RuntimeServer ownerA = RuntimeServer.startM3(configuration,
                ownerActivation("1", "owner-a", ownerAForwarding));
            RuntimeServer ownerB = null;
            try {
              ownerB = RuntimeServer.startM3(configuration,
                  ownerActivation("2", "owner-b", ownerBForwarding));
              assertTrue(ownerA.dataReady());
              assertTrue(ownerB.dataReady());
              assertPeerCertificateWithoutConfiguredIdentityIsRejected(
                  ownerAPort, "owner-a", trustedPeers, unregisteredPeerCertificate);
              CommitState activeOwner = controlSnapshot(plan, initializer, identity).state();
              assertEquals("owner-a", activeOwner.ownerInstance().orElseThrow());
              long scopeProbeLease = leaseIdForPrefix(run.prefix(), "forwarding-context-probe");
              var invalidContextRequest = keepAliveForwardRequest(source.tableGroupId(),
                  runtimeForwardingIdentity, Rpc.LeaseKeepAliveRequest.newBuilder()
                      .setID(scopeProbeLease).build(), InvocationId.create(), activeOwner.ownerEpoch())
                  .toBuilder()
                  .setAuthorizationContextReference(ByteString.copyFromUtf8("unresolved-context"))
                  .build();
              ForwardMutationResponse invalidContextReply = forwardMutation(ownerAPort, "owner-a",
                  trustedPeers, ownerBCertificate, invalidContextRequest);
              assertEquals(ForwardMutationResponse.Outcome.UNKNOWN, invalidContextReply.getOutcome());
              assertEquals("PERMISSION", invalidContextReply.getFailureCode());
              CommitState afterInvalidContext = controlSnapshot(plan, initializer, identity).state();
              assertEquals(activeOwner.publishedSequence(), afterInvalidContext.publishedSequence(),
                  "an unresolved forwarded service context must be rejected before invocation binding");

              Rpc.PutResponse forwardedPut = putThrough(ownerB, "forwarded-before-takeover", 2);
              assertEquals(initialized.clusterId(), forwardedPut.getHeader().getClusterId());
              assertEquals(2, forwardedPut.getHeader().getMemberId());
              assertForwardedPutsRespectZeroQueue(ownerB);

              long leaseA = leaseIdForPrefix(run.prefix(), "keepalive-a");
              long leaseB = leaseIdForPrefix(run.prefix(), "keepalive-b");
              long missingLease = leaseIdForPrefix(run.prefix(), "keepalive-missing");
              grantThrough(ownerB, leaseA, 30);
              grantThrough(ownerB, leaseB, 60);
              CommitState beforeRenewal = controlSnapshot(plan, initializer, identity).state();
              List<Rpc.LeaseKeepAliveResponse> renewals = keepAliveThrough(
                  ownerB, leaseA, leaseB, missingLease);
              assertEquals(List.of(leaseA, leaseB, missingLease),
                  renewals.stream().map(Rpc.LeaseKeepAliveResponse::getID).toList());
              assertTrue(renewals.get(0).getTTL() > 0);
              assertTrue(renewals.get(1).getTTL() > 0);
              assertEquals(0, renewals.get(2).getTTL());
              for (var renewal : renewals) {
                assertEquals(beforeRenewal.publishedRevision(), renewal.getHeader().getRevision());
                assertEquals(initialized.clusterId(), renewal.getHeader().getClusterId());
                assertEquals(2, renewal.getHeader().getMemberId());
              }
              CommitState afterRenewal = controlSnapshot(plan, initializer, identity).state();
              assertEquals(beforeRenewal.publishedRevision(), afterRenewal.publishedRevision());
              assertEquals(beforeRenewal.publishedSequence() + 2,
                  afterRenewal.publishedSequence(),
                  "unknown Lease KeepAlive must persist its TTL=0 result without advancing sequence");

              InvocationId missingInvocation = InvocationId.create();
              var missingKeepAlive = Rpc.LeaseKeepAliveRequest.newBuilder().setID(missingLease).build();
              ForwardMutationRequest missingAttempt = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, missingKeepAlive, missingInvocation,
                  afterRenewal.ownerEpoch());
              ForwardMutationResponse firstMissingReply = forwardMutation(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, missingAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, firstMissingReply.getOutcome());
              assertTrue(java.util.Arrays.equals(missingInvocation.bytes(),
                  firstMissingReply.getOperationResult().toByteArray()));
              CommitState afterMissingReply = controlSnapshot(plan, initializer, identity).state();
              assertEquals(afterRenewal.publishedSequence(), afterMissingReply.publishedSequence());
              assertEquals(afterRenewal.publishedRevision(), afterMissingReply.publishedRevision());

              ForwardMutationResponse replayedMissingReply = forwardMutation(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, missingAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, replayedMissingReply.getOutcome());
              assertTrue(java.util.Arrays.equals(missingInvocation.bytes(),
                  replayedMissingReply.getOperationResult().toByteArray()));
              CommitState afterMissingReplay = controlSnapshot(plan, initializer, identity).state();
              assertEquals(afterMissingReply.publishedSequence(), afterMissingReplay.publishedSequence());
              assertEquals(afterMissingReply.publishedRevision(), afterMissingReplay.publishedRevision());

              long oldEpoch = afterMissingReplay.ownerEpoch();
              ownerA.close();
              M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
              CommitState newOwner = awaitOwner(plan, initializer, identity, "owner-b", oldEpoch);
              assertTrue(newOwner.ownerEpoch() > oldEpoch);
              assertEquals(2, putThrough(ownerB, "forwarded-after-takeover", 2)
                  .getHeader().getMemberId());
              CommitState beforeStaleRenewal = controlSnapshot(plan, initializer, identity).state();
              assertRejectedStaleOwner(ownerBForwarding, trustedPeers, ownerBPort, ownerACertificate,
                  source.tableGroupId(), identity, leaseA, oldEpoch);
              CommitState afterStaleRenewal = controlSnapshot(plan, initializer, identity).state();
              assertEquals(beforeStaleRenewal.publishedSequence(), afterStaleRenewal.publishedSequence());
              assertEquals(beforeStaleRenewal.publishedRevision(), afterStaleRenewal.publishedRevision());
            } finally {
              if (ownerB != null) ownerB.close();
              ownerA.close();
            }
        }
      }
    }
  }

  static boolean verifyPersistedKeepAliveSurvivesLostOwnerReplyAndRestart(
      Path testDirectory) throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      var runtimeForwardingIdentity = runtimeForwardingIdentity(source.identityProviderId(), authScope);
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-lost-reply", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration firstActivation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-lost-reply", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        TestTlsCertificate ownerACertificate = TestTlsCertificate.create(testDirectory, "owner-a");
        try {
          TestTlsCertificate ownerBCertificate = TestTlsCertificate.create(testDirectory, "owner-b");
          try {
            Path trustedPeers = testDirectory.resolve("trusted-lost-reply-peers.pem");
            Files.writeString(trustedPeers,
                Files.readString(ownerACertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(ownerBCertificate.certificate()));
            int ownerAPort = availablePort();
            int ownerBPort = availablePort();
            var peers = List.of(
                new M3RuntimeConfiguration.OwnerPeer("owner-a", "127.0.0.1", ownerAPort, "owner-a"),
                new M3RuntimeConfiguration.OwnerPeer("owner-b", "127.0.0.1", ownerBPort, "owner-b"));
            var ownerAForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerAPort, ownerACertificate.certificate().toString(),
                ownerACertificate.privateKey().toString(), trustedPeers.toString(), peers);
            var ownerBForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerBPort, ownerBCertificate.certificate().toString(),
                ownerBCertificate.privateKey().toString(), trustedPeers.toString(), peers);

            RuntimeServer ownerA = RuntimeServer.startM3(configuration,
                ownerActivation("1", "owner-a", ownerAForwarding));
            RuntimeServer ownerB = null;
            try {
              ownerB = RuntimeServer.startM3(configuration,
                  ownerActivation("2", "owner-b", ownerBForwarding));
              assertTrue(ownerA.dataReady());
              assertTrue(ownerB.dataReady());

              long leaseId = leaseIdForPrefix(run.prefix(), "keepalive-lost-reply");
              grantThrough(ownerB, leaseId, 30);
              CommitState beforeRenewal = controlSnapshot(plan, initializer, identity).state();
              LeaseState leaseBeforeRenewal = leaseStateAt(plan, initializer, identity, leaseId,
                  beforeRenewal.publishedSequence());
              long originalOwnerEpoch = beforeRenewal.ownerEpoch();
              Rpc.LeaseKeepAliveRequest keepAlive = Rpc.LeaseKeepAliveRequest.newBuilder()
                  .setID(leaseId).build();
              InvocationId invocation = InvocationId.create();
              ForwardMutationRequest firstAttempt = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, keepAlive, invocation, originalOwnerEpoch);
              ForwardMutationResponse droppedReply = forwardMutationAndDropReply(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, firstAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, droppedReply.getOutcome());
              assertTrue(java.util.Arrays.equals(invocation.bytes(), droppedReply.getOperationResult().toByteArray()));

              CommitState selected = controlSnapshot(plan, initializer, identity).state();
              assertEquals(beforeRenewal.publishedRevision(), selected.publishedRevision(),
                  "KeepAlive must not publish a KV revision");
              assertEquals(beforeRenewal.publishedSequence() + 1, selected.publishedSequence(),
                  "the renewal must be selected before its response is lost");
              LeaseState leaseAfterRenewal = leaseStateAt(plan, initializer, identity, leaseId,
                  selected.publishedSequence());
              assertEquals(leaseBeforeRenewal.renewalGeneration() + 1,
                  leaseAfterRenewal.renewalGeneration(),
                  "the selected renewal must persist its generation, not only advance control state");
              assertTrue(leaseAfterRenewal.deadlineEpochMillis()
                  > leaseBeforeRenewal.deadlineEpochMillis(),
                  "the selected renewal must persist a later deadline");
              assertEquals(selected.publishedSequence(), leaseAfterRenewal.sequence());
              long selectedOwnerEpoch = selected.ownerEpoch();

              ownerA.close();
              M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
              CommitState newOwner = awaitOwner(plan, initializer, identity, "owner-b", selectedOwnerEpoch);
              ForwardMutationRequest retry = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, keepAlive, invocation, newOwner.ownerEpoch());
              ForwardMutationResponse recoveredReply = forwardMutation(
                  ownerBPort, "owner-b", trustedPeers, ownerACertificate, retry);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, recoveredReply.getOutcome());
              assertTrue(java.util.Arrays.equals(invocation.bytes(), recoveredReply.getOperationResult().toByteArray()));

              CommitState afterRetry = controlSnapshot(plan, initializer, identity).state();
              assertEquals(selected.publishedSequence(), afterRetry.publishedSequence(),
                  "replaying the original invocation must not renew the Lease a second time");
              assertEquals(selected.publishedRevision(), afterRetry.publishedRevision());
              assertEquals(leaseAfterRenewal, leaseStateAt(plan, initializer, identity, leaseId,
                  afterRetry.publishedSequence()),
                  "replay after owner takeover must preserve the exact durable Lease version");
              Rpc.LeaseTimeToLiveResponse ttl = timeToLive(ownerB, leaseId, true);
              assertEquals(leaseId, ttl.getID());
              assertTrue(ttl.getTTL() > 0);
              assertEquals(0, ttl.getKeysCount());
              assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
              assertEquals(selected.publishedRevision(), ttl.getHeader().getRevision());
              return true;
            } finally {
              if (ownerB != null) ownerB.close();
              ownerA.close();
            }
          } finally {
            ownerBCertificate.delete();
          }
        } finally {
          ownerACertificate.delete();
        }
      }
    }
  }

  static void verifyPersistedKeepAliveSurvivesClientCancellationBeforeResponse() throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials(
               "m4-client-cancel", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration firstActivation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-client-cancel", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        long leaseId = leaseIdForPrefix(run.prefix(), "client-cancel");
        CommitState renewed;
        try (RuntimeServer server = RuntimeServer.startM3(configuration, firstActivation)) {
          assertTrue(server.dataReady());
          Rpc.LeaseGrantResponse granted = grantThrough(server, leaseId, 30);
          assertEquals(initialized.clusterId(), granted.getHeader().getClusterId());
          CommitState before = controlSnapshot(plan, initializer, identity).state();
          LeaseState leaseBeforeRenewal = leaseStateAt(plan, initializer, identity, leaseId,
              before.publishedSequence());
          var channel = NettyChannelBuilder.forAddress(
              "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
          var responseReceived = new AtomicBoolean();
          var closed = new CountDownLatch(1);
          var closeStatus = new AtomicReference<Status>();
          try {
            ClientCall<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse> call = channel.newCall(
                LeaseGrpc.getLeaseKeepAliveMethod(),
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS));
            call.start(new ClientCall.Listener<>() {
              @Override public void onMessage(Rpc.LeaseKeepAliveResponse response) {
                responseReceived.set(true);
              }
              @Override public void onClose(Status status, Metadata trailers) {
                closeStatus.set(status);
                closed.countDown();
              }
            }, new Metadata());
            call.sendMessage(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());

            renewed = awaitFinalizedSequence(plan, initializer, identity,
                before.publishedSequence() + 1, Duration.ofSeconds(30));
            assertEquals(before.publishedRevision(), renewed.publishedRevision(),
                "KeepAlive must not change the KV revision");
            LeaseState leaseAfterRenewal = leaseStateAt(plan, initializer, identity, leaseId,
                renewed.publishedSequence());
            assertEquals(leaseBeforeRenewal.renewalGeneration() + 1,
                leaseAfterRenewal.renewalGeneration(),
                "the durable renewal must advance the persisted generation before response delivery");
            assertTrue(leaseAfterRenewal.deadlineEpochMillis()
                > leaseBeforeRenewal.deadlineEpochMillis(),
                "the durable renewal must persist a later deadline before response delivery");
            assertEquals(renewed.publishedSequence(), leaseAfterRenewal.sequence());

            // Manual ClientCall flow control is intentionally left at zero demand. The server may
            // persist and queue its response, but the client cancels before consuming that response.
            call.cancel("cancel after durable renewal", null);
            assertTrue(closed.await(10, TimeUnit.SECONDS), "client cancellation did not close the RPC");
            assertEquals(Status.Code.CANCELLED, closeStatus.get().getCode());
            assertFalse(responseReceived.get(), "the cancelled client must not consume a success response");
            CommitState afterCancel = controlSnapshot(plan, initializer, identity).state();
            assertEquals(renewed.publishedSequence(), afterCancel.publishedSequence(),
                "cancelling the public RPC must not undo or duplicate a selected renewal");
            assertEquals(renewed.publishedRevision(), afterCancel.publishedRevision());
          } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
          }
        }

        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
        try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
          assertTrue(restarted.dataReady());
          CommitState recovered = controlSnapshot(plan, initializer, identity).state();
          assertEquals(renewed.publishedSequence(), recovered.publishedSequence());
          assertEquals(renewed.publishedRevision(), recovered.publishedRevision());
          assertEquals(leaseStateAt(plan, initializer, identity, leaseId,
                  renewed.publishedSequence()),
              leaseStateAt(plan, initializer, identity, leaseId, recovered.publishedSequence()),
              "restart must retain the exact renewed Lease version selected before cancellation");
          Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseId, true);
          assertEquals(leaseId, ttl.getID());
          assertTrue(ttl.getTTL() > 0, "the durable renewal must survive client cancellation and restart");
          assertEquals(30, ttl.getGrantedTTL());
          assertEquals(0, ttl.getKeysCount());
          assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
          assertEquals(renewed.publishedRevision(), ttl.getHeader().getRevision());
        }
      }
    }
  }

  private static CommitState awaitFinalizedSequence(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long expectedSequence, Duration timeout) throws Exception {
    long deadline = Math.addExact(System.nanoTime(), timeout.toNanos());
    while (System.nanoTime() < deadline) {
      CommitState state = controlSnapshot(plan, provider, identity).state();
      if (state.publishedSequence() == expectedSequence
          && state.finalizedSequence() == expectedSequence) return state;
      if (state.publishedSequence() > expectedSequence || state.finalizedSequence() > expectedSequence) {
        throw new AssertionError("more than one KeepAlive decision was published before cancellation");
      }
      TimeUnit.MILLISECONDS.sleep(25);
    }
    throw new AssertionError("KeepAlive decision was not finalized before the client deadline");
  }

  private static LeaseState leaseStateAt(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, long sequence) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
      var entry = records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.routePrefix(leaseId), sequence, scope).join().orElseThrow();
      assertEquals(leaseId, LeaseRecordIdentity.leaseId(entry.identity()));
      assertEquals(sequence, LeaseRecordIdentity.sequence(entry.identity()));
      LeaseState state = LeaseStateMessages.decode(entry.value());
      assertEquals(leaseId, state.identity().leaseId());
      assertEquals(sequence, state.sequence());
      return state;
    }
  }

  static void verifyGoLeaseBindingsMatchNativeAndSurviveRestartAcrossLayouts() throws Exception {
    RunContext run = prepareBindingRun();
    prepareNativeBindingEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runBindingLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compareBindings(run, "HISTORY");
    compareBindings(run, "NATIVE");
  }

  static void verifyGoLeaseKeepAliveMatchesNativeAcrossLayouts() throws Exception {
    RunContext run = prepareKeepAliveRun();
    prepareNativeKeepAliveEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runKeepAliveLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compareKeepAlive(run, "HISTORY");
    compareKeepAlive(run, "NATIVE");
  }

  private static void verifyKeepAliveMetrics(
      io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalSnapshot metrics) {
    assertTrue(metrics.requests() >= 8,
        "KeepAlive metrics must count the four raw and four managed renewals, not Grant calls");
    assertTrue(metrics.responses() > 0);
    assertTrue(metrics.selected() > 0);
    assertTrue(metrics.auxiliaryRecords() > 0);
    assertTrue(metrics.auxiliaryPayloadBytes() > 0);
    assertTrue(metrics.hbaseClientOperations() > 0,
        "KeepAlive metrics must count owner-side HBase client operations");
    assertTrue(metrics.logicalHBaseRequestPayloadBytes() > 0,
        "KeepAlive metrics must count encoded row and cell payload bytes");
    assertTrue(metrics.requestBytes() > 0);
    assertTrue(metrics.responseBytes() > 0);
    for (var stage : io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalStage.values()) {
      assertTrue(metrics.stages().get(stage).count() > 0,
          "missing KeepAlive metric stage " + stage);
    }
  }

  static void verifyExpiredActiveLeaseIsRejectedUntilDurableRevocation() throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials(
               "m4-expired-keepalive", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration activation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, activation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-expired-keepalive", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        try (RuntimeServer server = RuntimeServer.startM3(configuration, activation)) {
          assertTrue(server.dataReady());
          long leaseId = leaseIdForPrefix(run.prefix(), "expired-active");
          Rpc.LeaseGrantResponse granted = grantThrough(server, leaseId, 5);
          assertEquals(initialized.clusterId(), granted.getHeader().getClusterId());
          CommitState before = controlSnapshot(plan, initializer, identity).state();

          TimeUnit.SECONDS.sleep(7);

          assertEquals(Status.Code.NOT_FOUND, keepAliveFailureCode(server, leaseId),
              "an expired ACTIVE record must wait for durable revocation instead of returning TTL=0");
          CommitState after = controlSnapshot(plan, initializer, identity).state();
          assertEquals(before.publishedSequence(), after.publishedSequence(),
              "a rejected renewal must not select an empty auxiliary commit");
          assertEquals(before.publishedRevision(), after.publishedRevision());
        }
      }
    }
  }

  private static void runKeepAliveLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-keepalive", "127.0.0.1", 0,
              source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
          initialization.tableGroups(), RESOURCES);

      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      CommitState selected;
      try {
        assertTrue(first.dataReady());
        runKeepAliveTargetDriver(run, layout, first.boundListeners().getFirst().port());
        verifyKeepAliveMetrics(first.metrics().getFirst().leaseRenewals());
        selected = controlSnapshot(plan, initializer, identity).state();
        assertEquals(1, selected.publishedRevision(),
            "Grant and KeepAlive must not change the KV revision");
        assertTrue(selected.publishedSequence() > 1,
            "Grant and KeepAlive auxiliary state must be durably selected");
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      long leaseA = leaseIdForPrefix(run.prefix(), "keepalive-a");
      long leaseB = leaseIdForPrefix(run.prefix(), "keepalive-b");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
        assertTrue(restarted.dataReady());
        for (long leaseId : List.of(leaseA, leaseB)) {
          Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseId, true);
          assertEquals(leaseId, ttl.getID());
          assertTrue(ttl.getTTL() > 0, "renewed Lease must survive an owner restart");
          assertEquals(30, ttl.getGrantedTTL());
          assertEquals(0, ttl.getKeysCount());
          assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
          assertEquals(1, ttl.getHeader().getRevision());
        }
      }
    }
  }

  private static void runLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      ControlSnapshot preUpgrade = controlSnapshot(plan, initializer, identity);
      assertEquals(CommitState.FORMAT_VERSION, preUpgrade.state().formatVersion());
      long staleWriterNow = System.currentTimeMillis();
      CommitState staleWriterClaim = preUpgrade.state().claimOwner("pre-m4-writer", staleWriterNow,
          Math.addExact(staleWriterNow, 60_000));
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);

      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      String prefix = run.prefix();
      long explicitId;
      try {
        assertTrue(first.dataReady());
        assertOldWriterCannotPublish(plan, initializer, identity, preUpgrade, staleWriterClaim);
        runTargetDriver(run, layout, first.boundListeners().getFirst().port());
        assertEquals(1, assertControlAndTableAreAuxiliary(plan, initializer, identity));
        explicitId = leaseIdForPrefix(prefix, "explicit");
        assertTrue(explicitId > 0);
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      if (layout.equals("HISTORY")) {
        assertM4CannotBeDisabledAfterFormatUpgrade(configuration, activation("3", false));
      }
      M3RuntimeConfiguration secondActivation = activation("2");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, secondActivation)) {
        assertTrue(restarted.dataReady());
        Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, explicitId, true);
        assertEquals(explicitId, ttl.getID());
        assertTrue(ttl.getTTL() > 0);
        assertEquals(30, ttl.getGrantedTTL());
        assertEquals(0, ttl.getKeysCount());
        assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
        assertEquals(1, ttl.getHeader().getRevision());
        Rpc.LeaseLeasesResponse leases = leases(restarted);
        assertTrue(leases.getLeasesList().stream().anyMatch(value -> value.getID() == explicitId));
        assertEquals(initialized.clusterId(), leases.getHeader().getClusterId());
        assertEquals(1, leases.getHeader().getRevision());
        assertEquals(2, leases.getHeader().getMemberId());
      }
    }
  }

  private static void runBindingLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);

      long publishedRevision;
      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      try {
        assertTrue(first.dataReady());
        runBindingTargetDriver(run, layout, first.boundListeners().getFirst().port());
        publishedRevision = assertControlAndTableAreAuxiliary(plan, initializer, identity);
        assertTrue(publishedRevision > 1, "binding scenario should advance the KV revision");
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      long leaseB = leaseIdForPrefix(run.prefix(), "binding-b");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
        assertTrue(restarted.dataReady());
        Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseB, true);
        assertEquals(leaseB, ttl.getID());
        assertTrue(ttl.getTTL() > 0);
        assertEquals(60, ttl.getGrantedTTL());
        assertEquals(2, ttl.getKeysCount());
        assertEquals(Set.of(
            ByteString.copyFrom(run.prefix() + "survivor/registered", StandardCharsets.UTF_8),
            ByteString.copyFrom(run.prefix() + "atomic/unselected-else", StandardCharsets.UTF_8)),
            Set.copyOf(ttl.getKeysList()));
        assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
        assertEquals(publishedRevision, ttl.getHeader().getRevision());
      }
    }
  }

  private static ControlSnapshot controlSnapshot(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      return HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join()
          .read(scope).join().orElseThrow();
    }
  }

  private static void assertOldWriterCannotPublish(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      ControlSnapshot preUpgrade, CommitState staleWriterClaim) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      assertFalse(control.compareAndSet(preUpgrade, staleWriterClaim, scope).join());
      assertTrue(control.read(scope).join().orElseThrow().state().supportsAuxiliaryRecords());
    }
  }

  private static void assertM4CannotBeDisabledAfterFormatUpgrade(
      RuntimeConfiguration configuration, M3RuntimeConfiguration disabledActivation) {
    IOException failure = assertThrows(IOException.class,
        () -> RuntimeServer.startM3(configuration, disabledActivation));
    assertTrue(causeChainContains(failure,
        "M4 Lease activation must remain enabled for a table group with auxiliary state"));
  }

  private static boolean causeChainContains(Throwable failure, String expected) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) return true;
    }
    return false;
  }

  private static long assertControlAndTableAreAuxiliary(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      var state = control.read(scope).join().orElseThrow().state();
      assertTrue(state.supportsAuxiliaryRecords());
      assertTrue(state.supportsLeaseBindings());
      assertTrue(state.publishedSequence() > 0);
      var connection = provider.connectionFor(identity).toCompletableFuture().join();
      var descriptor = connection.getAdmin().getDescriptor(plan.system().table()).join();
      assertTrue(plan.system().supportsAuxiliary(descriptor));
      assertTrue(plan.system().supportsLeaseBindings(descriptor));
      return state.publishedRevision();
    }
  }

  private static Rpc.LeaseTimeToLiveResponse timeToLive(RuntimeServer server, long id,
      boolean includeKeys) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseTimeToLive(Rpc.LeaseTimeToLiveRequest.newBuilder().setID(id).setKeys(includeKeys).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Rpc.LeaseLeasesResponse leases(RuntimeServer server) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static int availablePort() throws IOException {
    try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  private static M3RuntimeConfiguration ownerActivation(String memberId, String instanceId,
      M3RuntimeConfiguration.OwnerForwardingSettings forwarding) throws Exception {
    M3RuntimeConfiguration base = activation(memberId);
    return new M3RuntimeConfiguration(base.memberId(), base.ownerLeaseMs(), base.heartbeatMs(),
        base.startupTimeoutMs(), base.shutdownTimeoutMs(), base.memoryBytes(),
        base.maxEtcdRequestBytes(), base.pinShards(), base.pinSlotsPerShard(), base.pinLeaseMs(),
        base.readCacheEnabled(), base.m4LeaseApiEnabled(), base.leaseMinTtlSeconds(),
        base.leaseClockUncertaintyMs(), base.leaseMaximumBindings(), base.leaseMaximumKeyBytes(),
        base.leaseMaximumRevokeCostBytes(), instanceId, forwarding);
  }

  private static Rpc.PutResponse putThrough(RuntimeServer server, String key, long expectedMemberId)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
            "127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      Rpc.PutResponse response = KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .put(Rpc.PutRequest.newBuilder()
              .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
              .setValue(ByteString.copyFromUtf8("m4-owner-forwarding"))
              .build());
      assertEquals(expectedMemberId, response.getHeader().getMemberId());
      return response;
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertForwardedPutsRespectZeroQueue(RuntimeServer standby) throws Exception {
    var revisions = new java.util.HashSet<Long>();
    for (int index = 0; index < 4; index++) {
      Rpc.PutResponse response = putThrough(standby, "forwarded-sequential-" + index, 2);
      revisions.add(response.getHeader().getRevision());
    }
    assertEquals(4, revisions.size(),
        "each forwarded write must publish a revision without relying on request queueing");
  }

  private static Rpc.LeaseGrantResponse grantThrough(RuntimeServer server, long leaseId, int ttl)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
            "127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseGrant(Rpc.LeaseGrantRequest.newBuilder().setID(leaseId).setTTL(ttl).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static List<Rpc.LeaseKeepAliveResponse> keepAliveThrough(
      RuntimeServer server, long... leaseIds) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
            "127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    var responses = new CopyOnWriteArrayList<Rpc.LeaseKeepAliveResponse>();
    var failure = new AtomicReference<Throwable>();
    var terminated = new CountDownLatch(1);
    try {
      StreamObserver<Rpc.LeaseKeepAliveResponse> receiver = new StreamObserver<>() {
        @Override public void onNext(Rpc.LeaseKeepAliveResponse response) { responses.add(response); }
        @Override public void onError(Throwable cause) {
          failure.set(cause);
          terminated.countDown();
        }
        @Override public void onCompleted() { terminated.countDown(); }
      };
      StreamObserver<Rpc.LeaseKeepAliveRequest> sender = LeaseGrpc.newStub(channel)
          .withDeadlineAfter(60, TimeUnit.SECONDS).leaseKeepAlive(receiver);
      for (long leaseId : leaseIds) {
        sender.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());
      }
      sender.onCompleted();
      assertTrue(terminated.await(60, TimeUnit.SECONDS), "KeepAlive stream did not terminate");
      if (failure.get() != null) throw new AssertionError("KeepAlive stream failed", failure.get());
      return List.copyOf(responses);
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Status.Code keepAliveFailureCode(RuntimeServer server, long leaseId)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
            "127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    var responses = new CopyOnWriteArrayList<Rpc.LeaseKeepAliveResponse>();
    var failure = new AtomicReference<Throwable>();
    var terminated = new CountDownLatch(1);
    try {
      StreamObserver<Rpc.LeaseKeepAliveRequest> sender = LeaseGrpc.newStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS).leaseKeepAlive(new StreamObserver<>() {
            @Override public void onNext(Rpc.LeaseKeepAliveResponse response) { responses.add(response); }
            @Override public void onError(Throwable cause) {
              failure.set(cause);
              terminated.countDown();
            }
            @Override public void onCompleted() { terminated.countDown(); }
          });
      sender.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());
      sender.onCompleted();
      assertTrue(terminated.await(30, TimeUnit.SECONDS), "KeepAlive failure stream did not terminate");
      assertTrue(responses.isEmpty(), "an expired ACTIVE Lease must not return a successful response");
      Throwable observed = failure.get();
      assertTrue(observed != null, "an expired ACTIVE Lease must terminate with a failure");
      return Status.fromThrowable(observed).getCode();
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static CommitState awaitOwner(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      String expectedOwner, long previousEpoch) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    CommitState observed = null;
    while (System.nanoTime() < deadline) {
      observed = controlSnapshot(plan, provider, identity).state();
      if (observed.ownerInstance().filter(expectedOwner::equals).isPresent()
          && observed.ownerEpoch() > previousEpoch) {
        return observed;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    throw new AssertionError("owner did not advance to " + expectedOwner
        + " with a new epoch; last state=" + observed);
  }

  private static void assertRejectedStaleOwner(
      M3RuntimeConfiguration.OwnerForwardingSettings forwarding, Path trustedPeers, int port,
      TestTlsCertificate staleOwnerCertificate, String tableGroup,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, long leaseId, long staleEpoch)
      throws Exception {
    Rpc.LeaseKeepAliveRequest keepAlive = Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build();
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(keepAlive.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    ByteKey authorization = authorizationDigest(identity);
    var request = ForwardMutationRequest.newBuilder()
        .setTableGroupId(tableGroup)
        .setInvocationId(ByteString.copyFrom(InvocationId.create().bytes()))
        .setRequestDigest(ByteString.copyFrom(digest.bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(authorization.bytes()))
        .setAuthorizationContextReference(ByteString.copyFrom(
            ServiceAuthorizationContextReference.create(identity, authorization).bytes()))
        .setExpectedOwnerEpoch(staleEpoch)
        .setRemainingTimeoutNanos(TimeUnit.SECONDS.toNanos(30))
        .setHopCount(MutationForwardRequest.MAX_HOPS)
        .setOperationBytes(ByteString.copyFrom(operation.bytes()))
        .build();
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(staleOwnerCertificate.certificate().toFile(), staleOwnerCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(forwarding.peers().stream()
            .filter(peer -> peer.port() == port).findFirst().orElseThrow().peerIdentity())
        .disableRetry().build();
    try {
      ForwardMutationResponse response = InternalOwnerServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request);
      assertEquals(ForwardMutationResponse.Outcome.UNKNOWN, response.getOutcome());
      assertEquals("UNAVAILABLE", response.getFailureCode());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertPeerCertificateWithoutConfiguredIdentityIsRejected(
      int port, String authority, Path trustedPeers, TestTlsCertificate unregisteredPeerCertificate)
      throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(unregisteredPeerCertificate.certificate().toFile(),
            unregisteredPeerCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    try {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
          () -> InternalOwnerServiceGrpc.newBlockingStub(channel)
              .withDeadlineAfter(30, TimeUnit.SECONDS)
              .forwardMutation(ForwardMutationRequest.getDefaultInstance()));
      assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ForwardMutationRequest keepAliveForwardRequest(String tableGroup,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      Rpc.LeaseKeepAliveRequest keepAlive, InvocationId invocation, long expectedOwnerEpoch)
      throws Exception {
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(keepAlive.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    ByteKey authorization = authorizationDigest(identity);
    return ForwardMutationRequest.newBuilder()
        .setTableGroupId(tableGroup)
        .setInvocationId(ByteString.copyFrom(invocation.bytes()))
        .setRequestDigest(ByteString.copyFrom(digest.bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(authorization.bytes()))
        .setAuthorizationContextReference(ByteString.copyFrom(
            ServiceAuthorizationContextReference.create(identity, authorization).bytes()))
        .setExpectedOwnerEpoch(expectedOwnerEpoch)
        .setRemainingTimeoutNanos(TimeUnit.SECONDS.toNanos(30))
        .setHopCount(MutationForwardRequest.MAX_HOPS)
        .setOperationBytes(ByteString.copyFrom(operation.bytes()))
        .build();
  }

  private static io.etcdhbase.spi.identity.AuthorizedBackendIdentity runtimeForwardingIdentity(
      String providerId, io.etcdhbase.spi.identity.AuthenticationScope scope) throws IOException {
    String subject = org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName();
    return new io.etcdhbase.spi.identity.AuthorizedBackendIdentity(providerId, subject,
        "m4-test-forwarding-context", scope, Set.of(scope.tableGroupId()), 0);
  }

  private static ForwardMutationResponse forwardMutationAndDropReply(int port, String authority,
      Path trustedPeers, TestTlsCertificate clientCertificate,
      ForwardMutationRequest request) throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(clientCertificate.certificate().toFile(), clientCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel transport = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    var receivedReply = new CountDownLatch(1);
    var droppedResponse = new AtomicReference<ForwardMutationResponse>();
    ClientInterceptor dropReply = new ClientInterceptor() {
      @Override
      public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> interceptCall(
          MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<ResponseT> responseListener, Metadata headers) {
            ClientCall<RequestT, ResponseT> interceptedCall = this;
            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
              private final AtomicBoolean terminal = new AtomicBoolean();

              @Override
              public void onMessage(ResponseT message) {
                if (message instanceof ForwardMutationResponse response) droppedResponse.set(response);
                receivedReply.countDown();
                if (terminal.compareAndSet(false, true)) {
                  responseListener.onClose(
                      Status.UNAVAILABLE.withDescription("simulated lost owner reply"), new Metadata());
                  interceptedCall.cancel("simulated lost owner reply", null);
                }
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                if (terminal.compareAndSet(false, true)) super.onClose(status, trailers);
              }
            }, headers);
          }
        };
      }
    };
    Channel dropAtClient = ClientInterceptors.intercept(transport, dropReply);
    try {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
          () -> InternalOwnerServiceGrpc.newBlockingStub(dropAtClient)
              .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request));
      assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
      assertTrue(receivedReply.await(30, TimeUnit.SECONDS),
          "the owner must finish the durable invocation before the test drops its reply");
      return java.util.Objects.requireNonNull(droppedResponse.get(),
          "the owner did not produce a response before the simulated transport loss");
    } finally {
      transport.shutdownNow();
      assertTrue(transport.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ForwardMutationResponse forwardMutation(int port, String authority,
      Path trustedPeers, TestTlsCertificate clientCertificate,
      ForwardMutationRequest request) throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(clientCertificate.certificate().toFile(), clientCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    try {
      return InternalOwnerServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request);
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ByteKey authorizationDigest(
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (var output = new DataOutputStream(bytes)) {
      output.writeInt(1);
      output.writeUTF(identity.providerId());
      output.writeUTF(identity.subject());
      output.writeUTF(identity.scope().tableGroupId());
      output.writeUTF(identity.scope().connectionProfileId());
      output.writeLong(identity.credentialGeneration());
      output.writeInt(identity.allowedTableGroupIds().size());
      for (String group : identity.allowedTableGroupIds().stream().sorted().toList()) {
        output.writeUTF(group);
      }
    }
    return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
  }

  private static InitializationConfiguration initialization(String file, int clientPort) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/j05").resolve(file);
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("namespace", "m4_" + UUID.randomUUID().toString().replace("-", ""))
        .put("quorum", "127.0.0.1").put("client_port", clientPort);
    ObjectNode system = (ObjectNode) tree.withArray("table_groups").get(0).get("system_table");
    boolean hasResultCategory = false;
    for (var category : system.withArray("record_categories")) {
      if (category.asText().equals("RESULT")) {
        hasResultCategory = true;
        break;
      }
    }
    if (!hasResultCategory) {
      system.withArray("record_categories").add("RESULT");
    }
    return new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static M3RuntimeConfiguration activation(String memberId) throws Exception {
    return activation(memberId, true);
  }

  private static M3RuntimeConfiguration activation(String memberId, boolean enableLeaseApi)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/m4/activation.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    // MiniCluster admin procedures can exceed a 400 ms heartbeat timeout under Maven load.
    tree.put("member_id", memberId).put("owner_lease_ms", TEST_OWNER_LEASE_MILLIS)
        .put("heartbeat_ms", TEST_HEARTBEAT_MILLIS)
        .put("m4_lease_api_enabled", enableLeaseApi);
    return new M3RuntimeConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static long leaseIdForPrefix(String prefix, String purpose) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256")
        .digest((prefix + ":" + purpose).getBytes(StandardCharsets.UTF_8));
    long id = ByteBuffer.wrap(digest).getLong() & Long.MAX_VALUE;
    return id == 0 ? 1 : id;
  }

  private static RunContext prepareRun() throws IOException {
    Path root = WireProcesses.repositoryRoot();
    String runId = System.getenv().getOrDefault("M4_RUN_ID", "rpc-" + UUID.randomUUID());
    if (!runId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid M4 fixture run ID");
    }
    String configuredRoot = System.getenv("M4_ARTIFACT_ROOT");
    Path artifacts = configuredRoot == null
        ? root.resolve(".artifacts/m4").resolve(runId) : Path.of(configuredRoot).toAbsolutePath();
    if (!artifacts.normalize().startsWith(root.resolve(".artifacts/m4").toAbsolutePath().normalize())
        || !artifacts.getFileName().toString().equals(runId)) {
      throw new IllegalArgumentException("M4 fixture output must be under its run directory");
    }
    Files.createDirectories(artifacts);
    return new RunContext(root, artifacts, runId,
        "etcd-hbase-m4-test/" + runId + "/", artifacts.resolve("native-trace.jsonl"));
  }

  private static RunContext prepareBindingRun() throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j41");
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "bindings/", artifacts.resolve("native-trace.jsonl"));
  }

  private static RunContext prepareKeepAliveRun() throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j42");
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "keepalive/", artifacts.resolve("native-trace.jsonl"));
  }

  private static void prepareNativeEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runDriver(run, run.artifacts().resolve("native.log"), "run",
          "--native-data-dir", run.artifacts().resolve("native-data").toString(),
          "--scope", "native-etcd", "--prefix", run.prefix(),
          "--trace", run.nativeTrace().toString(),
          "--metadata", run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void prepareNativeBindingEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runBindingDriver(run, run.artifacts().resolve("native.log"), "run",
          "--native-data-dir", run.artifacts().resolve("native-data").toString(),
          "--scope", "native-etcd", "--prefix", run.prefix(),
          "--trace", run.nativeTrace().toString(),
          "--metadata", run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void prepareNativeKeepAliveEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runDriverWithScenario(run, run.artifacts().resolve("native.log"),
          "m4-lease-keepalive.yaml", "run", "--native-data-dir",
          run.artifacts().resolve("native-data").toString(), "--scope", "native-etcd",
          "--prefix", run.prefix(), "--trace", run.nativeTrace().toString(), "--metadata",
          run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void runTargetDriver(RunContext run, String layout, int port) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriver(run, run.artifacts().resolve("target-" + suffix + ".log"), "run",
        "--endpoint", "127.0.0.1:" + port, "--scope", "target-hbase", "--layout", layout,
        "--prefix", run.prefix(), "--trace", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(),
        "--metadata", run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static void runBindingTargetDriver(RunContext run, String layout, int port) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runBindingDriver(run, run.artifacts().resolve("target-" + suffix + ".log"), "run",
        "--endpoint", "127.0.0.1:" + port, "--scope", "target-hbase", "--layout", layout,
        "--prefix", run.prefix(), "--trace", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(),
        "--metadata", run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static void runKeepAliveTargetDriver(RunContext run, String layout, int port)
      throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("target-" + suffix + ".log"),
        "m4-lease-keepalive.yaml", "run", "--endpoint", "127.0.0.1:" + port,
        "--scope", "target-hbase", "--layout", layout, "--prefix", run.prefix(), "--trace",
        run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(), "--metadata",
        run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static void compare(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriver(run, run.artifacts().resolve("compare-" + suffix + ".log"), "compare",
        "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void compareBindings(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runBindingDriver(run, run.artifacts().resolve("compare-" + suffix + ".log"), "compare-bindings",
        "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void compareKeepAlive(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("keepalive-" + suffix + ".log"),
        "m4-lease-keepalive.yaml", "compare-keepalive", "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void runDriver(RunContext run, Path log, String... arguments) throws Exception {
    runDriverWithScenario(run, log, "m4-lease.yaml", arguments);
  }

  private static void runBindingDriver(RunContext run, Path log, String... arguments) throws Exception {
    runDriverWithScenario(run, log, "m4-lease-bindings.yaml", arguments);
  }

  private static void runDriverWithScenario(RunContext run, Path log, String scenarioFile,
      String... arguments) throws Exception {
    var command = new java.util.ArrayList<String>();
    command.add(run.root().resolve("scripts/dev/go-program.sh").toString());
    command.add("m4driver");
    if (arguments.length > 0 && arguments[0].equals("run")) {
      command.addAll(List.of("run", "--scenario",
          run.root().resolve("testkit/scenarios").resolve(scenarioFile).toString()));
      command.addAll(List.of(arguments).subList(1, arguments.length));
    } else {
      command.addAll(List.of(arguments));
    }
    ProcessBuilder builder = new ProcessBuilder(command).directory(run.root().toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile());
    builder.environment().put("M4_RUN_ID", run.runId());
    String source = System.getenv("M4_SOURCE_COMMIT");
    if (source != null) builder.environment().put("M4_SOURCE_COMMIT", source);
    Process process = builder.start();
    try {
      if (!process.waitFor(180, TimeUnit.SECONDS)) {
        throw new IllegalStateException("M4 driver timed out; inspect the local run log");
      }
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        assertEquals(0, exitCode, "M4 Go driver failed; last log bytes:\n" + tail(log));
      }
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }

  private static String tail(Path log) {
    try {
      long size = Files.size(log);
      long offset = Math.max(0, size - MAX_DRIVER_DIAGNOSTIC_BYTES);
      try (var input = Files.newInputStream(log)) {
        input.skipNBytes(offset);
        byte[] bytes = input.readNBytes(MAX_DRIVER_DIAGNOSTIC_BYTES);
        return (offset == 0 ? "" : "[earlier log output omitted]\n")
            + new String(bytes, StandardCharsets.UTF_8);
      }
    } catch (IOException failure) {
      return "driver log unavailable (" + failure.getClass().getSimpleName() + ")";
    }
  }

  private record RunContext(Path root, Path artifacts, String runId, String prefix, Path nativeTrace) {}
}
