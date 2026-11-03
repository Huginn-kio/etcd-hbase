package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventDirectoryPage;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestDirectoryPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotGcBoundary;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** MiniCluster evidence for durable cross-manager snapshot pins and conservative GC bounds. */
class M2SnapshotPinIT {
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));
  private static final byte[] FAMILY = {'s'};
  private static final ByteKey AUTHORIZATION = digest(9);
  private static final SnapshotPinConfiguration CONFIGURATION =
      new SnapshotPinConfiguration(4, 4, Duration.ofSeconds(1));

  @Test
  void independentManagersShareDurablePinsFloorsAndExpiryFencing() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      String suffix = UUID.randomUUID().toString().replace("-", "");
      TableName table = TableName.valueOf("m2_pin_" + suffix);
      String tableGroup = "m2-pin-" + suffix;
      String saltProfile = "m2-pin-salt-" + suffix;
      SystemTableBinding binding = new SystemTableBinding(table, FAMILY,
          tableGroup, saltProfile, SALT);
      environment.create(table, descriptor(binding, saltProfile),
          SALT.splitKeys().toArray(byte[][]::new));

      SystemRecordStore firstStore = openStore(environment, binding, scope);
      SystemRecordStore secondStore = openStore(environment, binding, scope);
      MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
      SnapshotPinManager first = openManager(firstStore, clock, scope);
      SnapshotPinManager second = openManager(secondStore, clock, scope);

      PinnedView old = openView(binding, first, 5, scope);
      SnapshotGcBoundary observedBySecond = second.readBoundary(scope).join();
      assertEquals(1, observedBySecond.protections().size());
      assertEquals(0, observedBySecond.safeRevision(), "GC stays disabled until a floor is persisted");

      SnapshotGcBoundary advanced = second.advanceRegistrationFloors(8, 4, scope).join();
      assertEquals(5, advanced.safeRevision());
      assertEquals(4, advanced.safeSequence());
      assertEquals(1, advanced.protections().size());
      old.close();
      SnapshotGcBoundary released = second.readBoundary(scope).join();
      assertEquals(8, released.safeRevision());
      assertTrue(released.protections().isEmpty());
      assertInstanceOf(IllegalStateException.class,
          failure(() -> second.register(old.view(), scope).join()));

      PinnedView current = openView(binding, first, 8, scope);
      ReadViewPinRegistry.Pin expiring = first.register(current.view(), scope).join();
      current.close();
      clock.advance(Duration.ofSeconds(2));
      SnapshotGcBoundary expiredStillProtected = second.readBoundary(scope).join();
      assertEquals(8, expiredStillProtected.safeRevision());
      assertEquals(1, expiredStillProtected.protections().size());
      SnapshotGcBoundary revoked = second.revokeExpiredPins(scope).join();
      assertTrue(revoked.protections().isEmpty());
      assertEquals(8, revoked.safeRevision());
      BackendFailure renewalFailure = assertInstanceOf(BackendFailure.class,
          failure(() -> expiring.renew(scope).join()));
      assertEquals(BackendFailure.Category.CANCELLED, renewalFailure.category());

      SnapshotPinManager restarted = openManager(openStore(environment, binding, scope), clock, scope);
      SnapshotGcBoundary afterRestart = restarted.readBoundary(scope).join();
      assertEquals(8, afterRestart.safeRevision());
      assertEquals(4, afterRestart.safeSequence());
      assertTrue(afterRestart.protections().isEmpty());
      System.out.println("j26-snapshot-pin sharedRows=true floorsDurable=true expiryRequiresRevocation=true");
    }
  }

  private static PinnedView openView(SystemTableBinding binding, SnapshotPinManager pins,
      long revision, RequestScope scope) {
    CommitState state = CommitState.initial(binding.profileFingerprint(), revision);
    ReadViewService views = new ReadViewService(new StaticControl(state),
        new ManifestCatalog(new EmptyManifestRepository()), pins, binding.profileFingerprint());
    return new PinnedView(views.open(OptionalLong.empty(), AUTHORIZATION, scope).join());
  }

  private static SystemRecordStore openStore(M1TestEnvironment environment,
      SystemTableBinding binding, RequestScope scope) throws Exception {
    return M1TestEnvironment.await(HBaseSystemRecordStore.open(
        environment.factory(), environment.identity(), binding, scope));
  }

  private static SnapshotPinManager openManager(SystemRecordStore records, Clock clock,
      RequestScope scope) {
    return SnapshotPinManager.open(records, CONFIGURATION, clock, scope).join();
  }

  private static org.apache.hadoop.hbase.client.TableDescriptor descriptor(
      SystemTableBinding binding, String saltProfile) {
    return TableDescriptorBuilder.newBuilder(binding.table())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", binding.tableGroupId())
        .setValue("etcdhbase.m0.salt-profile-id", saltProfile)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count",
            Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes",
            Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN")
        .build();
  }

  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }

  private static Throwable failure(Runnable action) {
    try {
      action.run();
    } catch (CompletionException failure) {
      Throwable cause = failure.getCause();
      return cause == null ? failure : cause;
    }
    throw new AssertionError("expected operation to fail");
  }

  private record PinnedView(io.etcdhbase.core.commit.PinnedReadView pinned) {
    private io.etcdhbase.core.commit.ReadView view() { return pinned.view(); }
    private void close() throws Exception {
      pinned.close();
      pinned.termination().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
  }

  private static final class MutableClock extends Clock {
    private Instant current;
    private MutableClock(Instant current) { this.current = current; }
    private void advance(Duration duration) { current = current.plus(duration); }
    @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
    @Override public Clock withZone(ZoneId zone) { return this; }
    @Override public Instant instant() { return current; }
  }

  private static final class StaticControl implements CommitControlStore {
    private final CommitState state;
    private StaticControl(CommitState state) { this.state = state; }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState,
        RequestScope scope) { return CompletableFuture.completedFuture(false); }
    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed,
        CommitState replacement, RequestScope scope) { return CompletableFuture.completedFuture(false); }
  }

  private static final class EmptyManifestRepository implements ManifestRepository {
    @Override public int candidateBucket(PreparedCandidate candidate) {
      throw new UnsupportedOperationException("no selected candidate is expected");
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot root, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    private static <T> CompletableFuture<T> unsupported() {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("unexpected manifest write"));
    }
  }
}
