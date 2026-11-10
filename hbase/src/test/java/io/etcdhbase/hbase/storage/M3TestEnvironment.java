package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/** Disposable, development-identity fixture shared with the server's M3 integration tests. */
public final class M3TestEnvironment implements AutoCloseable {
  private static final byte[] SYSTEM_FAMILY = {'s'};
  private static final byte[] DATA_FAMILY = {'d'};
  private static final String GROUP = "m1";
  private static final String SALT_PROFILE = "m3-fixture-salt";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);
  private static final SnapshotPinConfiguration PIN_CONFIGURATION =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));

  private final M1TestEnvironment environment;
  private final HashSaltStrategy salt;
  private final SystemTableBinding system;
  private final StorageTableBinding primary;
  private final Optional<StorageTableBinding> current;
  private final SystemRecordStore records;
  private final HBaseCommitControlStore control;
  private final HBaseMaterializationStore materialization;
  private final SnapshotPinManager pins;
  private final long clusterId;

  private M3TestEnvironment(M1TestEnvironment environment, HashSaltStrategy salt,
      SystemTableBinding system, StorageTableBinding primary, Optional<StorageTableBinding> current,
      SystemRecordStore records, HBaseCommitControlStore control,
      HBaseMaterializationStore materialization, SnapshotPinManager pins, long clusterId) {
    this.environment = environment;
    this.salt = salt;
    this.system = system;
    this.primary = primary;
    this.current = current;
    this.records = records;
    this.control = control;
    this.materialization = materialization;
    this.pins = pins;
    this.clusterId = clusterId;
  }

  /** Provisions fresh tables only; never adopts or modifies an existing table group. */
  public static M3TestEnvironment start(StorageTableBinding.Layout layout) throws Exception {
    return start(layout, HashSaltStrategy.PUBLIC_PROFILE.bucketCount());
  }

  /** The bucket count is a disposable public test profile, not a target salt decision. */
  public static M3TestEnvironment start(StorageTableBinding.Layout layout, int bucketCount)
      throws Exception {
    if (layout != StorageTableBinding.Layout.HISTORY && layout != StorageTableBinding.Layout.NATIVE) {
      throw new IllegalArgumentException("M3 fixture requires HISTORY or NATIVE layout");
    }
    var salt = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, bucketCount, 2));
    M1TestEnvironment environment = M1TestEnvironment.start(1);
    try {
      return provision(environment, layout, salt);
    } catch (Exception | Error failure) {
      try {
        environment.close();
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static M3TestEnvironment provision(M1TestEnvironment environment,
      StorageTableBinding.Layout layout, HashSaltStrategy salt) throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    SystemTableBinding system = new SystemTableBinding(TableName.valueOf("m3_sys_" + suffix),
        SYSTEM_FAMILY, GROUP, SALT_PROFILE, salt);
    StorageTableBinding primary = new StorageTableBinding(TableName.valueOf("m3_primary_" + suffix),
        DATA_FAMILY, layout, salt);
    Optional<StorageTableBinding> current = layout == StorageTableBinding.Layout.HISTORY
        ? Optional.of(new StorageTableBinding(TableName.valueOf("m3_current_" + suffix),
            DATA_FAMILY, StorageTableBinding.Layout.CURRENT, salt))
        : Optional.empty();
    byte[][] splits = salt.splitKeys().toArray(byte[][]::new);
    String clusterId = M1TestEnvironment.await(
        environment.connection().getAdmin().getClusterMetrics()).getClusterId();
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalStateException("fixture cluster ID is missing");
    }
    String plan = suffix + suffix;
    environment.create(system.table(), ownedDescriptor(
        systemDescriptor(system, salt), clusterId, plan, layout), splits);
    environment.create(primary.table(), ownedDescriptor(primary.descriptor(), clusterId, plan, layout), splits);
    if (current.isPresent()) {
      StorageTableBinding currentBinding = current.orElseThrow();
      environment.create(currentBinding.table(),
          ownedDescriptor(currentBinding.descriptor(), clusterId, plan, layout), splits);
    }

    try (RequestScope scope = RequestScope.withTimeout(STARTUP_TIMEOUT)) {
      SystemRecordStore records = M1TestEnvironment.await(HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), system, scope));
      HBaseCommitControlStore control = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), system, scope));
      if (!M1TestEnvironment.await(control.initializeIfAbsent(
          CommitState.initial(system.profileFingerprint(), 1), scope))) {
        throw new IllegalStateException("fresh fixture control row was already initialized");
      }
      // The randomly named table is new and this fixture is its only initializer.
      SnapshotPinManager pins = M1TestEnvironment.await(
          SnapshotPinManager.initializeFresh(records, PIN_CONFIGURATION, scope));
      long groupClusterId = M1TestEnvironment.await(new TableGroupIdentityService(records).initialize(scope));
      HBaseMaterializationStore materialization = M1TestEnvironment.await(
          HBaseMaterializationStore.open(environment.factory(), environment.identity(), primary,
              current, scope));
      return new M3TestEnvironment(environment, salt, system, primary, current, records,
          control, materialization, pins, groupClusterId);
    }
  }

  private static TableDescriptor systemDescriptor(SystemTableBinding binding, HashSaltStrategy salt) {
    return TableDescriptorBuilder.newBuilder(binding.table())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(SYSTEM_FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
  }

  private static TableDescriptor ownedDescriptor(TableDescriptor descriptor, String clusterId,
      String plan, StorageTableBinding.Layout layout) {
    return TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.table-group-id", GROUP)
        .setValue("etcdhbase.m0.backend-cluster-id", clusterId)
        .setValue("etcdhbase.m0.plan-fingerprint", plan)
        .setValue("etcdhbase.m0.history-layout", layout == StorageTableBinding.Layout.HISTORY
            ? "SEPARATE_HISTORY_TABLE" : "NATIVE_CELL_VERSIONS")
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.schema-version", "1")
        .build();
  }

  public HBaseRecordStore openPrimary(RequestScope scope) throws Exception {
    return environment.store(primary, scope);
  }

  /** Creates a format-compatible foreign table to check table-group startup isolation. */
  public StorageTableBinding createForeignPrimary() throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    var foreign = new StorageTableBinding(TableName.valueOf("m3_foreign_" + suffix),
        DATA_FAMILY, primary.layout(), salt);
    TableDescriptor systemTable = M1TestEnvironment.await(
        environment.connection().getAdmin().getDescriptor(system.table()));
    var descriptor = TableDescriptorBuilder.newBuilder(ownedDescriptor(foreign.descriptor(),
        systemTable.getValue("etcdhbase.m0.backend-cluster-id"),
        systemTable.getValue("etcdhbase.m0.plan-fingerprint"), primary.layout()))
        .setValue("etcdhbase.m0.table-group-id", "foreign")
        .build();
    environment.create(foreign.table(), descriptor, salt.splitKeys().toArray(byte[][]::new));
    return foreign;
  }

  public HBaseConnectionFactory factory() { return environment.factory(); }
  public AuthorizedBackendIdentity identity() { return environment.identity(); }
  public HashSaltStrategy salt() { return salt; }
  public String saltProfileId() { return SALT_PROFILE; }
  public SystemTableBinding system() { return system; }
  public StorageTableBinding primary() { return primary; }
  public Optional<StorageTableBinding> current() { return current; }
  public SystemRecordStore records() { return records; }
  public HBaseCommitControlStore control() { return control; }
  public HBaseMaterializationStore materialization() { return materialization; }
  public SnapshotPinManager pins() { return pins; }
  public long clusterId() { return clusterId; }

  /** Exports only the disposable MiniCluster client config to a caller-owned temporary file. */
  public void writeClientConfiguration(Path destination) throws Exception {
    try (var output = Files.newOutputStream(destination, StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE)) {
      environment.clientConfiguration().writeXml(output);
    }
  }

  @Override
  public void close() throws Exception {
    environment.close();
  }
}
