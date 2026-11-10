package io.etcdhbase.admin;

import static io.etcdhbase.admin.HBaseOperationAwaiter.OPERATION_TIMEOUT;
import static io.etcdhbase.admin.HBaseOperationAwaiter.await;

import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseMaterializationStore;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.TableName;

/** Explicit offline initialization for a newly created M3 table group. */
public final class M3TableGroupInitializer {
  private static final Duration BOOTSTRAP_TIMEOUT = Duration.ofMinutes(2);

  private M3TableGroupInitializer() {}

  /**
   * Creates new tables and installs durable metadata. The operator must exclude other bootstraps
   * and direct writes for this group; this method never adopts existing tables or retries an
   * uncertain creation. The caller owns the authorized identity and connection factory.
   */
  public static InitializationResult initializeFresh(M3TableGroupPlan plan, String confirmation,
      AuthorizedBackendIdentity identity, HBaseConnectionFactory connections,
      SnapshotPinConfiguration pinConfiguration) throws Exception {
    Objects.requireNonNull(plan, "plan");
    TableGroupInitializer.confirm(plan.source(), confirmation);
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(connections, "connections");
    Objects.requireNonNull(pinConfiguration, "pinConfiguration");
    var source = plan.source();
    var expectedScope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    if (!expectedScope.equals(identity.scope())
        || !source.identityProviderId().equals(identity.providerId())
        || !identity.allowedTableGroupIds().contains(source.tableGroupId())) {
      throw new BackendIdentityException(BackendIdentityException.Reason.INSUFFICIENT_PERMISSION);
    }
    var connection = await(connections.connectionFor(identity).toCompletableFuture(), OPERATION_TIMEOUT);
    M3FreshTableCreator.FreshTables created;
    try {
      created = M3FreshTableCreator.create(plan, new AsyncTableGroupAdmin(connection));
    } catch (M3FreshTableCreator.PartialCreationException failure) {
      throw new TableCreationException(failure.observedCreated(), source.fingerprint(), failure);
    }
    try (var scope = RequestScope.withTimeout(BOOTSTRAP_TIMEOUT)) {
      await(HBaseMaterializationStore.openForTableGroup(connections, identity, plan.system(),
          plan.primary(), plan.current(), scope), BOOTSTRAP_TIMEOUT);
      var records = await(HBaseSystemRecordStore.open(connections, identity, plan.system(), scope),
          BOOTSTRAP_TIMEOUT);
      await(SnapshotPinManager.initializeFresh(records, pinConfiguration, scope), BOOTSTRAP_TIMEOUT);
      long clusterId = await(new TableGroupIdentityService(records).initialize(scope), BOOTSTRAP_TIMEOUT);
      var control = await(HBaseCommitControlStore.open(connections, identity, plan.system(), scope),
          BOOTSTRAP_TIMEOUT);
      boolean initialized = await(control.initializeIfAbsent(
          CommitState.initial(plan.system().profileFingerprint(), 1), scope), BOOTSTRAP_TIMEOUT);
      if (!initialized) throw new IllegalStateException("fresh M3 control row already exists");
      return new InitializationResult(created.tables(), created.planFingerprint(), clusterId);
    } catch (Exception failure) {
      // Control is written last. A timed-out write may still complete; no implicit retry or
      // deletion is safe until the operator has established that prior writers have stopped.
      throw new MetadataInitializationException(created.tables(), created.planFingerprint(), failure);
    }
  }

  public static final class TableCreationException extends Exception {
    private static final long serialVersionUID = 1L;
    private final List<TableName> observedCreated;
    private final String planFingerprint;

    private TableCreationException(
        List<TableName> observedCreated, String planFingerprint, Throwable cause) {
      super("M3 table creation failed or has an unknown outcome", cause);
      this.observedCreated = List.copyOf(observedCreated);
      this.planFingerprint = planFingerprint;
    }

    public List<TableName> observedCreated() { return observedCreated; }
    public String planFingerprint() { return planFingerprint; }
  }

  public record InitializationResult(List<TableName> tables, String planFingerprint, long clusterId) {
    public InitializationResult {
      tables = List.copyOf(tables);
      Objects.requireNonNull(planFingerprint, "planFingerprint");
      if (clusterId == 0) throw new IllegalArgumentException("cluster ID must not be zero");
    }
  }

  public static final class MetadataInitializationException extends Exception {
    private static final long serialVersionUID = 1L;
    private final List<TableName> createdTables;
    private final String planFingerprint;

    private MetadataInitializationException(
        List<TableName> createdTables, String planFingerprint, Throwable cause) {
      super("M3 metadata initialization failed or has an unknown outcome", cause);
      this.createdTables = List.copyOf(createdTables);
      this.planFingerprint = planFingerprint;
    }

    public List<TableName> createdTables() { return createdTables; }
    public String planFingerprint() { return planFingerprint; }
  }
}
