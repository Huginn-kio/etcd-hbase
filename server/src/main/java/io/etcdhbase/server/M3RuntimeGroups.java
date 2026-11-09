package io.etcdhbase.server;

import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.DevelopmentHBaseConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.security.UserGroupInformation;

/** Owns activated local-development group runtimes and their verified HBase connections. */
final class M3RuntimeGroups implements AutoCloseable {
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m3-group-cleanup").start(task);
  private final Map<String, ManagedGroup> groups;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  private M3RuntimeGroups(Map<String, ManagedGroup> groups) { this.groups = Map.copyOf(groups); }

  static M3RuntimeGroups open(RuntimeConfiguration configuration, M3RuntimeConfiguration activation)
      throws IOException {
    var budgets = M3RuntimeBudgets.create(configuration, activation);
    Map<String, ManagedGroup> opened = new LinkedHashMap<>();
    try {
      for (String group : configuration.listeners().stream()
          .map(RuntimeConfiguration.Listener::tableGroupId).distinct().toList()) {
        var source = TableGroupPlanFactory.create(configuration.initialization(), group, null);
        var plan = M3TableGroupPlan.from(source);
        if (source.hbaseProfile().authenticationMode() != AuthenticationMode.DEV_NO_AUTH
            || !source.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID)) {
          throw new IllegalArgumentException("configured target identity activation is not available");
        }
        ManagedGroup managed = openGroup(plan, configuration, activation, budgets);
        try {
          opened.put(group, managed);
        } catch (RuntimeException | Error failure) {
          try { managed.runtime().close(); }
          catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          awaitCleanup(closeAfter(managed.runtime().drained(), managed.provider()),
              activation.shutdownTimeoutMs(), failure);
          throw failure;
        }
      }
      return new M3RuntimeGroups(opened);
    } catch (Exception | Error failure) {
      var rollback = new M3RuntimeGroups(opened);
      try { rollback.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      awaitCleanup(rollback.drained, activation.shutdownTimeoutMs(), failure);
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof RuntimeException invalid) throw invalid;
      if (failure instanceof IOException io) throw io;
      throw new IOException("M3 table-group activation failed", failure);
    }
  }

  private static ManagedGroup openGroup(M3TableGroupPlan plan, RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation, io.etcdhbase.core.scan.BudgetManager budgets) throws Exception {
    var source = plan.source();
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    var timeout = Duration.ofMillis(Math.min(activation.startupTimeoutMs(), 300_000));
    var provider = new HBaseBackendIdentityProvider(source.identityProviderId(), scope,
        DevelopmentHBaseConfiguration.create(source.hbaseProfile()), HBaseAuthenticators.simpleDevelopment(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        1, 1, timeout);
    boolean deferred = false;
    try {
      String principal = UserGroupInformation.getCurrentUser().getShortUserName();
      try (var credentials = new BackendCredentials(principal, new char[0]);
           var startup = RequestScope.withTimeout(Duration.ofMillis(activation.startupTimeoutMs()))) {
        var identity = provider.authenticate(credentials, scope).toCompletableFuture()
            .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
        try {
          var runtime = M3TableGroupRuntime.open(plan, activation, configuration.resources(),
              provider, identity, budgets, startup);
          try {
            return new ManagedGroup(runtime, provider);
          } catch (RuntimeException | Error failure) {
            deferred = true;
            try { runtime.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            awaitCleanup(closeAfter(runtime.drained(), provider), activation.shutdownTimeoutMs(), failure);
            throw failure;
          }
        } catch (M3TableGroupRuntime.StartupException failure) {
          deferred = true;
          awaitCleanup(closeAfter(failure.cleanup(), provider), activation.shutdownTimeoutMs(), failure);
          throw failure;
        } catch (Error fatal) {
          for (Throwable suppressed : fatal.getSuppressed()) {
            if (suppressed instanceof M3TableGroupRuntime.StartupCleanup cleanup) {
              deferred = true;
              awaitCleanup(closeAfter(cleanup.cleanup(), provider), activation.shutdownTimeoutMs(), fatal);
              break;
            }
          }
          throw fatal;
        }
      }
    } catch (Exception | Error failure) {
      if (!deferred) {
        try { provider.close(); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      }
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw failure;
    }
  }

  KvService service(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().putService();
  }

  RequestMetrics metrics(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().metrics();
  }

  boolean isReady() {
    return !closing.get() && !groups.isEmpty() && groups.values().stream().allMatch(value -> value.runtime().isReady());
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  @Override public void close() {
    if (!closing.compareAndSet(false, true)) return;
    List<CompletableFuture<Void>> cleanup = new ArrayList<>();
    for (ManagedGroup group : groups.values()) {
      try {
        group.runtime().close();
      } catch (RuntimeException | Error failure) {
        cleanup.add(CompletableFuture.failedFuture(failure));
      }
      cleanup.add(closeAfter(group.runtime().drained(), group.provider()));
    }
    CompletableFuture.allOf(cleanup.toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    });
  }

  private static CompletableFuture<Void> closeAfter(CompletionStage<Void> completion,
      HBaseBackendIdentityProvider provider) {
    return completion.toCompletableFuture().thenRunAsync(provider::close, CLEANUP);
  }

  private static void awaitCleanup(CompletableFuture<Void> cleanup, int timeoutMs, Throwable primary) {
    try {
      cleanup.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      if (failure != primary) primary.addSuppressed(failure);
    }
  }

  private record ManagedGroup(M3TableGroupRuntime runtime, HBaseBackendIdentityProvider provider) {}
}
