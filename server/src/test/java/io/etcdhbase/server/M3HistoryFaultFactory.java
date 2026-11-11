package io.etcdhbase.server;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Put;

/** Test-only HBase seam for durable preparation, publication, or materialization acknowledgements. */
final class M3HistoryFaultFactory implements HBaseConnectionFactory {
  private final HBaseConnectionFactory delegate;
  private final M3TableGroupPlan plan;
  private final AtomicBoolean armed;
  private final AtomicBoolean fired = new AtomicBoolean();
  private final Path marker;
  private final M3HistoryFaultMode mode;

  M3HistoryFaultFactory(HBaseConnectionFactory delegate, M3TableGroupPlan plan,
      AtomicBoolean armed, Path marker, M3HistoryFaultMode mode) {
    this.delegate = delegate;
    this.plan = plan;
    this.armed = armed;
    this.marker = marker;
    this.mode = mode;
  }

  @Override public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    return delegate.connectionFor(identity).thenApply(connection -> (AsyncConnection) Proxy.newProxyInstance(
        AsyncConnection.class.getClassLoader(), new Class<?>[] {AsyncConnection.class}, (proxy, method, args) -> {
          Object result = invoke(connection, method, args);
          if (method.getName().equals("getTableBuilder") && args != null && args.length > 0
              && args[0] instanceof TableName table
              && (plan.system().table().equals(table) || plan.primary().table().equals(table))) {
            return tableBuilder((AsyncTableBuilder<?>) result, table);
          }
          return result;
        }));
  }

  // The surrounding fixture owns and closes the delegate.
  @Override public void close() {}

  private Object tableBuilder(AsyncTableBuilder<?> builder, TableName name) {
    return Proxy.newProxyInstance(AsyncTableBuilder.class.getClassLoader(),
        new Class<?>[] {AsyncTableBuilder.class}, (proxy, method, args) -> {
          Object result = invoke(builder, method, args);
          if (method.getName().equals("build")) return table((AsyncTable<?>) result, name);
          return result == builder ? proxy : result;
        });
  }

  private Object table(AsyncTable<?> table, TableName name) {
    return Proxy.newProxyInstance(AsyncTable.class.getClassLoader(), new Class<?>[] {AsyncTable.class},
        (proxy, method, args) -> {
          boolean shouldHold = method.getName().equals("checkAndMutate") && args != null && args.length == 1
              && args[0] instanceof CheckAndMutate mutation && targetedMutation(name, mutation);
          Object result = invoke(table, method, args);
          if (shouldHold) {
            @SuppressWarnings("unchecked")
            var response = (CompletableFuture<CheckAndMutateResult>) result;
            return response.thenCompose(this::holdAcknowledgement);
          }
          return result;
        });
  }

  private CompletableFuture<CheckAndMutateResult> holdAcknowledgement(CheckAndMutateResult actual) {
    if (!actual.isSuccess() || !armed.get() || !fired.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(actual);
    }
    try {
      Files.writeString(marker, mode.wire());
      // The physical CAS completed; the caller cannot receive its ACK before the barrier is released.
      return mode.holdsAcknowledgementForRelease()
          ? waitForRelease(actual) : new CompletableFuture<>();
    } catch (java.io.IOException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private boolean targetedMutation(TableName table, CheckAndMutate mutation) {
    if (!armed.get()) return false;
    if (mode == M3HistoryFaultMode.PARTIAL_MATERIALIZATION) return plan.primary().table().equals(table);
    if (!plan.system().table().equals(table)) return false;
    if (mode == M3HistoryFaultMode.CAS_ACK_LOST || mode.holdsAcknowledgementForRelease()) {
      return selectedControl(mutation);
    }
    SystemRecordType type = plan.system().keyCodec().decode(mutation.getRow()).type();
    if (mode == M3HistoryFaultMode.CANDIDATE_PREPARATION) return type == SystemRecordType.CANDIDATE;
    if (mode == M3HistoryFaultMode.RESULT_PREPARATION) return type == SystemRecordType.RESULT;
    if (mode != M3HistoryFaultMode.SEALED_BEFORE_PUBLISH || type != SystemRecordType.INVOCATION
        || !(mutation.getAction() instanceof Put put)) return false;
    for (var cells : put.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        if (CellUtil.matchingQualifier(cell, new byte[] {'r'})) {
          return InvocationMessages.decode(CellUtil.cloneValue(cell)).state() == InvocationBinding.State.SEALED;
        }
      }
    }
    return false;
  }

  private CompletableFuture<CheckAndMutateResult> waitForRelease(CheckAndMutateResult actual) {
    var pending = new CompletableFuture<CheckAndMutateResult>();
    Thread.ofVirtual().name("m3-old-owner-ack").start(() -> {
      long deadline = System.nanoTime() + java.time.Duration.ofMinutes(2).toNanos();
      try {
        while (!Files.exists(marker.resolveSibling("release-old-ack"))) {
          if (System.nanoTime() >= deadline) throw new IllegalStateException("old owner ACK release timed out");
          Thread.sleep(25);
        }
        pending.complete(actual);
        Files.writeString(marker.resolveSibling("old-ack-delivered"), "delivered");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        pending.completeExceptionally(interrupted);
      } catch (Exception failure) {
        pending.completeExceptionally(failure);
      }
    });
    return pending;
  }

  private boolean selectedControl(CheckAndMutate mutation) {
    if (!armed.get() || !Arrays.equals(mutation.getRow(), plan.system().controlRow())
        || !(mutation.getAction() instanceof Put put)) return false;
    for (var cells : put.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        if (CellUtil.matchingQualifier(cell, new byte[] {'s'})) {
          var replacement = CommitControlMessages.decode(CellUtil.cloneValue(cell));
          byte[] expected = mutation.getValue();
          return expected != null && replacement.publication().isPresent()
              && replacement.publishedSequence() > CommitControlMessages.decode(expected).publishedSequence();
        }
      }
    }
    return false;
  }

  private static Object invoke(Object receiver, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(receiver, args);
    } catch (InvocationTargetException wrapped) {
      throw wrapped.getCause();
    }
  }
}
