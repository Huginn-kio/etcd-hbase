package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.ManualRequestClock;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanCleanupFailure;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoragePage;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ScanSchedulerTest {
  private static final BudgetManager.Limits LIMITS = new BudgetManager.Limits(8, 32, 1, 1, 1, 65536, 1_000_000);
  private static final BudgetManager.Reservation MEMORY = new BudgetManager.Reservation(8192, 512, 4096, 8192);
  private static final StoragePage EMPTY = new StoragePage(List.of(), true, new StoragePage.Cost(0, 0, 1));

  @Test
  void queuedDeadlineRemainsTimeoutWhenCapacityReturnsBeforeTheTimerRuns() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var clock = new ManualRequestClock();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(30), Runnable::run, timer);
        var firstScope = RequestScope.withoutDeadline(); var first = budget.admit("alpha", firstScope, MEMORY);
        var timedScope = clock.scope(Duration.ofSeconds(10)); var timed = budget.admit("alpha", timedScope, MEMORY)) {
      var active = scheduler.bind(physical, first).startScan(request(0));
      var waiting = scheduler.bind(physical, timed).startScan(request(1));
      assertEquals(1, physical.started.size());
      clock.advance(Duration.ofSeconds(20));
      physical.started.getFirst().result.complete(EMPTY);
      assertFailureCategory(BackendFailure.Category.TIMEOUT, waiting);
      assertEquals(1, physical.started.size(), "expired work must not start a scanner");
      active.close();
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void successfulPhysicalCompletionAfterDeadlineCannotBecomeCancellation() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var clock = new ManualRequestClock();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(30), Runnable::run, timer);
        var scope = clock.scope(Duration.ofSeconds(10)); var account = budget.admit("alpha", scope, MEMORY)) {
      var operation = scheduler.bind(physical, account).startScan(request(0));
      clock.advance(Duration.ofSeconds(20));
      physical.started.getFirst().result.complete(EMPTY);
      assertFailureCategory(BackendFailure.Category.TIMEOUT, operation);
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void alreadyQueuedReadsKeepResourceLimitAfterAnotherReadExhaustsTheAccount() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(30), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY,
            new BudgetManager.ReadLimits(32, 4096, 32, 1, 1))) {
      var source = scheduler.bind(physical, account);
      var first = source.startScan(request(0));
      var limitTrigger = source.startScan(request(1));
      var waiting = source.startScan(request(2));
      physical.started.getFirst().result.complete(EMPTY);
      first.close();
      assertEquals(2, physical.started.size());
      physical.started.get(1).result.complete(EMPTY);
      assertFailureCategory(BackendFailure.Category.RESOURCE_LIMIT, limitTrigger);
      assertFailureCategory(BackendFailure.Category.RESOURCE_LIMIT, waiting);
      assertEquals(2, physical.started.size(), "exhausted work must not start a replacement scanner");
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void explicitCancellationAndOriginalBackendFailureKeepTheirOwnMeaning() throws Exception {
    for (boolean backendFailed : List.of(false, true)) {
      var budget = budget();
      var physical = new ControlledSource();
      var clock = new ManualRequestClock();
      try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(30), Runnable::run, timer);
          var scope = clock.scope(Duration.ofSeconds(10)); var account = budget.admit("alpha", scope, MEMORY)) {
        var operation = scheduler.bind(physical, account).startScan(request(0));
        operation.close();
        clock.advance(Duration.ofSeconds(20));
        if (backendFailed) {
          physical.started.getFirst().result.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        } else {
          physical.started.getFirst().result.complete(EMPTY);
        }
        assertFailureCategory(backendFailed ? BackendFailure.Category.UNAVAILABLE : BackendFailure.Category.CANCELLED, operation);
        assertEquals(0, budget.snapshot().scanners());
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void sixteenBucketsProgressWithOnePhysicalScannerAndNoHeldScannerPerHead() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var source = scheduler.bind(physical, account);
      List<ScanOperation> pages = new ArrayList<>();
      for (int bucket = 0; bucket < 16; bucket++) pages.add(source.startScan(request(bucket)));
      assertEquals(1, physical.started.size());
      for (int bucket = 0; bucket < 16; bucket++) {
        assertEquals(1, budget.snapshot().scanners());
        physical.started.get(bucket).result.complete(EMPTY);
        pages.get(bucket).completion().toCompletableFuture().join();
        assertEquals(0, budget.snapshot().scanners());
        assertEquals(bucket + 1, physical.started.size(), "retained page holds bytes, never a scanner");
        pages.get(bucket).close();
      }
      assertEquals(0, budget.snapshot().queued());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
    assertEquals(0, budget.snapshot().requests());
  }

  @Test
  void cancellingARunningReadCannotReleasePhysicalCapacityBeforeCleanupCompletes() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var operation = scheduler.bind(physical, account).startScan(request(0));
      operation.close();
      assertTrue(physical.started.getFirst().closed);
      assertFalse(operation.completion().toCompletableFuture().isDone());
      assertEquals(1, budget.snapshot().scanners());
      physical.started.getFirst().result.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
      assertThrows(CompletionException.class, () -> operation.completion().toCompletableFuture().join());
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertFalse(scope.isClosed());
    }
  }

  @Test
  void queueExpirationIsBoundedAndDoesNotOpenAnotherScanner() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var source = scheduler.bind(physical, account);
      var first = source.startScan(request(0));
      var waiting = source.startScan(request(1));
      timer.expireAll();
      var failure = assertThrows(CompletionException.class, () -> waiting.completion().toCompletableFuture().join());
      assertEquals(BackendFailure.Category.TIMEOUT, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, physical.started.size());
      first.close();
      physical.started.getFirst().result.complete(EMPTY);
    }
  }

  @Test
  void fairDispatchRotatesGroupsAndRequestsWhileAnotherPageIsRetained() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var firstScope = RequestScope.withoutDeadline(); var first = budget.admit("alpha", firstScope, MEMORY);
        var secondScope = RequestScope.withoutDeadline(); var second = budget.admit("alpha", secondScope, MEMORY);
        var thirdScope = RequestScope.withoutDeadline(); var third = budget.admit("beta", thirdScope, MEMORY)) {
      var alpha = scheduler.bind(physical, first);
      var held = alpha.startScan(request(0));
      var sameRequest = alpha.startScan(request(1));
      var otherRequest = scheduler.bind(physical, second).startScan(request(2));
      var otherGroup = scheduler.bind(physical, third).startScan(request(3));
      physical.started.getFirst().result.complete(EMPTY);
      assertEquals(2, physical.started.size());
      assertTrue(physical.started.get(1).bucket >= 2, "a retained page cannot block independent requests");
      physical.started.get(1).result.complete(EMPTY);
      assertEquals(3, physical.started.size());
      assertEquals(List.of(2, 3), physical.started.subList(1, 3).stream().map(operation -> operation.bucket).sorted().toList());
      physical.started.get(2).result.complete(EMPTY);
      otherRequest.close();
      otherGroup.close();
      held.close();
      assertEquals(1, physical.started.getLast().bucket);
      physical.started.getLast().result.complete(EMPTY);
      sameRequest.close();
    }
  }

  @Test
  void queueFullAndTotalReadExhaustionReturnErrorsRatherThanPartialSuccess() throws Exception {
    var limits = new BudgetManager.Limits(2, 1, 1, 1, 1, 65536, 1_000_000);
    var budget = new BudgetManager(limits, Map.of("alpha", limits), limits);
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY,
            new BudgetManager.ReadLimits(32, 1024, 1, 1, 1))) {
      var source = scheduler.bind(physical, account);
      var first = source.startScan(request(0));
      var queued = source.startScan(request(1));
      var rejected = source.startScan(request(2));
      var failure = assertThrows(CompletionException.class, () -> rejected.completion().toCompletableFuture().join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      physical.started.getFirst().result.complete(EMPTY);
      first.close();
      physical.started.getLast().result.complete(EMPTY);
      var exhausted = assertThrows(CompletionException.class, () -> queued.completion().toCompletableFuture().join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) exhausted.getCause()).category());
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void cleanupFailureRetiresItsCapacityRatherThanOpeningUnboundedReplacementScanners() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var source = scheduler.bind(physical, account);
      var failed = source.startScan(request(0));
      physical.started.getFirst().result.completeExceptionally(new ScanCleanupFailure(new IllegalStateException("synthetic")));
      assertThrows(CompletionException.class, () -> failed.completion().toCompletableFuture().join());
      assertEquals(1, budget.snapshot().retiredScanners());
      assertEquals(1, budget.snapshot().scanners());
      var waiting = source.startScan(request(1));
      timer.expireAll();
      assertThrows(CompletionException.class, () -> waiting.completion().toCompletableFuture().join());
      assertEquals(1, physical.started.size());
    }
  }

  @Test
  void aCleanupMarkerPreservesThePrimaryFailureAndStillRetiresCapacity() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var cancellation = new CancellationException();
    var failure = new BackendFailure(BackendFailure.Category.CANCELLED, cancellation);
    failure.addSuppressed(new ScanCleanupFailure(new IllegalStateException("synthetic cleanup failure")));
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var source = scheduler.bind(physical, account);
      var failed = source.startScan(request(0));
      physical.started.getFirst().result.completeExceptionally(failure);
      var delivered = assertThrows(CompletionException.class, () -> failed.completion().toCompletableFuture().join());
      assertSame(failure, delivered.getCause());
      assertSame(cancellation, failure.getCause());
      assertEquals(1, budget.snapshot().retiredScanners());
      assertEquals(1, budget.snapshot().scanners());
      var waiting = source.startScan(request(1));
      timer.expireAll();
      assertThrows(CompletionException.class, () -> waiting.completion().toCompletableFuture().join());
      assertEquals(1, physical.started.size());
      scheduler.close();
      var termination = assertThrows(CompletionException.class,
          () -> scheduler.termination().toCompletableFuture().join());
      assertSame(failure, termination.getCause());
    }
  }

  @Test
  void closeFailureIsSuppressedOnKnownBackendFailureAndStillRetiresCapacity() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var closeFailure = new IllegalStateException("synthetic close failure");
    physical.closeFailure = closeFailure;
    var timeout = new BackendFailure(BackendFailure.Category.TIMEOUT);
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var source = scheduler.bind(physical, account);
      var failed = source.startScan(request(0));
      physical.started.getFirst().result.completeExceptionally(timeout);

      var delivered = assertThrows(CompletionException.class, () -> failed.completion().toCompletableFuture().join());
      assertTrue(physical.started.getFirst().closed);
      assertSame(timeout, delivered.getCause());
      assertEquals(BackendFailure.Category.TIMEOUT, ((BackendFailure) delivered.getCause()).category());
      assertEquals(1, delivered.getSuppressed().length);
      assertTrue(delivered.getSuppressed()[0] instanceof ScanCleanupFailure);
      assertSame(closeFailure, delivered.getSuppressed()[0].getCause());
      assertEquals(1, budget.snapshot().retiredScanners());
      assertEquals(1, budget.snapshot().scanners());

      var waiting = source.startScan(request(1));
      timer.expireAll();
      assertFailureCategory(BackendFailure.Category.TIMEOUT, waiting);
      assertEquals(1, physical.started.size(), "retired capacity must not start a replacement scanner");

      scheduler.close();
      var termination = assertThrows(CompletionException.class,
          () -> scheduler.termination().toCompletableFuture().join());
      assertSame(timeout, termination.getCause());
      assertEquals(1, termination.getSuppressed().length);
      assertTrue(termination.getSuppressed()[0] instanceof ScanCleanupFailure);
      assertSame(closeFailure, termination.getSuppressed()[0].getCause());
    }
  }

  @Test
  void schedulerCloseWaitsForInProgressPhysicalCleanupBeforeTermination() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var closeFailure = new IllegalStateException("synthetic close failure");
    var timeout = new BackendFailure(BackendFailure.Category.TIMEOUT);
    var closeStarted = new CountDownLatch(1);
    var releaseClose = new CountDownLatch(1);
    physical.closeFailure = closeFailure;
    physical.closeStarted = closeStarted;
    physical.releaseClose = releaseClose;
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var failed = scheduler.bind(physical, account).startScan(request(0));
      var completing = Thread.ofPlatform().start(() -> physical.started.getFirst().result.completeExceptionally(timeout));
      try {
        assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
        scheduler.close();
        assertFalse(failed.completion().toCompletableFuture().isDone());
        assertFalse(scheduler.termination().toCompletableFuture().isDone());
      } finally {
        releaseClose.countDown();
        completing.join(TimeUnit.SECONDS.toMillis(5));
      }

      assertFalse(completing.isAlive(), "physical completion callback must finish");
      var delivered = assertThrows(CompletionException.class, () -> failed.completion().toCompletableFuture().join());
      assertSame(timeout, delivered.getCause());
      assertEquals(1, delivered.getSuppressed().length);
      assertTrue(delivered.getSuppressed()[0] instanceof ScanCleanupFailure);
      assertSame(closeFailure, delivered.getSuppressed()[0].getCause());
      assertEquals(1, budget.snapshot().retiredScanners());
      assertEquals(1, budget.snapshot().scanners());

      var termination = assertThrows(CompletionException.class,
          () -> scheduler.termination().toCompletableFuture().join());
      assertSame(timeout, termination.getCause());
      assertEquals(1, termination.getSuppressed().length);
      assertTrue(termination.getSuppressed()[0] instanceof ScanCleanupFailure);
      assertSame(closeFailure, termination.getSuppressed()[0].getCause());
    }
  }

  @Test
  void closeFailureAfterSuccessfulReadIsReportedAndRetiresCapacity() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    var closeFailure = new IllegalStateException("synthetic close failure");
    physical.closeFailure = closeFailure;
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var operation = scheduler.bind(physical, account).startScan(request(0));
      physical.started.getFirst().result.complete(EMPTY);

      var delivered = assertThrows(CompletionException.class,
          () -> operation.completion().toCompletableFuture().join());
      assertTrue(delivered.getCause() instanceof ScanCleanupFailure);
      assertSame(closeFailure, delivered.getCause().getCause());
      assertEquals(1, budget.snapshot().retiredScanners());
      assertEquals(1, budget.snapshot().scanners());
      scheduler.close();
      var termination = assertThrows(CompletionException.class,
          () -> scheduler.termination().toCompletableFuture().join());
      assertSame(delivered.getCause(), termination.getCause());
    }
  }

  @Test
  void normalPhysicalCloseKeepsSuccessfulReadAndTermination() throws Exception {
    var budget = budget();
    var physical = new ControlledSource();
    try (var timer = new ManualTimer(); var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1), Runnable::run, timer);
        var scope = RequestScope.withoutDeadline(); var account = budget.admit("alpha", scope, MEMORY)) {
      var operation = scheduler.bind(physical, account).startScan(request(0));
      physical.started.getFirst().result.complete(EMPTY);

      assertSame(EMPTY, operation.completion().toCompletableFuture().join());
      assertEquals(0, budget.snapshot().retiredScanners());
      operation.close();
      scheduler.close();
      assertEquals(0, budget.snapshot().scanners());
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertNull(scheduler.termination().toCompletableFuture().join());
    }
  }

  private static BudgetManager budget() { return new BudgetManager(LIMITS, Map.of("alpha", LIMITS, "beta", LIMITS), LIMITS); }
  private static void assertFailureCategory(BackendFailure.Category expected, ScanOperation operation) {
    var failure = assertThrows(CompletionException.class, () -> operation.completion().toCompletableFuture().join());
    assertEquals(expected, ((BackendFailure) failure.getCause()).category());
  }
  private static ScanRequest request(int bucket) {
    return new ScanRequest(bucket, KeyRange.all(), Optional.empty(), Long.MAX_VALUE, ReadProjection.METADATA, 1, 512, 32);
  }

  private static final class ControlledSource implements ScanSource {
    final List<Operation> started = new ArrayList<>();
    RuntimeException closeFailure;
    CountDownLatch closeStarted;
    CountDownLatch releaseClose;
    @Override public ScanOperation startScan(ScanRequest request) {
      var operation = new Operation(request.bucket(), closeFailure, closeStarted, releaseClose);
      started.add(operation);
      return operation;
    }
  }

  private static final class Operation implements ScanOperation {
    final CompletableFuture<StoragePage> result = new CompletableFuture<>();
    final int bucket;
    final RuntimeException closeFailure;
    final CountDownLatch closeStarted;
    final CountDownLatch releaseClose;
    boolean closed;
    Operation(int bucket, RuntimeException closeFailure, CountDownLatch closeStarted, CountDownLatch releaseClose) {
      this.bucket = bucket;
      this.closeFailure = closeFailure;
      this.closeStarted = closeStarted;
      this.releaseClose = releaseClose;
    }
    @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }
    @Override public void close() {
      closed = true;
      if (closeStarted != null) closeStarted.countDown();
      if (releaseClose != null) {
        try {
          if (!releaseClose.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("physical close was not released");
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("physical close was interrupted", interrupted);
        }
      }
      if (closeFailure != null) throw closeFailure;
    }
  }

  private static final class ManualTimer extends ScheduledThreadPoolExecutor {
    private final ArrayDeque<ManualTask> pending = new ArrayDeque<>();
    ManualTimer() { super(1); setRemoveOnCancelPolicy(true); }
    @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      var task = new ManualTask(command); pending.add(task); return task;
    }
    void expireAll() { while (!pending.isEmpty()) pending.remove().run(); }
  }

  private static final class ManualTask extends FutureTask<Void> implements ScheduledFuture<Void> {
    ManualTask(Runnable task) { super(task, null); }
    @Override public long getDelay(TimeUnit unit) { return 0; }
    @Override public int compareTo(Delayed other) { return 0; }
  }
}
