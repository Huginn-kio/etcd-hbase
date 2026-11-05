package io.etcdhbase.hbase.storage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/** Test-only bounded-fair single-active queue used by the J29 mixed workload. */
final class M2CostFairQueue {
  private static final int MAX_BUSINESS_BURST = 3;
  private static final int MAX_RENEWAL_BURST = 1;

  record Work(String kind, int candidates, int valueBytes, boolean renewal) {}

  record Sample(String kind, boolean renewal, int candidates, long waitNanos,
      long classSwitchGapNanos, long serviceNanos) {}

  @FunctionalInterface
  interface CommitAction {
    long run(Work work) throws Exception;
  }

  private M2CostFairQueue() {}

  static void run(String layout, long seed, String workloadDigest, CommitAction action)
      throws Exception {
    List<Work> schedule = workload();
    ExecutorService singleActive = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "j29-public-single-active");
      thread.setDaemon(true);
      return thread;
    });
    AtomicReference<Work> previousWork = new AtomicReference<>();
    AtomicLong previousCompletion = new AtomicLong();
    List<CompletableFuture<Sample>> submitted = new ArrayList<>(schedule.size());
    try {
      for (Work work : schedule) {
        long enqueuedAt = System.nanoTime();
        submitted.add(CompletableFuture.supplyAsync(
            () -> execute(work, action, previousWork, previousCompletion, enqueuedAt), singleActive));
      }

      print(layout, seed, workloadDigest, submitted.stream().map(CompletableFuture::join).toList());
    } finally {
      singleActive.shutdownNow();
      if (!singleActive.awaitTermination(20, TimeUnit.SECONDS)) {
        throw new IllegalStateException("J29 single-active workload thread did not stop");
      }
    }
  }

  private static Sample execute(Work work, CommitAction action, AtomicReference<Work> previousWork,
      AtomicLong previousCompletion, long enqueuedAt) {
    long started = System.nanoTime();
    Work previous = previousWork.get();
    long priorCompletion = previousCompletion.get();
    long switchGap = previous != null && previous.renewal() != work.renewal()
        ? Math.max(0, started - priorCompletion) : 0;
    try {
      long serviceNanos = action.run(work);
      previousWork.set(work);
      previousCompletion.set(System.nanoTime());
      return new Sample(work.kind(), work.renewal(), work.candidates(),
          Math.max(0, started - enqueuedAt), switchGap, serviceNanos);
    } catch (Exception failure) {
      throw new java.util.concurrent.CompletionException(failure);
    }
  }

  private static List<Work> workload() {
    var business = new ArrayDeque<Work>();
    var renewals = new ArrayDeque<Work>();
    for (int index = 0; index < 3; index++) business.add(new Work("SHORT_KV", 1, 64, false));
    business.add(new Work("LARGE_KV", 8, 512, false));
    business.add(new Work("SHORT_KV", 1, 64, false));
    business.add(new Work("LARGE_KV", 8, 512, false));
    for (int index = 0; index < 2; index++) renewals.add(new Work("SIMULATED_RENEWAL", 0, 0, true));
    return schedule(business, renewals);
  }

  private static List<Work> schedule(ArrayDeque<Work> business, ArrayDeque<Work> renewals) {
    List<Work> result = new ArrayList<>(business.size() + renewals.size());
    int businessBurst = 0;
    while (!business.isEmpty() || !renewals.isEmpty()) {
      if (!renewals.isEmpty() && (business.isEmpty() || businessBurst >= MAX_BUSINESS_BURST)) {
        result.add(renewals.removeFirst());
        businessBurst = 0;
      } else {
        result.add(business.removeFirst());
        businessBurst++;
      }
    }
    return List.copyOf(result);
  }

  private static void print(String layout, long seed, String workloadDigest, List<Sample> samples) {
    List<Long> waits = samples.stream().map(Sample::waitNanos).toList();
    List<Long> switchGaps = samples.stream().map(Sample::classSwitchGapNanos)
        .filter(gap -> gap > 0).toList();
    long consecutiveBusiness = maximumRun(samples, sample -> !sample.renewal());
    long consecutiveRenewals = maximumRun(samples, Sample::renewal);
    if (samples.size() != 8 || consecutiveBusiness > MAX_BUSINESS_BURST
        || consecutiveRenewals > MAX_RENEWAL_BURST) {
      throw new IllegalStateException("J29 fair queue did not respect its bounded workload contract");
    }
    String sampleJson = samples.stream().map(sample -> String.format(Locale.ROOT,
        "{\"kind\":\"%s\",\"renewal\":%s,\"candidates\":%d,\"queueWaitMicros\":%d,"
            + "\"serviceMicros\":%d,\"classSwitchGapMicros\":%d}", sample.kind(),
        sample.renewal(), sample.candidates(), sample.waitNanos() / 1000,
        sample.serviceNanos() / 1000, sample.classSwitchGapNanos() / 1000))
        .collect(java.util.stream.Collectors.joining(",", "[", "]"));
    System.out.printf(Locale.ROOT,
        "j29-queue {\"layout\":\"%s\",\"seed\":%d,\"workloadSha256\":\"%s\","
            + "\"policy\":\"bounded-fair-simulation-3-business-1-renewal\",\"submitted\":%d,"
            + "\"p50WaitMicros\":%d,\"p95WaitMicros\":%d,\"p99WaitMicros\":%d,"
            + "\"classSwitches\":%d,\"classSwitchGapP50Micros\":%d,"
            + "\"classSwitchGapP95Micros\":%d,\"maxConsecutiveBusiness\":%d,"
            + "\"maxConsecutiveRenewals\":%d,\"samples\":%s}%n",
        layout, seed, workloadDigest, samples.size(), percentile(waits, 50) / 1000,
        percentile(waits, 95) / 1000, percentile(waits, 99) / 1000, switchGaps.size(),
        percentile(switchGaps, 50) / 1000, percentile(switchGaps, 95) / 1000,
        consecutiveBusiness, consecutiveRenewals, sampleJson);
  }

  private static long maximumRun(List<Sample> samples, Predicate<Sample> selected) {
    long run = 0;
    long maximum = 0;
    for (Sample sample : samples) {
      if (selected.test(sample)) {
        run++;
        maximum = Math.max(maximum, run);
      } else {
        run = 0;
      }
    }
    return maximum;
  }

  private static long percentile(List<Long> values, int percentile) {
    List<Long> sorted = values.stream().sorted().toList();
    if (sorted.isEmpty()) return 0;
    int index = Math.max(0, (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1);
    return sorted.get(index);
  }
}
