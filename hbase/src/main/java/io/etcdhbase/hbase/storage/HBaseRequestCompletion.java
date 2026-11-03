package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.error.HBaseFailures;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Separates a request's cancellable result from the HBase producer's actual completion. */
final class HBaseRequestCompletion {
  private HBaseRequestCompletion() {}

  static <T> CompletableFuture<T> startRequest(
      RequestScope scope, Supplier<CompletableFuture<T>> startProducer) {
    try {
      if (scope.isDeadlineExceeded()) throw new TimeoutException();
      if (scope.isClosed()) throw new CancellationException();
      return bindRequestToProducer(scope, startProducer);
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
  }

  static <T> CompletableFuture<T> bindRequestToProducer(
      RequestScope scope, Supplier<CompletableFuture<T>> startProducer) {
    var request = new CompletableFuture<T>();
    var actualCompletion = new CompletableFuture<Void>();
    var registration = scope.registerScopedCleanup(
        () -> request.completeExceptionally(new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED)),
        actualCompletion);
    AtomicReference<CompletableFuture<T>> producerReference = new AtomicReference<>();
    boolean started;
    try {
      started = scope.startIfOpen(() -> producerReference.set(
          Objects.requireNonNull(startProducer.get(), "producer")));
    } catch (Exception failure) {
      request.completeExceptionally(HBaseFailures.classify(failure));
      actualCompletion.complete(null);
      closeRegistration(registration, request);
      return request;
    }
    if (!started) {
      request.completeExceptionally(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      actualCompletion.complete(null);
      closeRegistration(registration, request);
      return request;
    }
    CompletableFuture<T> producer = producerReference.get();
    producer.whenComplete((value, failure) -> {
      if (failure != null) {
        request.completeExceptionally(HBaseFailures.classify(failure));
      } else if (scope.isDeadlineExceeded()) {
        request.completeExceptionally(new BackendFailure(BackendFailure.Category.TIMEOUT));
      } else if (scope.isClosed()) {
        request.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
      } else {
        request.complete(value);
      }
      actualCompletion.complete(null);
      closeRegistration(registration, request);
    });
    return request;
  }

  private static void closeRegistration(RequestScope.ScopedCleanup registration,
      CompletableFuture<?> request) {
    try {
      registration.close();
    } catch (Exception failureToClose) {
      request.completeExceptionally(HBaseFailures.classify(failureToClose));
    }
  }
}
