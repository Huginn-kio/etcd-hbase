package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Persists and verifies every bounded result page before returning a sealable reference. */
public final class ResultPreparation {
  private final ResultRepository repository;
  private final ResultCatalog catalog;

  public ResultPreparation(ResultRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.catalog = new ResultCatalog(repository);
  }

  /** Splits one length-framed logical result; the caller accounts for its input and page copies. */
  public static List<ResultPage> split(ResultIdentity identity, ByteBuffer encoded) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(encoded, "encoded");
    int length = encoded.remaining();
    if (length == 0 || length > ResultDirectory.MAX_PAGES * ResultPage.MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("encoded result exceeds its page bounds");
    }
    ByteBuffer source = encoded.asReadOnlyBuffer();
    List<ResultPage> pages = new ArrayList<>((length + ResultPage.MAX_PAYLOAD_BYTES - 1)
        / ResultPage.MAX_PAYLOAD_BYTES);
    while (source.hasRemaining()) {
      byte[] chunk = new byte[Math.min(source.remaining(), ResultPage.MAX_PAYLOAD_BYTES)];
      source.get(chunk);
      pages.add(new ResultPage(identity, pages.size(), chunk));
    }
    return List.copyOf(pages);
  }

  /**
   * The caller owns the page memory and its output budget. On cancellation it must retain that
   * budget until the scope's physical producers drain, even if this future has already failed.
   */
  public CompletableFuture<ResultReference> prepare(ResultIdentity identity, List<ResultPage> pages,
      RequestScope scope) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(pages, "pages");
    Objects.requireNonNull(scope, "scope");
    if (pages.isEmpty() || pages.size() > ResultDirectory.MAX_PAGES) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid result page count"));
    }
    List<ResultPage> snapshot = List.copyOf(pages);
    ResultDirectory directory = ResultDirectory.create(identity,
        snapshot.stream().map(ResultPageReference::from).toList());
    CompletableFuture<Void> writes = CompletableFuture.completedFuture(null);
    for (ResultPage page : snapshot) {
      writes = writes.thenCompose(ignored -> persistPage(page, scope));
    }
    return writes.thenCompose(ignored -> persistDirectory(directory, scope))
        .thenCompose(ignored -> catalog.verify(directory.reference(), scope))
        .thenApply(ignored -> directory.reference());
  }

  private CompletableFuture<Void> persistPage(ResultPage page, RequestScope scope) {
    return repository.createPage(page, scope).handle(WriteOutcome::new).thenCompose(outcome -> {
      rejectConflict(outcome);
      return repository.readPage(page.identity(), page.index(), scope).thenAccept(found -> {
        if (found.isEmpty()) throw missing(outcome, "result page");
        if (!ResultPageReference.from(page).matches(found.orElseThrow())) {
          throw new IllegalStateException("durable result page differs from its preparation");
        }
      });
    });
  }

  private CompletableFuture<Void> persistDirectory(ResultDirectory directory, RequestScope scope) {
    return repository.createDirectory(directory, scope).handle(WriteOutcome::new).thenCompose(outcome -> {
      rejectConflict(outcome);
      return repository.readDirectory(directory.identity(), scope).thenAccept(found -> {
        if (found.isEmpty()) throw missing(outcome, "result directory");
        if (!directory.reference().matches(found.orElseThrow())) {
          throw new IllegalStateException("durable result directory differs from its preparation");
        }
      });
    });
  }

  private static void rejectConflict(WriteOutcome outcome) {
    if (outcome.failure() != null && !isUnknown(outcome.failure())) {
      throw new CompletionException(CompletionFailures.unwrapCompletion(outcome.failure()));
    }
    if (outcome.result() == SystemRecordStore.CreateResult.CONFLICT) {
      throw new IllegalStateException("conflicting durable result record");
    }
    if (outcome.result() == null && outcome.failure() == null) {
      throw new IllegalStateException("result repository returned no write outcome");
    }
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    return cause instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private static RuntimeException missing(WriteOutcome outcome, String record) {
    if (outcome.failure() != null) return new CompletionException(outcome.failure());
    return new IllegalStateException("durable " + record + " is missing after write");
  }

  private record WriteOutcome(SystemRecordStore.CreateResult result, Throwable failure) {}
}
