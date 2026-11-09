package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ResultPreparationTest {
  @Test
  void lostAcknowledgementIsResolvedByReadingTheSameDurablePage() {
    MemoryRepository repository = new MemoryRepository();
    repository.loseFirstPageAcknowledgement = true;
    repository.loseDirectoryAcknowledgement = true;
    ResultIdentity identity = identity();
    List<ResultPage> pages = pages(identity);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      ResultReference reference = new ResultPreparation(repository).prepare(identity, pages, scope).join();
      assertEquals(2, reference.pageCount());
      assertEquals(reference, new ResultCatalog(repository).verify(reference, scope).join().reference());
      assertEquals(reference, new ResultPreparation(repository).prepare(identity, pages, scope).join());
    }
  }

  @Test
  void doesNotPublishAReferenceWhenAWriteIsUnknownOrConflicts() {
    MemoryRepository repository = new MemoryRepository();
    ResultIdentity identity = identity();
    repository.failBeforeFirstPageWrite = true;
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertThrows(CompletionException.class,
          () -> new ResultPreparation(repository).prepare(identity, pages(identity), scope).join());
      assertTrue(repository.directories.isEmpty());
      repository.failBeforeFirstPageWrite = false;
      repository.pages.put(0, new ResultPage(identity, 0, new byte[] {9}));
      assertThrows(CompletionException.class,
          () -> new ResultPreparation(repository).prepare(identity, pages(identity), scope).join());
      assertTrue(repository.directories.isEmpty());
    }
  }

  @Test
  void detectsMissingOrMismatchedPageAfterDirectoryWasStored() {
    MemoryRepository repository = new MemoryRepository();
    ResultIdentity identity = identity();
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      ResultReference reference = new ResultPreparation(repository).prepare(identity, pages(identity), scope).join();
      repository.pages.remove(1);
      assertThrows(CompletionException.class, () -> new ResultCatalog(repository).verify(reference, scope).join());
      repository.pages.put(1, new ResultPage(identity, 1, new byte[] {9}));
      assertThrows(CompletionException.class, () -> new ResultCatalog(repository).verify(reference, scope).join());
    }
  }

  @Test
  void doesNotHideDefinitePermissionFailureEvenIfThePageExists() {
    MemoryRepository repository = new MemoryRepository();
    ResultIdentity identity = identity();
    repository.pages.put(0, pages(identity).getFirst());
    repository.failPermission = true;
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      CompletionException failure = assertThrows(CompletionException.class,
          () -> new ResultPreparation(repository).prepare(identity, pages(identity), scope).join());
      assertTrue(failure.getCause() instanceof BackendFailure);
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertTrue(repository.directories.isEmpty());
    }
  }

  @Test
  void splitRetainsExactBytesWithoutAdvancingOrBorrowingCallerBuffer() {
    ResultIdentity identity = identity();
    byte[] source = new byte[ResultPage.MAX_PAYLOAD_BYTES + 1];
    source[0] = 1;
    source[source.length - 1] = 2;
    var buffer = java.nio.ByteBuffer.wrap(source);
    List<ResultPage> pages = ResultPreparation.split(identity, buffer);
    source[0] = 9;
    assertEquals(0, buffer.position());
    assertEquals(2, pages.size());
    assertEquals(1, pages.getFirst().itemCount());
    assertEquals(0, pages.getLast().itemCount());
    assertEquals(1, pages.getFirst().payload()[0]);
    assertEquals(2, pages.getLast().payload()[0]);
    assertThrows(IllegalArgumentException.class,
        () -> ResultPreparation.split(identity, java.nio.ByteBuffer.allocate(0)));
  }

  private static List<ResultPage> pages(ResultIdentity identity) {
    return List.of(new ResultPage(identity, 0, new byte[] {1}),
        new ResultPage(identity, 1, new byte[] {2}));
  }

  private static ResultIdentity identity() {
    return ResultIdentity.from(InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1));
  }

  private static final class MemoryRepository implements ResultRepository {
    private final Map<Integer, ResultPage> pages = new HashMap<>();
    private final Map<ResultIdentity, ResultDirectory> directories = new HashMap<>();
    private boolean loseFirstPageAcknowledgement;
    private boolean loseDirectoryAcknowledgement;
    private boolean failBeforeFirstPageWrite;
    private boolean failPermission;

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage page, RequestScope scope) {
      if (page.index() == 0 && failBeforeFirstPageWrite) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      if (failPermission) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      ResultPage old = pages.putIfAbsent(page.index(), page);
      if (page.index() == 0 && loseFirstPageAcknowledgement) {
        loseFirstPageAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(old == null ? SystemRecordStore.CreateResult.CREATED
          : ResultPageReference.from(page).matches(old) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
          : SystemRecordStore.CreateResult.CONFLICT);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory directory, RequestScope scope) {
      ResultDirectory old = directories.putIfAbsent(directory.identity(), directory);
      if (loseDirectoryAcknowledgement) {
        loseDirectoryAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(old == null ? SystemRecordStore.CreateResult.CREATED
          : directory.reference().matches(old) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
          : SystemRecordStore.CreateResult.CONFLICT);
    }

    @Override
    public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(pages.get(index)));
    }

    @Override
    public CompletableFuture<Optional<ResultDirectory>> readDirectory(
        ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(directories.get(identity)));
    }
  }
}
