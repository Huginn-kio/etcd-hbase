package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ScanDirection;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Copy-on-write private index. Only a fully written operation checkpoint advances its root. */
public final class PagedWorksetIndex {
  private static final int PAGE_MAGIC = 0x57535031;
  private static final int CHECKPOINT_MAGIC = 0x57534331;
  private static final int START_MAGIC = 0x57535331;
  private static final int MAX_ENTRIES = 16;
  private static final int MAX_DEPTH = 16;
  private static final int MAX_PAGE_IO = 100_000;
  private static final int MAX_KEY_BYTES = 65535;
  private static final int PAGE_ID_BYTES = 16 + 1 + Long.BYTES + Integer.BYTES;
  private static final byte PAGE_KIND = 1;
  private static final byte CHECKPOINT_KIND = 2;
  private static final byte START_KIND = 3;
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();

  /** Refers to one immutable event; its digest is checked again before consuming its value. */
  public record EventRef(int bucket, ByteKey key, long ordinal, ByteKey digest) {
    public EventRef {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(digest, "digest");
      if (bucket < 0 || bucket >= 4096 || key.length() == 0 || key.length() > MAX_KEY_BYTES
          || ordinal < 0 || digest.length() != 32) {
        throw new IllegalArgumentException("invalid private workset event reference");
      }
    }
    private SortKey sortKey() { return new SortKey(bucket, key); }
  }

  private record SortKey(int bucket, ByteKey key) implements Comparable<SortKey> {
    private SortKey {
      Objects.requireNonNull(key, "key");
      if (bucket < 0 || bucket >= 4096 || key.length() == 0 || key.length() > MAX_KEY_BYTES) {
        throw new IllegalArgumentException("invalid workset index key");
      }
    }
    @Override public int compareTo(SortKey other) {
      int order = Integer.compare(bucket, other.bucket);
      return order == 0 ? key.compareTo(other.key) : order;
    }
  }

  private record PageRef(SortKey first, SortKey last, int level, ByteKey row, ByteKey digest) {
    private PageRef {
      Objects.requireNonNull(first, "first");
      Objects.requireNonNull(last, "last");
      Objects.requireNonNull(row, "row");
      Objects.requireNonNull(digest, "digest");
      if (first.compareTo(last) > 0 || level < 0 || level > MAX_DEPTH
          || row.length() != PAGE_ID_BYTES || digest.length() != 32) {
        throw new IllegalArgumentException("invalid workset page reference");
      }
    }
  }

  private record Page(int level, List<EventRef> entries, List<PageRef> children) {
    private Page {
      entries = List.copyOf(entries);
      children = List.copyOf(children);
      if (level < 0 || level > MAX_DEPTH || (level == 0 && (entries.isEmpty() || !children.isEmpty()))
          || (level > 0 && (!entries.isEmpty() || children.isEmpty()))
          || Math.max(entries.size(), children.size()) > MAX_ENTRIES) {
        throw new IllegalArgumentException("invalid workset page shape");
      }
      SortKey previous = null;
      if (level == 0) {
        for (EventRef entry : entries) {
          if (previous != null && previous.compareTo(entry.sortKey()) >= 0) {
            throw new IllegalArgumentException("unordered workset leaf");
          }
          previous = entry.sortKey();
        }
      } else {
        for (PageRef child : children) {
          if (child.level() != level - 1 || previous != null && previous.compareTo(child.first()) >= 0) {
            throw new IllegalArgumentException("overlapping workset child pages");
          }
          previous = child.last();
        }
      }
    }
    private SortKey first() { return level == 0 ? entries.getFirst().sortKey() : children.getFirst().first(); }
    private SortKey last() { return level == 0 ? entries.getLast().sortKey() : children.getLast().last(); }
  }

  private final ResultIdentity identity;
  private final ByteKey identityDigest;
  private final SystemRecordStore records;
  private final RequestScope scope;
  private final BudgetManager.Account account;
  private final int pageBytes;
  private PageRef root;
  private long operations;
  private long events;
  private long entries;
  private ByteKey eventChain = ByteKey.copyOf(new byte[32]);
  private boolean writing;
  private boolean starting;
  private boolean started;
  private boolean failed;
  private int pageIo;

  /** pageBytes limits each encoded index page; the caller reserves the corresponding request memory. */
  public PagedWorksetIndex(ResultIdentity identity, SystemRecordStore records,
      RequestScope scope, BudgetManager.Account account, int pageBytes) {
    this.identity = Objects.requireNonNull(identity, "identity");
    this.records = Objects.requireNonNull(records, "records");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.account = Objects.requireNonNull(account, "account");
    if (pageBytes < 128 || pageBytes > 1024 * 1024) {
      throw new IllegalArgumentException("invalid workset page budget");
    }
    if (!account.belongsTo(scope)) throw new IllegalArgumentException("workset budget and scope differ");
    this.pageBytes = pageBytes;
    this.identityDigest = ManifestDigest.sha256(identity::writeTo);
  }

  public synchronized long eventCount() { return events; }
  public synchronized long entryCount() { return entries; }
  public synchronized ByteKey eventChain() { return eventChain; }

  /** Claims this invocation once; an unknown or existing claim is never replayed blindly. */
  public CompletableFuture<Void> begin() {
    synchronized (this) {
      if (started || starting || failed || inactive()) return CompletableFuture.failedFuture(unavailable());
      starting = true;
    }
    CompletableFuture<Void> claim;
    try {
      chargePage(false);
      byte[] value = ManifestDigest.encode(output -> {
        output.writeInt(START_MAGIC);
        output.write(identityDigest.bytes());
      });
      ByteKey row = ByteKey.copyOf(ByteBuffer.allocate(17).put(identity.transactionId().bytes())
          .put(START_KIND).array());
      claim = records.createIfAbsent(SystemRecordType.WORKSET, row, ByteKey.copyOf(value), scope)
          .thenApply(result -> {
            if (result != SystemRecordStore.CreateResult.CREATED || inactive()) {
              throw new java.util.concurrent.CompletionException(unavailable());
            }
            return null;
          });
    } catch (BackendFailure | RuntimeException failure) {
      synchronized (this) { starting = false; failed = true; }
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      synchronized (this) { starting = false; failed = true; }
      throw fatal;
    }
    return claim.whenComplete((ignored, failure) -> {
      boolean cancelled;
      synchronized (PagedWorksetIndex.this) {
        starting = false;
        cancelled = inactive();
        if (failure == null && !cancelled) started = true;
        else failed = true;
      }
      if (failure == null && cancelled) throw new java.util.concurrent.CompletionException(unavailable());
    }).copy();
  }

  /** Chained in operation order so the final checkpoint binds every event, including overwritten keys. */
  public static ByteKey advanceEventChain(ByteKey previous, EventRef event) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(event, "event");
    if (previous.length() != 32) throw new IllegalArgumentException("invalid workset event chain");
    return ManifestDigest.sha256(output -> {
      output.write(previous.bytes());
      output.writeInt(event.bucket());
      ManifestDigest.writeBytes(output, event.key().bytes());
      output.writeLong(event.ordinal());
      output.write(event.digest().bytes());
    });
  }

  /**
   * Writes every entry of one operation, then atomically creates its immutable checkpoint.
   * Until that checkpoint succeeds, concurrent readers continue using the prior root.
   * Any failed or uncertain operation fences this attempt against further use.
   */
  public CompletableFuture<Void> commitOperation(AsyncCursor<EventRef> input) {
    Objects.requireNonNull(input, "input");
    Update update;
    synchronized (this) {
      if (!started || failed || writing || inactive()) return CompletableFuture.failedFuture(unavailable());
      writing = true;
      update = new Update(input, root, operations, events, entries, eventChain);
    }
    CompletableFuture<Void> operation;
    try {
      operation = update.start();
    } catch (RuntimeException | Error failure) {
      synchronized (this) { writing = false; failed = true; }
      throw failure;
    }
    return operation.whenComplete((ignored, failure) -> {
      synchronized (PagedWorksetIndex.this) {
        writing = false;
        if (failure != null || inactive()) failed = true;
      }
    }).copy();
  }

  /** Reads only an entry from the latest completed operation checkpoint. */
  public CompletableFuture<Optional<EventRef>> find(int bucket, ByteKey key) {
    SortKey target = new SortKey(bucket, key);
    PageRef snapshot;
    synchronized (this) {
      if (!started || failed || inactive()) return CompletableFuture.failedFuture(unavailable());
      snapshot = root;
    }
    try {
      return fenceOnFailure(snapshot == null ? CompletableFuture.completedFuture(Optional.empty())
          : find(snapshot, target)).copy();
    } catch (RuntimeException failure) {
      synchronized (this) { failed = true; }
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      synchronized (this) { failed = true; }
      throw fatal;
    }
  }

  /** Returns one ordered reference per call, with no materialized list of modified keys. */
  public AsyncCursor<EventRef> scan(int bucket, KeyRange range) {
    return scan(bucket, range, ScanDirection.ASCENDING);
  }

  public AsyncCursor<EventRef> scan(int bucket, KeyRange range, ScanDirection direction) {
    if (bucket < 0 || bucket >= 4096) throw new IllegalArgumentException("invalid workset bucket");
    return cursor(bucket, Objects.requireNonNull(range, "range"), Objects.requireNonNull(direction, "direction"));
  }

  /** Traverses all buckets in bucket/key order for final candidate preparation. */
  public AsyncCursor<EventRef> scanAll() { return cursor(-1, KeyRange.all(), ScanDirection.ASCENDING); }

  private AsyncCursor<EventRef> cursor(int bucket, KeyRange range, ScanDirection direction) {
    PageRef snapshot;
    synchronized (this) {
      if (!started || failed || inactive()) throw new IllegalStateException("workset is not active");
      snapshot = root;
    }
    return new IndexCursor(bucket, range, snapshot, direction);
  }

  private final class IndexCursor implements AsyncCursor<EventRef> {
    private final int bucket;
    private final KeyRange range;
    private final PageRef snapshot;
    private final ScanDirection direction;
    private final AtomicBoolean reading = new AtomicBoolean();
    private SortKey after;
    private boolean ended;

    private IndexCursor(int bucket, KeyRange range, PageRef snapshot, ScanDirection direction) {
      this.bucket = bucket;
      this.range = range;
      this.snapshot = snapshot;
      this.direction = direction;
    }

    @Override public CompletableFuture<Optional<EventRef>> next() {
      if (!reading.compareAndSet(false, true)) {
        return CompletableFuture.failedFuture(new IllegalStateException("concurrent workset cursor reads"));
      }
      try {
        CompletableFuture<Optional<EventRef>> next;
        synchronized (PagedWorksetIndex.this) {
          if (!started || failed || inactive()) {
            reading.set(false);
            return CompletableFuture.failedFuture(unavailable());
          }
          next = ended || snapshot == null || range.isEmpty()
              ? CompletableFuture.completedFuture(Optional.empty()) : seek(snapshot, after, bucket, range, direction);
        }
        return fenceOnFailure(next.thenApply(this::advance)).whenComplete((ignored, failure) ->
            reading.set(false)).copy();
      } catch (RuntimeException failure) {
        reading.set(false);
        synchronized (PagedWorksetIndex.this) { failed = true; }
        return CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        reading.set(false);
        synchronized (PagedWorksetIndex.this) { failed = true; }
        throw fatal;
      }
    }

    private Optional<EventRef> advance(Optional<EventRef> value) {
      if (value.isPresent()) after = value.orElseThrow().sortKey();
      else ended = true;
      return value;
    }
  }

  private CompletableFuture<Optional<EventRef>> find(PageRef reference, SortKey target) {
    if (target.compareTo(reference.first()) < 0 || target.compareTo(reference.last()) > 0) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    return readPage(reference).thenCompose(page -> {
      if (page.level() == 0) {
        return CompletableFuture.completedFuture(page.entries().stream()
            .filter(entry -> entry.sortKey().equals(target)).findFirst());
      }
      for (PageRef child : page.children()) {
        if (target.compareTo(child.first()) >= 0 && target.compareTo(child.last()) <= 0) {
          return find(child, target);
        }
      }
      return CompletableFuture.completedFuture(Optional.empty());
    });
  }

  private CompletableFuture<Optional<EventRef>> seek(PageRef reference, SortKey after,
      int bucket, KeyRange range, ScanDirection direction) {
    if (pageBeforeResume(reference, after, direction) || !intersects(reference, bucket, range)) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    return readPage(reference).thenCompose(page -> {
      if (page.level() == 0) {
        return CompletableFuture.completedFuture(selectLeaf(page.entries(), after, bucket, range, direction));
      }
      int first = direction == ScanDirection.ASCENDING ? 0 : page.children().size() - 1;
      return seekChildren(page.children(), first, after, bucket, range, direction);
    });
  }

  private static boolean pageBeforeResume(PageRef reference, SortKey after, ScanDirection direction) {
    if (after == null) return false;
    return direction == ScanDirection.ASCENDING
        ? reference.last().compareTo(after) <= 0 : reference.first().compareTo(after) >= 0;
  }

  private static Optional<EventRef> selectLeaf(List<EventRef> entries, SortKey after, int bucket,
      KeyRange range, ScanDirection direction) {
    int step = direction == ScanDirection.ASCENDING ? 1 : -1;
    for (int index = step > 0 ? 0 : entries.size() - 1; index >= 0 && index < entries.size(); index += step) {
      EventRef entry = entries.get(index);
      if (entryAfterResume(entry, after, direction) && entryInRange(entry, bucket, range)) return Optional.of(entry);
    }
    return Optional.empty();
  }

  private static boolean entryAfterResume(EventRef entry, SortKey after, ScanDirection direction) {
    if (after == null) return true;
    return direction == ScanDirection.ASCENDING
        ? entry.sortKey().compareTo(after) > 0 : after.compareTo(entry.sortKey()) > 0;
  }

  private static boolean entryInRange(EventRef entry, int bucket, KeyRange range) {
    return bucket < 0 || entry.bucket() == bucket && range.contains(entry.key());
  }

  private static boolean intersects(PageRef reference, int bucket, KeyRange range) {
    if (bucket < 0) return true;
    if (reference.last().bucket() < bucket || reference.first().bucket() > bucket) return false;
    if (range.lowerInclusive().isPresent() && reference.last().bucket() == bucket
        && reference.last().key().compareTo(range.lowerInclusive().orElseThrow()) < 0) return false;
    if (reference.first().bucket() == bucket) {
      if (range.kind() == KeyRange.Kind.POINT
          && reference.first().key().compareTo(range.lowerInclusive().orElseThrow()) > 0) return false;
      if (range.upperExclusive().isPresent()
          && reference.first().key().compareTo(range.upperExclusive().orElseThrow()) >= 0) return false;
    }
    return true;
  }

  private CompletableFuture<Optional<EventRef>> seekChildren(List<PageRef> children, int index,
      SortKey after, int bucket, KeyRange range, ScanDirection direction) {
    if (index < 0 || index >= children.size()) return CompletableFuture.completedFuture(Optional.empty());
    int next = index + (direction == ScanDirection.ASCENDING ? 1 : -1);
    return seek(children.get(index), after, bucket, range, direction).thenComposeAsync(found -> found.isPresent()
        ? CompletableFuture.completedFuture(found) : seekChildren(children, next, after, bucket, range, direction),
        CONTINUE);
  }

  private final class Update {
    private final AsyncCursor<EventRef> input;
    private final long ordinal;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private PageRef nextRoot;
    private long nextEvent;
    private long nextEntries;
    private ByteKey nextChain;
    private int pageNumber;
    private boolean insertedKey;

    private Update(AsyncCursor<EventRef> input, PageRef root, long ordinal, long events, long entries,
        ByteKey eventChain) {
      this.input = input;
      this.nextRoot = root;
      this.ordinal = ordinal;
      this.nextEvent = events;
      this.nextEntries = entries;
      this.nextChain = eventChain;
    }

    private CompletableFuture<Void> start() {
      next();
      return completion;
    }

    private void next() {
      try {
        if (inactive()) {
          completion.completeExceptionally(unavailable());
          return;
        }
        Objects.requireNonNull(input.next(), "workset source future")
            .whenCompleteAsync(this::acceptInput, CONTINUE);
      } catch (RuntimeException failure) {
        completion.completeExceptionally(failure);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void acceptInput(Optional<EventRef> value, Throwable failure) {
      if (failure != null) {
        completion.completeExceptionally(failure);
        return;
      }
      try {
        acceptValue(value);
      } catch (RuntimeException problem) {
        completion.completeExceptionally(problem);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void acceptValue(Optional<EventRef> entry) {
      if (inactive()) {
        completion.completeExceptionally(unavailable());
        return;
      }
      Objects.requireNonNull(entry, "workset source value");
      if (entry.isEmpty()) {
        checkpoint().whenComplete(this::finishCheckpoint);
        return;
      }
      EventRef event = entry.orElseThrow();
      if (event.ordinal() != nextEvent || nextEvent == Long.MAX_VALUE) {
        throw new IllegalArgumentException("workset events must be contiguous");
      }
      insertedKey = nextRoot == null;
      insert(nextRoot, event).thenComposeAsync(this::growRoot, CONTINUE)
          .whenCompleteAsync((updated, problem) -> advance(event, updated, problem), CONTINUE);
    }

    private void finishCheckpoint(Void ignored, Throwable problem) {
      if (problem == null) completion.complete(null);
      else completion.completeExceptionally(problem);
    }

    private void advance(EventRef event, PageRef updated, Throwable problem) {
      if (problem != null) {
        completion.completeExceptionally(problem);
        return;
      }
      try {
        nextRoot = updated;
        nextChain = advanceEventChain(nextChain, event);
        if (insertedKey) nextEntries = Math.addExact(nextEntries, 1);
        nextEvent++;
        next();
      } catch (RuntimeException failureDuringAdvance) {
        completion.completeExceptionally(failureDuringAdvance);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private CompletableFuture<List<PageRef>> insert(PageRef previous, EventRef event) {
      if (previous == null) return writeLeaves(List.of(event));
      return readPage(previous).thenComposeAsync(page -> {
        if (page.level() == 0) {
          var values = new ArrayList<>(page.entries());
          int index = 0;
          while (index < values.size() && values.get(index).sortKey().compareTo(event.sortKey()) < 0) index++;
          if (index < values.size() && values.get(index).sortKey().equals(event.sortKey())) values.set(index, event);
          else {
            values.add(index, event);
            insertedKey = true;
          }
          return writeLeaves(values);
        }
        int selected = 0;
        while (selected + 1 < page.children().size()
            && page.children().get(selected + 1).first().compareTo(event.sortKey()) <= 0) selected++;
        int child = selected;
        return insert(page.children().get(child), event).thenComposeAsync(replacement -> {
          var children = new ArrayList<>(page.children());
          children.remove(child);
          children.addAll(child, replacement);
          return writeBranches(page.level(), children);
        }, CONTINUE);
      }, CONTINUE);
    }

    private CompletableFuture<PageRef> growRoot(List<PageRef> roots) {
      if (roots.size() == 1) return CompletableFuture.completedFuture(roots.getFirst());
      int level = roots.getFirst().level() + 1;
      if (level > MAX_DEPTH || partitions(level, List.of(), roots).size() >= roots.size()) {
        return CompletableFuture.failedFuture(resourceLimit());
      }
      return writeBranches(level, roots).thenComposeAsync(this::growRoot, CONTINUE);
    }

    private CompletableFuture<List<PageRef>> writeLeaves(List<EventRef> entries) {
      return writePages(partitions(0, entries, List.of()), 0, new ArrayList<>());
    }

    private CompletableFuture<List<PageRef>> writeBranches(int level, List<PageRef> children) {
      return writePages(partitions(level, List.of(), children), 0, new ArrayList<>());
    }

    private CompletableFuture<List<PageRef>> writePages(List<Page> pages, int index, List<PageRef> written) {
      if (index == pages.size()) return CompletableFuture.completedFuture(List.copyOf(written));
      if (pageNumber == Integer.MAX_VALUE || inactive()) return CompletableFuture.failedFuture(unavailable());
      Page page = pages.get(index);
      byte[] bytes = encode(page);
      ByteKey row = pageId(ordinal, pageNumber++);
      PageRef reference = new PageRef(page.first(), page.last(), page.level(), row, ManifestDigest.sha256(bytes));
      try { chargePage(false); }
      catch (BackendFailure failure) { return CompletableFuture.failedFuture(failure); }
      return records.createIfAbsent(SystemRecordType.WORKSET, row, ByteKey.copyOf(bytes), scope)
          .thenComposeAsync(result -> {
            requireCreated(result);
            if (inactive()) return CompletableFuture.failedFuture(unavailable());
            written.add(reference);
            return writePages(pages, index + 1, written);
          }, CONTINUE);
    }

    private CompletableFuture<Void> checkpoint() {
      if (ordinal == Long.MAX_VALUE || inactive()) return CompletableFuture.failedFuture(unavailable());
      byte[] encoded = ManifestDigest.encode(output -> {
        output.writeInt(CHECKPOINT_MAGIC);
        output.write(identityDigest.bytes());
        output.writeLong(ordinal);
        output.writeLong(nextEvent);
        output.writeLong(nextEntries);
        output.write(nextChain.bytes());
        output.writeBoolean(nextRoot != null);
        if (nextRoot != null) writeReference(output, nextRoot);
      });
      if (encoded.length > pageBytes) return CompletableFuture.failedFuture(resourceLimit());
      try { chargePage(false); }
      catch (BackendFailure failure) { return CompletableFuture.failedFuture(failure); }
      return records.createIfAbsent(SystemRecordType.WORKSET, checkpointId(ordinal), ByteKey.copyOf(encoded), scope)
          .thenApply(result -> {
            requireCreated(result);
            synchronized (PagedWorksetIndex.this) {
              if (inactive() || failed) throw new java.util.concurrent.CompletionException(unavailable());
              root = nextRoot;
              events = nextEvent;
              entries = nextEntries;
              eventChain = nextChain;
              operations = ordinal + 1;
            }
            return null;
          });
    }
  }

  private List<Page> partitions(int level, List<EventRef> entries, List<PageRef> children) {
    int size = level == 0 ? entries.size() : children.size();
    var pages = new ArrayList<Page>();
    int start = 0;
    while (start < size) {
      int end = start;
      int bytes = 4 + 32 + 4 + 4;
      while (end < size && end - start < MAX_ENTRIES) {
        int item = level == 0 ? entryBytes(entries.get(end)) : referenceBytes(children.get(end));
        if (item > pageBytes - bytes) break;
        bytes += item;
        end++;
      }
      if (end == start) throw new java.util.concurrent.CompletionException(resourceLimit());
      pages.add(level == 0 ? new Page(0, entries.subList(start, end), List.of())
          : new Page(level, List.of(), children.subList(start, end)));
      start = end;
    }
    return List.copyOf(pages);
  }

  private CompletableFuture<Page> readPage(PageRef reference) {
    if (inactive()) return CompletableFuture.failedFuture(unavailable());
    requirePageIdentity(reference.row());
    try { chargePage(true); }
    catch (BackendFailure failure) { return CompletableFuture.failedFuture(failure); }
    return records.read(SystemRecordType.WORKSET, reference.row(), scope).thenApply(value -> {
      if (inactive()) throw new java.util.concurrent.CompletionException(unavailable());
      byte[] encoded = value.orElseThrow(() -> new IllegalStateException("workset page is missing")).bytes();
      if (encoded.length > pageBytes || !ManifestDigest.sha256(encoded).equals(reference.digest())) {
        throw new IllegalStateException("workset page digest or size differs");
      }
      Page page = decode(encoded);
      if (page.level() != reference.level() || !page.first().equals(reference.first())
          || !page.last().equals(reference.last())) {
        throw new IllegalStateException("workset page bounds differ from its reference");
      }
      return page;
    });
  }

  private byte[] encode(Page page) {
    byte[] encoded = ManifestDigest.encode(output -> {
      output.writeInt(PAGE_MAGIC);
      output.write(identityDigest.bytes());
      output.writeInt(page.level());
      output.writeInt(page.level() == 0 ? page.entries().size() : page.children().size());
      if (page.level() == 0) {
        for (EventRef entry : page.entries()) {
          writeKey(output, entry.sortKey());
          output.writeLong(entry.ordinal());
          output.write(entry.digest().bytes());
        }
      } else {
        for (PageRef child : page.children()) writeReference(output, child);
      }
    });
    if (encoded.length > pageBytes) throw new java.util.concurrent.CompletionException(resourceLimit());
    return encoded;
  }

  private Page decode(byte[] encoded) {
    try (var input = new DataInputStream(new ByteArrayInputStream(encoded))) {
      if (input.readInt() != PAGE_MAGIC || !Arrays.equals(readFixed(input, 32), identityDigest.bytes())) {
        throw new IllegalArgumentException("workset page identity or version differs");
      }
      int level = input.readInt();
      int count = input.readInt();
      if (level < 0 || level > MAX_DEPTH || count < 1 || count > MAX_ENTRIES) {
        throw new IllegalArgumentException("invalid workset page header");
      }
      var entries = new ArrayList<EventRef>();
      var children = new ArrayList<PageRef>();
      for (int index = 0; index < count; index++) {
        if (level == 0) {
          SortKey key = readKey(input);
          entries.add(new EventRef(key.bucket(), key.key(), input.readLong(), ByteKey.copyOf(readFixed(input, 32))));
        } else {
          PageRef child = readReference(input);
          requirePageIdentity(child.row());
          children.add(child);
        }
      }
      if (input.available() != 0) throw new IllegalArgumentException("trailing workset page bytes");
      return new Page(level, entries, children);
    } catch (IOException failure) {
      throw new IllegalArgumentException("truncated workset page", failure);
    }
  }

  private static void writeKey(DataOutputStream output, SortKey key) throws IOException {
    output.writeInt(key.bucket());
    ManifestDigest.writeBytes(output, key.key().bytes());
  }

  private static SortKey readKey(DataInputStream input) throws IOException {
    int bucket = input.readInt();
    int length = input.readInt();
    if (length <= 0 || length > MAX_KEY_BYTES || length > input.available()) {
      throw new IllegalArgumentException("invalid workset key length");
    }
    return new SortKey(bucket, ByteKey.copyOf(readFixed(input, length)));
  }

  private static void writeReference(DataOutputStream output, PageRef ref) throws IOException {
    writeKey(output, ref.first());
    writeKey(output, ref.last());
    output.writeInt(ref.level());
    output.write(ref.row().bytes());
    output.write(ref.digest().bytes());
  }

  private static PageRef readReference(DataInputStream input) throws IOException {
    SortKey first = readKey(input);
    SortKey last = readKey(input);
    return new PageRef(first, last, input.readInt(), ByteKey.copyOf(readFixed(input, PAGE_ID_BYTES)),
        ByteKey.copyOf(readFixed(input, 32)));
  }

  private static byte[] readFixed(DataInputStream input, int length) throws IOException {
    byte[] bytes = input.readNBytes(length);
    if (bytes.length != length) throw new IllegalArgumentException("truncated workset record");
    return bytes;
  }

  private static int entryBytes(EventRef entry) { return 4 + 4 + entry.key().length() + 8 + 32; }
  private static int referenceBytes(PageRef ref) {
    return 4 + 4 + ref.first().key().length() + 4 + 4 + ref.last().key().length()
        + 4 + PAGE_ID_BYTES + 32;
  }

  private ByteKey pageId(long operation, int page) {
    return ByteKey.copyOf(ByteBuffer.allocate(PAGE_ID_BYTES).put(identity.transactionId().bytes())
        .put(PAGE_KIND).putLong(operation).putInt(page).array());
  }

  private ByteKey checkpointId(long operation) {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Long.BYTES).put(identity.transactionId().bytes())
        .put(CHECKPOINT_KIND).putLong(operation).array());
  }

  private void requirePageIdentity(ByteKey row) {
    byte[] bytes = row.bytes();
    if (bytes.length != PAGE_ID_BYTES || bytes[16] != PAGE_KIND
        || !Arrays.equals(Arrays.copyOf(bytes, 16), identity.transactionId().bytes())
        || ByteBuffer.wrap(bytes, 17, Long.BYTES).getLong() < 0
        || ByteBuffer.wrap(bytes, 25, Integer.BYTES).getInt() < 0) {
      throw new IllegalArgumentException("workset page belongs to another transaction");
    }
  }

  private <T> CompletableFuture<T> fenceOnFailure(CompletableFuture<T> future) {
    return future.whenComplete((ignored, failure) -> {
      if (failure != null) synchronized (PagedWorksetIndex.this) { failed = true; }
    });
  }

  private void chargePage(boolean read) throws BackendFailure {
    synchronized (this) {
      if (inactive()) throw unavailable();
      if (pageIo >= MAX_PAGE_IO || !account.isActive()) throw resourceLimit();
      pageIo++;
    }
    // The persisted page is checked against this bound after reading; charge the attempt first.
    if (read) account.chargeInternalPointRead(pageBytes);
  }

  private boolean inactive() { return scope.isClosed() || scope.isDeadlineExceeded(); }
  private BackendFailure unavailable() {
    return new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT
        : scope.isClosed() ? BackendFailure.Category.CANCELLED : BackendFailure.Category.UNKNOWN);
  }
  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
  private static void requireCreated(SystemRecordStore.CreateResult result) {
    if (result != SystemRecordStore.CreateResult.CREATED && result != SystemRecordStore.CreateResult.ALREADY_PRESENT) {
      throw new IllegalStateException("immutable workset record conflicts with this attempt");
    }
  }
}
