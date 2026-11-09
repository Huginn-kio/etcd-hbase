package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TxnWorkingSetTest {
  @Test
  void orderedWritesShareOneRevisionAndOnlyTheWorkingViewSeesThem() throws Exception {
    var invocation = invocation();
    var working = new TxnWorkingSet(invocation, 64 * 1024);
    StoredRecord base = live("a", "old");
    assertEquals(base, working.current(key("a"), Optional.of(base)).orElseThrow());
    PutResult first = working.put(command("a", "new", true), Optional.of(base));
    working.put(command("b", "second", false), Optional.empty());
    assertArrayEquals(bytes("old"), first.previous().orElseThrow().value().orElseThrow());
    assertArrayEquals(bytes("new"), working.current(key("a"), Optional.of(base))
        .orElseThrow().value().orElseThrow());
    assertArrayEquals(bytes("old"), base.value().orElseThrow());
    assertEquals(List.of(6L, 6L), working.candidates(ignored -> 0).stream()
        .map(candidate -> candidate.record().metadata().modificationRevision()).toList());
    assertEquals(List.of(0L, 1L), working.events().stream().map(event -> event.ordinal()).toList());
    assertEquals(List.of(key("b"), key("a")), working.candidates(candidateKey ->
        candidateKey.equals(key("b")) ? 0 : 1).stream().map(candidate -> candidate.record().key()).toList());
    assertTrue(working.hasChanges());
  }

  @Test
  void duplicateWriteAndBudgetFailureLeaveEarlierOperationsUnchanged() throws Exception {
    var working = new TxnWorkingSet(invocation(), 64 * 1024);
    working.put(command("a", "one", false), Optional.empty());
    assertEquals(KvFailure.Reason.DUPLICATE_KEY, assertThrows(KvFailure.class,
        () -> working.put(command("a", "two", false), Optional.empty())).reason());
    assertEquals(1, working.events().size());
    assertArrayEquals(bytes("one"), working.current(key("a"), Optional.empty())
        .orElseThrow().value().orElseThrow());

    var bounded = new TxnWorkingSet(invocation(), 1024);
    BackendFailure exhausted = assertThrows(BackendFailure.class,
        () -> bounded.put(command("a", "one", false), Optional.empty()));
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, exhausted.category());
    assertTrue(bounded.candidates(ignored -> 0).isEmpty());
    assertTrue(bounded.events().isEmpty());
  }

  @Test
  void failedPutDoesNotConsumeWorkingSetCapacityOrCreateAnEvent() throws Exception {
    var working = new TxnWorkingSet(invocation(), 64 * 1024);
    var missingIgnore = new PutCommand(key("a"), new byte[0], 0, false, true, false);
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND, assertThrows(KvFailure.class,
        () -> working.put(missingIgnore, Optional.empty())).reason());
    assertEquals(0, working.retainedBytes());
    assertTrue(working.events().isEmpty());
  }

  private static InvocationBinding invocation() {
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    return InvocationBinding.begin(InvocationId.create(), digest, digest, TransactionId.create(), 1, 3, 5);
  }

  private static PutCommand command(String key, String value, boolean previous) {
    return new PutCommand(key(key), bytes(value), 0, previous, false, false);
  }

  private static StoredRecord live(String key, String value) {
    byte[] encoded = bytes(value);
    return StoredRecord.live(key(key),
        new StorageMetadata(1, 5, 2, 0, StorageMetadata.State.LIVE, encoded.length), encoded);
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
