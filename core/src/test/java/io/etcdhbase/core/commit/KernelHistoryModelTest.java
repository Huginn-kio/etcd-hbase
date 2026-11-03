package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Bounded history checks for the M2 control transition and minimal KV operation contract. */
class KernelHistoryModelTest {
  @Test
  void selectedUnknownTransactionIsVisibleAcrossCancellationAndRecovery() {
    KernelDriver driver = new KernelDriver(Map.of("alpha", "old-alpha", "beta", "old-beta"));
    Map<String, Mutation> changes = new LinkedHashMap<>();
    changes.put("alpha", Mutation.put("new-alpha"));
    changes.put("beta", Mutation.put("new-beta"));
    PendingTransaction transaction = driver.beginCompareAndWrite(
        0, "alpha", Optional.of("old-alpha"), changes);
    transaction.materializePrefix(1);
    assertEquals(1, transaction.materializedCount);
    assertEquals("new-alpha", driver.physical.get("alpha"));
    assertEquals("old-beta", driver.physical.get("beta"));

    HistoryOperation alphaRead = driver.read(1, 2, "alpha");
    assertEquals(Optional.of("new-alpha"), alphaRead.observedValue());
    RequestScope callerScope = RequestScope.withoutDeadline();
    HistoryOperation cancelled = transaction.cancelCallerAfterSelection(callerScope, 2);
    assertTrue(callerScope.isCancelled());
    HistoryOperation betaRead = driver.read(3, 4, "beta");
    assertEquals(Optional.of("new-beta"), betaRead.observedValue());

    driver.recoverSelectedPublication();
    driver.recoverSelectedPublication();
    HistoryOperation staleCompare = driver.compareAndWrite(5, 6, "alpha", Optional.of("old-alpha"),
        Map.of("alpha", Mutation.put("stale")));
    HistoryOperation put = driver.put(7, 8, "gamma", "created");
    HistoryOperation putRead = driver.read(9, 10, "gamma");
    HistoryOperation delete = driver.delete(11, 12, "beta");
    HistoryOperation deletedRead = driver.read(13, 14, "beta");

    List<HistoryOperation> history = List.of(
        cancelled, alphaRead, betaRead, staleCompare, put, putRead, delete, deletedRead);
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of(
        "alpha", "old-alpha", "beta", "old-beta"));

    assertTrue(checker.hasLinearization(history));
    assertFalse(checker.hasLinearization(withObservedValue(history, alphaRead.id(), Optional.of("old-alpha"))));
    assertTrue(driver.control.publication().isEmpty());
    assertEquals(driver.control.finalizedSequence(), driver.control.publishedSequence());
    assertEquals(3, driver.control.publishedSequence(),
        "the selected unknown transaction, put, and delete each advance the commit sequence once");
    assertEquals(4, driver.control.publishedRevision(),
        "replaying recovery must not allocate a second revision");
  }

  @Test
  void unresolvedUnknownMayHaveTakenEffectOrMayNotHaveTakenEffect() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(1, 4, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.UNKNOWN);
    HistoryOperation oldRead = HistoryOperation.read(5, 6, "alpha", Optional.of("old"));
    HistoryOperation newRead = HistoryOperation.read(5, 6, "alpha", Optional.of("new"));

    assertTrue(new SerialReferenceChecker(Map.of("alpha", "old"))
        .hasLinearization(List.of(unknown, oldRead)));
    assertTrue(new SerialReferenceChecker(Map.of("alpha", "old"))
        .hasLinearization(List.of(unknown, newRead)));
  }

  private static List<HistoryOperation> withObservedValue(
      List<HistoryOperation> history, int operationId, Optional<String> value) {
    return history.stream().map(operation -> operation.id() == operationId
        ? operation.withObservedValue(value) : operation).toList();
  }

  private static Map<String, Mutation> immutableMutations(Map<String, Mutation> mutations) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(mutations));
  }

  private enum Kind { COMPARE_AND_WRITE, PUT, DELETE, READ }
  private enum Outcome { COMPLETE, COMPARE_FAILED, UNKNOWN }
  private enum PublicationObservation { SELECTED, NOT_SELECTED, UNKNOWN }
  private enum MutationKind { PUT, DELETE }

  private record Mutation(MutationKind kind, Optional<String> value) {
    private Mutation {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(value, "value");
      if ((kind == MutationKind.PUT) != value.isPresent()) {
        throw new IllegalArgumentException("mutation value does not match its kind");
      }
    }

    private static Mutation put(String value) {
      return new Mutation(MutationKind.PUT, Optional.of(Objects.requireNonNull(value, "value")));
    }

    private static Mutation delete() {
      return new Mutation(MutationKind.DELETE, Optional.empty());
    }
  }

  /** Invocation and return times are distinct from the durable publication decision time. */
  private record HistoryOperation(int id, Kind kind, int invokedAt, int returnedAt,
      OptionalInt decisionAt, String key, Optional<String> expectedValue,
      Map<String, Mutation> mutations, Optional<String> observedValue,
      Outcome outcome, PublicationObservation publication) {
    private HistoryOperation {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(decisionAt, "decisionAt");
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(expectedValue, "expectedValue");
      mutations = immutableMutations(mutations);
      Objects.requireNonNull(observedValue, "observedValue");
      Objects.requireNonNull(outcome, "outcome");
      Objects.requireNonNull(publication, "publication");
      if (invokedAt < 0 || returnedAt <= invokedAt
          || decisionAt.isPresent() && (decisionAt.getAsInt() < invokedAt
              || decisionAt.getAsInt() > returnedAt)) {
        throw new IllegalArgumentException("invalid operation interval");
      }
      if (kind == Kind.READ && (outcome != Outcome.COMPLETE || !mutations.isEmpty())) {
        throw new IllegalArgumentException("read history entry has a write outcome");
      }
      if (kind != Kind.READ && mutations.isEmpty()) {
        throw new IllegalArgumentException("write history entry must contain a mutation");
      }
    }

    private static HistoryOperation compareAndWrite(int invokedAt, int returnedAt,
        OptionalInt decisionAt, String key, Optional<String> expectedValue,
        Map<String, Mutation> mutations, Outcome outcome, PublicationObservation publication) {
      return new HistoryOperation(0, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt, decisionAt,
          key, expectedValue, mutations, Optional.empty(), outcome, publication);
    }

    private static HistoryOperation mutation(int id, Kind kind, int invokedAt, int returnedAt,
        String key, Mutation mutation, OptionalInt decisionAt) {
      return new HistoryOperation(id, kind, invokedAt, returnedAt, decisionAt, key,
          Optional.empty(), Map.of(key, mutation), Optional.empty(), Outcome.COMPLETE,
          PublicationObservation.SELECTED);
    }

    private static HistoryOperation read(
        int invokedAt, int returnedAt, String key, Optional<String> observedValue) {
      return new HistoryOperation(0, Kind.READ, invokedAt, returnedAt, OptionalInt.empty(), key,
          Optional.empty(), Map.of(), observedValue, Outcome.COMPLETE,
          PublicationObservation.NOT_SELECTED);
    }

    private HistoryOperation withId(int newId) {
      return new HistoryOperation(newId, kind, invokedAt, returnedAt, decisionAt, key,
          expectedValue, mutations, observedValue, outcome, publication);
    }

    private HistoryOperation withObservedValue(Optional<String> value) {
      return new HistoryOperation(id, kind, invokedAt, returnedAt, decisionAt, key,
          expectedValue, mutations, value, outcome, publication);
    }
  }

  /** Backtracks over real-time-compatible orders and branches only on genuinely unknown outcomes. */
  private static final class SerialReferenceChecker {
    private final Map<String, String> initialValues;

    private SerialReferenceChecker(Map<String, String> initialValues) {
      this.initialValues = Map.copyOf(initialValues);
    }

    private boolean hasLinearization(List<HistoryOperation> history) {
      if (history.size() > 20) throw new IllegalArgumentException("history exceeds the bounded search size");
      List<HistoryOperation> operations = new ArrayList<>(history.size());
      for (int index = 0; index < history.size(); index++) {
        operations.add(history.get(index).withId(index));
      }
      return search(operations, 0L, initialValues);
    }

    private boolean search(List<HistoryOperation> operations, long completed,
        Map<String, String> values) {
      if (completed == (1L << operations.size()) - 1) return true;
      for (HistoryOperation operation : operations) {
        long bit = 1L << operation.id();
        if ((completed & bit) != 0
            || hasUnorderedPredecessor(operations, operation, completed)) continue;
        for (Map<String, String> next : apply(operation, values)) {
          if (search(operations, completed | bit, next)) return true;
        }
      }
      return false;
    }

    private boolean hasUnorderedPredecessor(List<HistoryOperation> operations,
        HistoryOperation candidate, long completed) {
      for (HistoryOperation predecessor : operations) {
        if (predecessor.id() == candidate.id() || (completed & (1L << predecessor.id())) != 0) continue;
        if (mustPrecede(predecessor, candidate)) return true;
      }
      return false;
    }

    private static boolean mustPrecede(HistoryOperation first, HistoryOperation second) {
      int firstLatest = first.decisionAt().orElse(first.returnedAt());
      int secondEarliest = second.decisionAt().orElse(second.invokedAt());
      return first.id() != second.id() && firstLatest <= secondEarliest;
    }

    private static List<Map<String, String>> apply(
        HistoryOperation operation, Map<String, String> values) {
      return switch (operation.kind()) {
        case READ -> Objects.equals(Optional.ofNullable(values.get(operation.key())),
            operation.observedValue()) ? List.of(values) : List.of();
        case PUT, DELETE -> applyWrite(operation, values);
        case COMPARE_AND_WRITE -> applyCompareWrite(operation, values);
      };
    }

    private static List<Map<String, String>> applyCompareWrite(
        HistoryOperation operation, Map<String, String> values) {
      boolean matches = Objects.equals(Optional.ofNullable(values.get(operation.key())),
          operation.expectedValue());
      if (!matches) {
        return operation.outcome() == Outcome.COMPARE_FAILED
            && operation.publication() == PublicationObservation.NOT_SELECTED ? List.of(values) : List.of();
      }
      if (operation.outcome() == Outcome.COMPARE_FAILED) return List.of();
      return applyWrite(operation, values);
    }

    private static List<Map<String, String>> applyWrite(
        HistoryOperation operation, Map<String, String> values) {
      if (operation.outcome() == Outcome.COMPARE_FAILED) return List.of();
      if (operation.publication() == PublicationObservation.SELECTED) {
        return operation.outcome() == Outcome.COMPLETE || operation.outcome() == Outcome.UNKNOWN
            ? List.of(applyMutations(values, operation.mutations())) : List.of();
      }
      if (operation.publication() == PublicationObservation.UNKNOWN
          && operation.outcome() == Outcome.UNKNOWN) {
        Map<String, String> unchanged = Map.copyOf(values);
        Map<String, String> changed = applyMutations(values, operation.mutations());
        return changed.equals(unchanged) ? List.of(unchanged) : List.of(unchanged, changed);
      }
      return List.of();
    }

    private static Map<String, String> applyMutations(
        Map<String, String> values, Map<String, Mutation> mutations) {
      Map<String, String> changed = new HashMap<>(values);
      mutations.forEach((key, mutation) -> {
        if (mutation.kind() == MutationKind.PUT) changed.put(key, mutation.value().orElseThrow());
        else changed.remove(key);
      });
      return Map.copyOf(changed);
    }
  }

  /** Drives the production immutable control state while keeping reference semantics separate. */
  private static final class KernelDriver {
    private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);

    private final Map<String, String> physical = new HashMap<>();
    private Map<String, Mutation> selectedCandidates = Map.of();
    private CommitState control = CommitState.initial(PROFILE, 1).claimOwner("history-owner", 1, 100);
    private int nextOperationId = 1;
    private int nextIdentity = 1;

    private KernelDriver(Map<String, String> initialValues) {
      physical.putAll(initialValues);
    }

    private PendingTransaction beginCompareAndWrite(int invokedAt, String key,
        Optional<String> expectedValue, Map<String, Mutation> mutations) {
      if (!Objects.equals(Optional.ofNullable(visible(key)), expectedValue)) {
        throw new IllegalStateException("fixture compare must pass before publication");
      }
      CommitState base = control;
      TransactionId transaction = transactionId(nextIdentity);
      InvocationId invocation = invocationId(nextIdentity++);
      ByteKey root = ByteKey.copyOf(new byte[] {(byte) nextIdentity});
      ByteKey digest = ByteKey.copyOf(new byte[32]);
      PublicationSlot slot = new PublicationSlot(transaction, invocation, base.ownerEpoch(),
          base.publishedSequence(), base.publishedRevision(),
          Math.addExact(base.publishedSequence(), 1), Math.addExact(base.publishedRevision(), 1),
          true, root, digest);
      control = base.publish(slot);
      selectedCandidates = immutableMutations(mutations);
      int id = nextOperationId++;
      return new PendingTransaction(this, id, invokedAt, key, expectedValue,
          immutableMutations(mutations), slot);
    }

    private HistoryOperation compareAndWrite(int invokedAt, int returnedAt, String key,
        Optional<String> expectedValue, Map<String, Mutation> mutations) {
      int id = nextOperationId++;
      if (!Objects.equals(Optional.ofNullable(visible(key)), expectedValue)) {
        return new HistoryOperation(id, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt,
            OptionalInt.empty(), key, expectedValue, mutations, Optional.empty(),
            Outcome.COMPARE_FAILED, PublicationObservation.NOT_SELECTED);
      }
      throw new IllegalStateException("this bounded driver only accepts comparisons that fail after selection");
    }

    private HistoryOperation delete(int invokedAt, int returnedAt, String key) {
      Mutation mutation = Mutation.delete();
      publishAndFinalize(key, mutation);
      return HistoryOperation.mutation(nextOperationId++, Kind.DELETE, invokedAt, returnedAt,
          key, mutation, OptionalInt.of(invokedAt + 1));
    }

    private HistoryOperation put(int invokedAt, int returnedAt, String key, String value) {
      Mutation mutation = Mutation.put(value);
      publishAndFinalize(key, mutation);
      return HistoryOperation.mutation(nextOperationId++, Kind.PUT, invokedAt, returnedAt,
          key, mutation, OptionalInt.of(invokedAt + 1));
    }

    private void publishAndFinalize(String key, Mutation mutation) {
      if (control.publication().isPresent()) throw new IllegalStateException("publication must be finalized first");
      CommitState base = control;
      TransactionId transaction = transactionId(nextIdentity);
      InvocationId invocation = invocationId(nextIdentity++);
      ByteKey root = ByteKey.copyOf(new byte[] {(byte) nextIdentity});
      PublicationSlot slot = new PublicationSlot(transaction, invocation, base.ownerEpoch(),
          base.publishedSequence(), base.publishedRevision(),
          Math.addExact(base.publishedSequence(), 1), Math.addExact(base.publishedRevision(), 1),
          true, root, ByteKey.copyOf(new byte[32]));
      control = base.publish(slot);
      selectedCandidates = Map.of(key, mutation);
      applySelected(selectedCandidates);
      control = control.finalizePublication(transaction);
      selectedCandidates = Map.of();
    }

    private HistoryOperation read(int invokedAt, int returnedAt, String key) {
      return new HistoryOperation(nextOperationId++, Kind.READ, invokedAt, returnedAt,
          OptionalInt.empty(), key, Optional.empty(), Map.of(), Optional.ofNullable(visible(key)),
          Outcome.COMPLETE, PublicationObservation.NOT_SELECTED);
    }

    private String visible(String key) {
      if (control.publication().isPresent() && selectedCandidates.containsKey(key)) {
        Mutation selected = selectedCandidates.get(key);
        return selected.kind() == MutationKind.PUT ? selected.value().orElseThrow() : null;
      }
      return physical.get(key);
    }

    private void applySelected(Map<String, Mutation> mutations) {
      mutations.forEach((key, mutation) -> {
        if (mutation.kind() == MutationKind.PUT) physical.put(key, mutation.value().orElseThrow());
        else physical.remove(key);
      });
    }

    private void recoverSelectedPublication() {
      if (control.publication().isEmpty()) return;
      PublicationSlot slot = control.publication().orElseThrow();
      applySelected(selectedCandidates);
      control = control.finalizePublication(slot.transactionId());
      selectedCandidates = Map.of();
    }
  }

  private static final class PendingTransaction {
    private final KernelDriver driver;
    private final int id;
    private final int invokedAt;
    private final String key;
    private final Optional<String> expectedValue;
    private final Map<String, Mutation> mutations;
    private final PublicationSlot slot;
    private boolean materialized;
    private int materializedCount;

    private PendingTransaction(KernelDriver driver, int id, int invokedAt, String key, Optional<String> expectedValue,
        Map<String, Mutation> mutations, PublicationSlot slot) {
      this.driver = driver;
      this.id = id;
      this.invokedAt = invokedAt;
      this.key = key;
      this.expectedValue = expectedValue;
      this.mutations = mutations;
      this.slot = slot;
    }

    private void materializePrefix(int count) {
      if (count < 0 || count >= mutations.size()) {
        throw new IllegalArgumentException("partial materialization must leave a candidate pending");
      }
      int applied = 0;
      for (Map.Entry<String, Mutation> entry : mutations.entrySet()) {
        if (applied++ == count) break;
        Mutation mutation = entry.getValue();
        if (mutation.kind() == MutationKind.PUT) driver.physical.put(entry.getKey(), mutation.value().orElseThrow());
        else driver.physical.remove(entry.getKey());
        materialized = true;
        materializedCount++;
      }
    }

    private HistoryOperation cancelCallerAfterSelection(RequestScope callerScope, int returnedAt) {
      Objects.requireNonNull(callerScope, "callerScope");
      if (!materialized || driver.control.publication().filter(slot::equals).isEmpty()) {
        throw new IllegalStateException("cancellation must follow a selected and partially materialized transaction");
      }
      callerScope.cancel();
      return new HistoryOperation(id, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt,
          OptionalInt.of(invokedAt + 1), key, expectedValue, mutations, Optional.empty(),
          Outcome.UNKNOWN, PublicationObservation.SELECTED);
    }
  }

  private static TransactionId transactionId(int value) {
    return new TransactionId(new UUID(0, value));
  }

  private static InvocationId invocationId(int value) {
    return new InvocationId(new UUID(1, value));
  }
}
