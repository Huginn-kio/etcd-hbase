package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Materializes versioned lease records through the same strongly-consistent system-record port. */
public final class SystemRecordAuxiliaryMaterializationStore
    implements AuxiliaryMaterializationStore {
  private static final int MAX_POINTER_CAS_ATTEMPTS = 32;

  private final SystemRecordStore records;
  private final Function<ByteKey, LeaseState> decodeLeaseState;
  private final Function<ByteKey, LeaseBindingVersion> decodeLeaseBinding;

  public SystemRecordAuxiliaryMaterializationStore(SystemRecordStore records,
      Function<ByteKey, LeaseState> decodeLeaseState) {
    this(records, decodeLeaseState, null);
  }

  public SystemRecordAuxiliaryMaterializationStore(SystemRecordStore records,
      Function<ByteKey, LeaseState> decodeLeaseState,
      Function<ByteKey, LeaseBindingVersion> decodeLeaseBinding) {
    this.records = Objects.requireNonNull(records, "records");
    this.decodeLeaseState = Objects.requireNonNull(decodeLeaseState, "decodeLeaseState");
    this.decodeLeaseBinding = decodeLeaseBinding;
  }

  @Override
  public CompletableFuture<Void> preflight(PreparedAuxiliaryRecord record, RequestScope scope) {
    Objects.requireNonNull(record, "record");
    Objects.requireNonNull(scope, "scope");
    try {
      validateRecord(record, targetSequence(record));
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return records.read(record.targetType(), record.targetIdentity(), scope).thenCompose(existing -> {
      if (record.targetType() == SystemRecordType.LEASE_ENUMERATION) {
        validatePointerBeforeSelection(record, existing);
      } else if (record.targetType() == SystemRecordType.LEASE_EXPIRY_CHECKPOINT) {
        return validateCheckpointBeforeSelection(record, existing, scope);
      } else if (record.targetType() == SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT) {
        return validateEventCheckpointBeforeSelection(record, existing, scope);
      } else if (existing.isPresent() && !existing.orElseThrow().equals(record.payload())) {
        throw conflict("immutable auxiliary target already has different bytes");
      }
      return CompletableFuture.completedFuture(null);
    });
  }

  @Override
  public CompletableFuture<ApplyResult> applySelected(CommitDecision decision,
      PreparedAuxiliaryRecord record, RequestScope scope) {
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(record, "record");
    Objects.requireNonNull(scope, "scope");
    try {
      if (!record.transactionId().equals(decision.transactionId())
          || targetSequence(record) != decision.sequence()) {
        throw new IllegalArgumentException("auxiliary record differs from its selected decision");
      }
      validateRecord(record, decision.sequence());
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    if (record.targetType() == SystemRecordType.LEASE_ENUMERATION) {
      return updatePointer(record, 0, scope);
    }
    return records.createIfAbsent(record.targetType(), record.targetIdentity(), record.payload(), scope)
        .thenApply(result -> switch (result) {
          case CREATED -> ApplyResult.APPLIED;
          case ALREADY_PRESENT -> ApplyResult.ALREADY_APPLIED;
          case CONFLICT -> throw conflict("immutable auxiliary target conflicts with selected bytes");
        });
  }

  private CompletableFuture<ApplyResult> updatePointer(
      PreparedAuxiliaryRecord record, int attempt, RequestScope scope) {
    if (attempt >= MAX_POINTER_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(
          new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    return records.read(SystemRecordType.LEASE_ENUMERATION, record.targetIdentity(), scope)
        .thenCompose(current -> compareAndAdvance(record, current, attempt, scope));
  }

  private CompletableFuture<ApplyResult> compareAndAdvance(PreparedAuxiliaryRecord record,
      Optional<ByteKey> current, int attempt, RequestScope scope) {
    Optional<ApplyResult> existing = resolveExistingPointer(record, current);
    if (existing.isPresent()) return completed(existing.orElseThrow());
    return records.compareAndSet(SystemRecordType.LEASE_ENUMERATION, record.targetIdentity(),
        current, record.payload(), scope).handle((updated, failure) -> new CasResult(updated, failure))
        .thenCompose(result -> finishPointerCas(record, attempt, scope, result));
  }

  private Optional<ApplyResult> resolveExistingPointer(
      PreparedAuxiliaryRecord record, Optional<ByteKey> existing) {
    if (existing.isEmpty()) return Optional.empty();
    ByteKey current = existing.orElseThrow();
    long observedSequence = LeaseEnumerationPointer.decode(current).sequence();
    LeaseEnumerationPointer replacement = LeaseEnumerationPointer.decode(record.payload());
    if (observedSequence > replacement.sequence()) return Optional.of(ApplyResult.SUPERSEDED);
    if (observedSequence < replacement.sequence()) return Optional.empty();
    if (!current.equals(record.payload())) {
      throw conflict("same-sequence lease pointer has different bytes");
    }
    return Optional.of(ApplyResult.ALREADY_APPLIED);
  }

  private CompletableFuture<ApplyResult> finishPointerCas(PreparedAuxiliaryRecord record,
      int attempt, RequestScope scope, CasResult result) {
    if (result.failure() == null && Boolean.TRUE.equals(result.updated())) {
      return completed(ApplyResult.APPLIED);
    }
    if (result.failure() == null) return updatePointer(record, attempt + 1, scope);
    return reconcilePointerCasFailure(record, attempt, scope, result.failure());
  }

  private CompletableFuture<ApplyResult> reconcilePointerCasFailure(PreparedAuxiliaryRecord record,
      int attempt, RequestScope scope, Throwable casFailure) {
    Throwable originalFailure = CompletionFailures.unwrapCompletion(casFailure);
    return records.read(SystemRecordType.LEASE_ENUMERATION, record.targetIdentity(), scope)
        .handle((observed, readFailure) -> resolvePointerAfterCasFailure(
            record, originalFailure, observed, readFailure))
        .thenCompose(resolution -> resolution.isPresent()
            ? completed(resolution.orElseThrow()) : updatePointer(record, attempt + 1, scope));
  }

  private Optional<ApplyResult> resolvePointerAfterCasFailure(PreparedAuxiliaryRecord record,
      Throwable casFailure, Optional<ByteKey> observed, Throwable readFailure) {
    if (readFailure != null) {
      Throwable unwrapped = CompletionFailures.unwrapCompletion(readFailure);
      if (unwrapped != casFailure) casFailure.addSuppressed(unwrapped);
      throw new java.util.concurrent.CompletionException(casFailure);
    }
    Optional<ApplyResult> resolution = resolveExistingPointer(record, observed);
    if (resolution.isPresent()) return resolution;
    if (isUnknown(casFailure)) return Optional.empty();
    throw new java.util.concurrent.CompletionException(casFailure);
  }

  private static void validatePointerBeforeSelection(
      PreparedAuxiliaryRecord record, Optional<ByteKey> existing) {
    LeaseEnumerationPointer replacement = LeaseEnumerationPointer.decode(record.payload());
    if (replacement.sequence() != targetSequence(record)) {
      throw new IllegalArgumentException("lease pointer must target the next commit sequence");
    }
    if (existing.isEmpty()) return;
    LeaseEnumerationPointer current = LeaseEnumerationPointer.decode(existing.orElseThrow());
    if (current.sequence() > replacement.sequence()) {
      throw conflict("lease pointer is ahead of the preparing commit sequence");
    }
    if (current.sequence() == replacement.sequence()
        && !existing.orElseThrow().equals(record.payload())) {
      throw conflict("same-sequence lease pointer has different bytes");
    }
  }

  private void validateRecord(PreparedAuxiliaryRecord record, long targetSequence) {
    switch (record.targetType()) {
      case LEASE_STATE -> {
        LeaseState state = Objects.requireNonNull(decodeLeaseState.apply(record.payload()), "lease state");
        if (LeaseRecordIdentity.sequence(record.targetIdentity()) != targetSequence
            || state.sequence() != targetSequence
            || state.identity().leaseId() != LeaseRecordIdentity.leaseId(record.targetIdentity())) {
          throw new IllegalArgumentException("lease state differs from its target identity or sequence");
        }
      }
      case LEASE_ENUMERATION -> {
        if (record.targetIdentity().length() != LeaseRecordIdentity.ROUTE_BYTES
            || LeaseEnumerationPointer.decode(record.payload()).sequence() != targetSequence) {
          throw new IllegalArgumentException("lease enumeration pointer differs from its commit sequence");
        }
      }
      case EXPIRY_CANDIDATE -> {
        ExpiryCandidateIdentity.decode(record.targetIdentity());
        if (ExpiryCandidateRecord.decode(record.payload()).stateSequence() > targetSequence) {
          throw new IllegalArgumentException("expiry candidate differs from its commit sequence");
        }
      }
      case LEASE_RESULT -> {
        if (record.targetIdentity().length() < 16) {
          throw new IllegalArgumentException("lease result identity is too short");
        }
      }
      case LEASE_BINDING -> {
        LeaseBindingIdentity.Decoded identity = LeaseBindingIdentity.decode(record.targetIdentity());
        if (identity.sequence() != targetSequence || decodeLeaseBinding == null) {
          throw new IllegalArgumentException("Lease binding differs from its commit sequence");
        }
        LeaseBindingVersion binding = Objects.requireNonNull(
            decodeLeaseBinding.apply(record.payload()), "Lease binding");
        if (binding.sequence() != targetSequence
            || binding.identity().leaseId() != identity.leaseId()
            || binding.identity().incarnation() != identity.incarnation()
            || !binding.key().equals(identity.key())) {
          throw new IllegalArgumentException("Lease binding payload differs from its identity");
        }
      }
      case LEASE_EXPIRY_CHECKPOINT -> {
        LeaseExpiryCheckpoint checkpoint = LeaseExpiryCheckpoint.decode(record.payload());
        if (!record.targetIdentity().equals(LeaseExpiryCheckpoint.versionIdentity(targetSequence))) {
          throw new IllegalArgumentException("Lease expiry checkpoint target identity is invalid");
        }
        if (checkpoint.ownerEpoch() != record.ownerEpoch() || checkpoint.sequence() != targetSequence
            || checkpoint.protectedSequence() != record.baseSequence()) {
          throw new IllegalArgumentException("Lease expiry checkpoint differs from its selection");
        }
      }
      case EVENT_INDEX_BACKFILL_CHECKPOINT -> {
        EventIndexBackfillCheckpoint checkpoint = EventIndexBackfillCheckpoint.decode(record.payload());
        if (!record.targetIdentity().equals(EventIndexBackfillCheckpoint.versionIdentity(targetSequence))
            || checkpoint.sequence() != targetSequence || checkpoint.ownerEpoch() != record.ownerEpoch()) {
          throw new IllegalArgumentException("event backfill checkpoint differs from its selection");
        }
      }
      default -> throw new IllegalArgumentException("unsupported auxiliary target type");
    }
  }

  private CompletableFuture<Void> validateEventCheckpointBeforeSelection(PreparedAuxiliaryRecord record,
      Optional<ByteKey> existing, RequestScope scope) {
    EventIndexBackfillCheckpoint replacement = EventIndexBackfillCheckpoint.decode(record.payload());
    if (existing.isPresent() && !existing.orElseThrow().equals(record.payload())) {
      return CompletableFuture.failedFuture(conflict("same-sequence event backfill checkpoints differ"));
    }
    return records.latestAtOrBefore(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
        EventIndexBackfillCheckpoint.routePrefix(), record.baseSequence(), scope).thenAccept(previous -> {
      if (previous.isEmpty()) {
        if (replacement.nextSequence() != replacement.firstSequence()) {
          throw conflict("initial event backfill checkpoint must start at the captured boundary");
        }
        return;
      }
      SystemRecordEntry entry = previous.orElseThrow();
      EventIndexBackfillCheckpoint current = EventIndexBackfillCheckpoint.decode(entry.value());
      if (EventIndexBackfillCheckpoint.sequence(entry.identity()) != current.sequence()
          || current.sequence() > record.baseSequence()) {
        throw conflict("stored event backfill checkpoint differs from its identity or read barrier");
      }
      try {
        EventIndexBackfillCheckpoint.requireForwardProgress(current, replacement);
      } catch (IllegalArgumentException stale) {
        throw conflict("event backfill checkpoint does not advance protected durable progress");
      }
    });
  }

  private CompletableFuture<Void> validateCheckpointBeforeSelection(PreparedAuxiliaryRecord record,
      Optional<ByteKey> existing, RequestScope scope) {
    LeaseExpiryCheckpoint replacement = LeaseExpiryCheckpoint.decode(record.payload());
    if (!record.targetIdentity().equals(LeaseExpiryCheckpoint.versionIdentity(replacement.sequence()))
        || replacement.sequence() != targetSequence(record)
        || replacement.protectedSequence() != record.baseSequence()) {
      return CompletableFuture.failedFuture(
          conflict("Lease expiry checkpoint target identity differs from its selected sequence"));
    }
    if (existing.isPresent() && !existing.orElseThrow().equals(record.payload())) {
      return CompletableFuture.failedFuture(
          conflict("same-sequence Lease expiry checkpoints differ"));
    }
    return records.latestAtOrBefore(SystemRecordType.LEASE_EXPIRY_CHECKPOINT,
        LeaseExpiryCheckpoint.routePrefix(), record.baseSequence(), scope).thenAccept(previous -> {
      if (previous.isEmpty()) {
        if (replacement.position().windowStartInclusive() != 0) {
          throw conflict("initial Lease expiry checkpoint must start at the first deadline boundary");
        }
        return;
      }
      LeaseExpiryCheckpoint current = LeaseExpiryCheckpoint.decode(previous.orElseThrow().value());
      try {
        LeaseExpiryCheckpoint.requireForwardProgress(current, replacement);
      } catch (IllegalArgumentException stale) {
        throw conflict("Lease expiry checkpoint does not advance protected durable progress");
      }
    });
  }

  private static long targetSequence(PreparedAuxiliaryRecord record) {
    return Math.addExact(record.baseSequence(), 1);
  }

  private static boolean isUnknown(Throwable failure) {
    return failure instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private static IllegalStateException conflict(String message) {
    return new IllegalStateException(message);
  }

  private static <T> CompletableFuture<T> completed(T value) {
    return CompletableFuture.completedFuture(value);
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private record CasResult(Boolean updated, Throwable failure) {}
}
