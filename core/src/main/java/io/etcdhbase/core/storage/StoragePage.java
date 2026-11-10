package io.etcdhbase.core.storage;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Physical rows are client-returned Result rows; observations are sampled counters, not total IO. */
public record StoragePage(List<StoredRecord> records, boolean exhausted, Cost cost,
    Optional<PhysicalScanProgress> physicalProgress) {
  public StoragePage(List<StoredRecord> records, boolean exhausted, Cost cost) {
    this(records, exhausted, cost, Optional.empty());
  }

  public StoragePage {
    records = List.copyOf(records);
    if (cost == null) throw new NullPointerException("cost");
    physicalProgress = java.util.Objects.requireNonNull(physicalProgress, "physicalProgress");
    if (physicalProgress.isPresent() && (exhausted || !records.isEmpty())) {
      throw new IllegalArgumentException("physical progress must not contain logical results");
    }
  }

  public enum Observation {
    SERVER_ROWS_SCANNED,
    BLOCK_BYTES_SCANNED,
    FS_BYTES,
    BLOCK_CACHE_BYTES,
    MEMSTORE_BYTES,
    SCANNER_RETRIES,
    MANIFEST_DIRECTORY_PAGES,
    MANIFEST_LEAF_PAGES,
    MANIFEST_CANDIDATE_RECORDS,
    MANIFEST_CANDIDATE_BYTES
  }

  public record Cost(long physicalRows, long returnedCellBytes, long scanRpcCalls, Map<Observation, Long> observations) {
    public Cost(long physicalRows, long returnedCellBytes, long scanRpcCalls) {
      this(physicalRows, returnedCellBytes, scanRpcCalls, Map.of());
    }
    public Cost {
      if (physicalRows < 0 || returnedCellBytes < 0 || scanRpcCalls < 0) {
        throw new IllegalArgumentException("negative storage cost");
      }
      observations = Map.copyOf(observations);
      if (observations.values().stream().anyMatch(value -> value < 0)) throw new IllegalArgumentException("negative observed counter");
    }

    public Cost plus(Cost other) {
      var merged = new EnumMap<Observation, Long>(Observation.class);
      merged.putAll(observations);
      other.observations.forEach((key, value) -> merged.merge(key, value, Math::addExact));
      return new Cost(Math.addExact(physicalRows, other.physicalRows), Math.addExact(returnedCellBytes, other.returnedCellBytes),
          Math.addExact(scanRpcCalls, other.scanRpcCalls), merged);
    }
  }
}
