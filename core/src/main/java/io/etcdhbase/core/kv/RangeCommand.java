package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.storage.StorageMetadata;
import java.util.Objects;

/** Range input without transport types or loss of the request's original revision semantics. */
public record RangeCommand(KeyRange range, Context context, long revision, long limit, Sort sort,
    RevisionFilter filter, boolean serializable, boolean keysOnly, boolean countOnly) {
  /** Etcd treats negative revisions differently for standalone and selected transaction reads. */
  public enum Context { STANDALONE, TRANSACTION }

  /** Raw protocol values are retained so unknown and unsupported options can be rejected. */
  public record Sort(int order, int target) {}

  /** Zero means no boundary; nonzero bounds are inclusive. */
  public record RevisionFilter(long minModRevision, long maxModRevision,
      long minCreateRevision, long maxCreateRevision) {
    public boolean matches(StorageMetadata metadata) {
      Objects.requireNonNull(metadata, "metadata");
      return (minModRevision == 0 || metadata.modificationRevision() >= minModRevision)
          && (maxModRevision == 0 || metadata.modificationRevision() <= maxModRevision)
          && (minCreateRevision == 0 || metadata.createRevision() >= minCreateRevision)
          && (maxCreateRevision == 0 || metadata.createRevision() <= maxCreateRevision);
    }
  }

  public RangeCommand {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(sort, "sort");
    Objects.requireNonNull(filter, "filter");
    if (range.kind() == KeyRange.Kind.POINT && range.lowerInclusive().orElseThrow().length() == 0) {
      throw new KvFailure(KvFailure.Reason.EMPTY_KEY);
    }
  }
}
