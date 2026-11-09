package io.etcdhbase.server.commit;

import io.etcdhbase.core.kv.ResultTree;

/** Request-local bounds shared by transaction readers and owner-side mutation planning. */
public record TxnExecutionLimits(long pointBytes, long resultBytes, long workingBytes,
    long pageBytes, int physicalRowsPerPage) {
  public TxnExecutionLimits {
    if (pointBytes < 1 || resultBytes < 128 || resultBytes > ResultTree.MAX_RETAINED_BYTES
        || workingBytes < 1 || workingBytes > ResultTree.MAX_RETAINED_BYTES || pageBytes < 1
        || pageBytes > 64L * 1024 * 1024 || physicalRowsPerPage < 1
        || physicalRowsPerPage > 1_000_000) {
      throw new IllegalArgumentException("invalid transaction execution limits");
    }
  }
}
