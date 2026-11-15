package io.etcdhbase.server;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Captures source-bound public evidence for the J29.4 Lease and business mixed workload. */
@Timeout(600)
class M4MixedLeaseBenchmarkIT {
  @TempDir Path testDirectory;

  @Test
  void emitsMixedLeaseLoadEvidenceAtIncreasingConcurrency() throws Exception {
    M4LeaseRpcIT.verifyMixedLeaseLoadEvidence(testDirectory);
  }
}
