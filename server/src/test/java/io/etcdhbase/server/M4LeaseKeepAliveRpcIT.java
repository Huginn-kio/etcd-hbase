package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M4LeaseKeepAliveRpcIT {
  @TempDir Path testDirectory;

  @Test
  void realGoKeepAliveMatchesNativeAcrossLayouts() throws Exception {
    M4LeaseRpcIT.verifyGoLeaseKeepAliveMatchesNativeAcrossLayouts();
  }

  @Test
  void expiredActiveLeaseIsRejectedUntilDurableRevocation() throws Exception {
    M4LeaseRpcIT.verifyExpiredActiveLeaseIsRejectedUntilDurableRevocation();
  }

  @Test
  void dualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner() throws Exception {
    M4LeaseRpcIT.verifyDualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner(testDirectory);
  }

  @Test
  void persistedKeepAliveSurvivesLostOwnerReplyAndRestart() throws Exception {
    assertTrue(M4LeaseRpcIT.verifyPersistedKeepAliveSurvivesLostOwnerReplyAndRestart(testDirectory));
  }

  @Test
  void persistedKeepAliveSurvivesClientCancellationBeforeResponse() throws Exception {
    M4LeaseRpcIT.verifyPersistedKeepAliveSurvivesClientCancellationBeforeResponse();
  }
}
