package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies durable expiry checkpoints and atomic Lease Revoke in both HBase layouts. */
@Timeout(600)
class M4LeaseRevokeRpcIT {
  @Test
  void realGoLeaseRevokeMatchesNativeAcrossLayouts() throws Exception {
    M4LeaseRpcIT.verifyGoLeaseRevokeMatchesNativeAcrossLayouts();
  }

  @Test
  void expiryCheckpointResumesAfterRuntimeRestart() throws Exception {
    M4LeaseRpcIT.verifyExpiryCheckpointResumesAfterRuntimeRestart();
  }

  @Test
  void revokeAndKeepAliveRaceHasOneDurableTerminalOutcome() throws Exception {
    M4LeaseRpcIT.verifyRevokeAndKeepAliveRaceHasOneDurableTerminalOutcome();
  }

  @Test
  void staleExpiryCandidatesPreserveReboundAndUnleasedRecreatedKeys() throws Exception {
    M4LeaseRpcIT.verifyStaleExpiryCandidatesPreserveReboundAndUnleasedRecreatedKeys();
  }

  @Test
  void largeLeaseRevokeDeletesEveryBindingInOneRevision() throws Exception {
    M4LeaseRpcIT.verifyLargeLeaseRevokeDeletesEveryBindingInOneRevision();
  }
}
