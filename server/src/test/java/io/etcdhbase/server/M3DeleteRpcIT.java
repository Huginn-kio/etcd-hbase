package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Compares standalone and transactional deletes with the pinned native server. */
@Timeout(600)
class M3DeleteRpcIT {
  @Test
  void goDeletesMatchNativeOnBothLayouts() throws Exception {
    M3TxnRpcIT.runScenario("j33", "m3-delete.yaml");
  }
}
