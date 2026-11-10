package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Compares nested transactions with the pinned native server on both HBase layouts. */
@Timeout(600)
class M3NestedRpcIT {
  @Test
  void goNestedTransactionsMatchNativeOnBothLayouts() throws Exception {
    M3TxnRpcIT.runScenario("j32", "m3-nested-txn.yaml");
  }
}
