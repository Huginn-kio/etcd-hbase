package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Compares sorted Go client requests with native etcd and both temporary HBase layouts. */
@Timeout(600)
class M3SortComparisonIT {
  @Test
  void goSortedRangesMatchNativeOnBothLayouts() throws Exception {
    M3TxnRpcIT.runScenario("j35", "m3-sort.yaml");
  }
}
