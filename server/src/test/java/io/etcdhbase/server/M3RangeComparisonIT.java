package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Compares pinned Go client traces across native etcd and both disposable HBase layouts. */
@Timeout(600)
class M3RangeComparisonIT {
  @Test
  void goRangesMatchNativeOnBothLayouts() throws Exception {
    M3TxnRpcIT.runScenario("j34", "m3-range.yaml");
  }
}
