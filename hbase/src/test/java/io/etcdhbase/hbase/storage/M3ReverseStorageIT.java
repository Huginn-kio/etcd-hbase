package io.etcdhbase.hbase.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Focused real-HBase coverage of reverse pages, continuation and ownership. */
@Timeout(600)
class M3ReverseStorageIT {
  @Test
  void reversePagesAreBoundedAndResumeAcrossVersionsOnBothLayouts() throws Exception {
    try {
      M1StorageIT.start();
      var fixture = new M1StorageIT();
      fixture.reversedPhysicalPagesSelectVisibleVersionsOnBothLayouts();
      fixture.reversedHistoryMakesProgressAcrossAKeyWithManyFutureVersions();
      fixture.reversedHistoryCursorAccountsForEveryPhysicalContinuation();
      fixture.reversedHistoryChecksAllFutureVersionsBeforeReportingAbsence();
      fixture.physicalContinuationCannotBeReusedWithAnotherStore();
    } finally {
      M1StorageIT.close();
    }
  }
}
