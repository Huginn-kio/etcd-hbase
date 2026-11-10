package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies only the disposable backend fixture; it is not an M3 business RPC test. */
class M3FixtureIT {
  @Test
  void singleBucketWorksForBothDisposableLayouts() throws Exception {
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout, 1);
           RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        assertEquals(1, fixture.salt().profile().bucketCount());
        ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});
        byte[] value = {7, 0, 8};
        var record = StoredRecord.live(key,
            new StorageMetadata(2, 2, 1, 0, StorageMetadata.State.LIVE, value.length), value);
        fixture.openPrimary(scope).put(record).join();
        var actual = fixture.openPrimary(scope).get(key, 2, ReadProjection.FULL).join().orElseThrow();
        assertArrayEquals(value, actual.value().orElseThrow());
        assertEquals(0, fixture.primary().codec().bucket(key));
      }
    }
  }

  @Test
  void provisionsBothLayoutsAndReadsBackFromRealHBase() throws Exception {
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout);
           RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        assertEquals(fixture.identity().scope().tableGroupId(), fixture.system().tableGroupId());
        assertEquals(layout, fixture.primary().layout());
        assertEquals(layout == StorageTableBinding.Layout.HISTORY, fixture.current().isPresent());
        assertNotEquals(0, fixture.clusterId());
        assertEquals(fixture.clusterId(), new TableGroupIdentityService(fixture.records())
            .readExisting(scope).join());
        assertEquals(1, fixture.control().read(scope).join().orElseThrow().state().publishedRevision());
        assertNotNull(fixture.pins().readBoundary(scope).join());

        ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});
        byte[] value = {2, 0, (byte) 0xfe};
        var record = StoredRecord.live(key,
            new StorageMetadata(2, 2, 1, 0, StorageMetadata.State.LIVE, value.length), value);
        fixture.openPrimary(scope).put(record).join();
        StoredRecord readBack = fixture.openPrimary(scope).get(key, 2, ReadProjection.FULL)
            .join().orElseThrow();
        assertEquals(record.metadata(), readBack.metadata());
        assertArrayEquals(value, readBack.value().orElseThrow());
        assertTrue(fixture.openPrimary(scope).get(key, 1, ReadProjection.FULL).join().isEmpty());
      }
    }
  }
}
