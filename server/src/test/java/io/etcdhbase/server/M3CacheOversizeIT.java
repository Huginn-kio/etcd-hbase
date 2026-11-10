package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Demonstrates the existing post-decode guard and its HBase client boundary. */
@Timeout(300)
class M3CacheOversizeIT {
  @Test
  void oversizedSystemCellReachesTheClientButIsRejectedBeforeApplicationCopy() throws Exception {
    try (var fixture = M3TestEnvironment.start(StorageTableBinding.Layout.NATIVE);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(2))) {
      var identity = ByteKey.copyOf("oversized-record-fixture".getBytes(StandardCharsets.UTF_8));
      var row = fixture.system().row(SystemRecordType.MANIFEST, identity);
      var family = fixture.system().family();
      var qualifier = new byte[] {'r'};
      var value = new byte[HBaseSystemRecordStore.MAX_RECORD_BYTES + 1];
      var connection = fixture.factory().connectionFor(fixture.identity()).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      var table = connection.getTable(fixture.system().table());
      table.put(new Put(row).addColumn(family, qualifier, value)).get(60, TimeUnit.SECONDS);

      // A raw HBase response already contains the whole oversized cell. This is evidence of the
      // current boundary, not a claim that transport or pre-decode memory is bounded.
      var raw = table.get(new Get(row).addColumn(family, qualifier)).get(60, TimeUnit.SECONDS);
      assertEquals(value.length, raw.rawCells()[0].getValueLength());
      var failure = assertThrows(ExecutionException.class,
          () -> fixture.records().read(SystemRecordType.MANIFEST, identity, scope).get(60, TimeUnit.SECONDS));
      assertInstanceOf(IllegalArgumentException.class, failure.getCause());
      assertEquals("system record value exceeds its storage budget", failure.getCause().getMessage());
    }
  }
}
