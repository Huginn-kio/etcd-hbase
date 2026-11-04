package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.StorageMessages;
import java.lang.reflect.Proxy;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.jupiter.api.Test;

class HBaseMaterializationStoreTest {
  @Test
  void rejectsOversizedCellsBeforeAccessingTheirBackingArrays() {
    Cell cell = cell();

    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyMetadata(guardedValue(cell, 4097)));
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyValuePayload(guardedValue(cell,
            StorageMessages.MAX_VALUE_BYTES + 129)));
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyFence(guardedValue(cell,
            RowFenceMessages.MAX_BYTES + 1)));
  }

  private static Cell cell() {
    return new KeyValue(new byte[] {1}, new byte[] {'d'}, new byte[] {'q'}, 1L, new byte[] {1});
  }

  private static Cell guardedValue(Cell cell, int length) {
    return (Cell) Proxy.newProxyInstance(Cell.class.getClassLoader(), new Class<?>[] {Cell.class},
        (proxy, method, arguments) -> {
          if (method.getName().equals("getValueLength")) return length;
          if (method.getName().equals("getValueArray")) {
            throw new AssertionError("oversized cell value accessed before rejection");
          }
          return method.invoke(cell, arguments);
        });
  }
}
