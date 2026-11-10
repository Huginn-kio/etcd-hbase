package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;

class HBaseSystemRecordDecodeTest {
  @Test
  void rejectsOversizedCellBeforeCopyingItsValue() {
    var cell = mock(Cell.class);
    when(cell.getQualifierArray()).thenReturn(new byte[] {'r'});
    when(cell.getQualifierLength()).thenReturn(1);
    when(cell.getValueLength()).thenReturn(HBaseSystemRecordStore.MAX_RECORD_BYTES + 1);
    var result = mock(Result.class);
    when(result.rawCells()).thenReturn(new Cell[] {cell});
    assertThrows(IllegalArgumentException.class,
        () -> HBaseSystemRecordStore.decode(result, new byte[] {'s'}));
    verify(result, never()).getValue(any(), any());
  }

  @Test
  void acceptsExpectedSingleCell() {
    byte[] family = {'s'};
    var cell = new KeyValue(new byte[] {'k'}, family, new byte[] {'r'}, new byte[] {1, 2});
    assertArrayEquals(new byte[] {1, 2},
        HBaseSystemRecordStore.decode(Result.create(List.of(cell)), family).orElseThrow().bytes());
  }
}
