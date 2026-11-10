package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.RangeCommand;
import org.junit.jupiter.api.Test;

class RangeRequestMapperTest {
  @Test
  void decodesPointFromKeyFullSpaceAndEmptyIntervalsAsBytes() {
    var point = request(bytes(0xff, 0), ByteString.EMPTY);
    var from = request(bytes(0xff), bytes(0));
    var all = request(bytes(0), bytes(0));
    var empty = request(bytes('z'), bytes('a'));

    var exact = RangeRequestMapper.fromWire(point, RangeCommand.Context.STANDALONE).range();
    assertEquals(KeyRange.Kind.POINT, exact.kind());
    assertTrue(exact.contains(key(0xff, 0)));
    assertFalse(exact.contains(key(0xff)));

    var unbounded = RangeRequestMapper.fromWire(from, RangeCommand.Context.STANDALONE).range();
    assertTrue(unbounded.contains(key(0xff, 0)));
    assertFalse(unbounded.contains(key(0xfe)));
    var full = RangeRequestMapper.fromWire(all, RangeCommand.Context.TRANSACTION).range();
    assertTrue(full.contains(key(0)));
    assertTrue(full.contains(key(0xff, 0)));
    assertTrue(RangeRequestMapper.fromWire(empty, RangeCommand.Context.STANDALONE).range().isEmpty());
  }

  @Test
  void preservesRequestContextAndAllOptionValuesWithoutNarrowing() {
    var request = Rpc.RangeRequest.newBuilder().setKey(bytes('k')).setRangeEnd(bytes('z'))
        .setRevision(Long.MIN_VALUE).setLimit(Long.MAX_VALUE)
        .setMinModRevision(-1).setMaxModRevision(Long.MAX_VALUE)
        .setMinCreateRevision(Long.MIN_VALUE).setMaxCreateRevision(42)
        .setSortOrderValue(99).setSortTargetValue(98)
        .setSerializable(true).setKeysOnly(true).setCountOnly(true).build();
    var command = RangeRequestMapper.fromWire(request, RangeCommand.Context.TRANSACTION);
    assertEquals(RangeCommand.Context.TRANSACTION, command.context());
    assertEquals(Long.MIN_VALUE, command.revision());
    assertEquals(Long.MAX_VALUE, command.limit());
    assertEquals(-1, command.filter().minModRevision());
    assertEquals(Long.MAX_VALUE, command.filter().maxModRevision());
    assertEquals(Long.MIN_VALUE, command.filter().minCreateRevision());
    assertEquals(42, command.filter().maxCreateRevision());
    assertEquals(99, command.sort().order());
    assertEquals(98, command.sort().target());
    assertTrue(command.serializable());
    assertTrue(command.keysOnly());
    assertTrue(command.countOnly());
  }

  @Test
  void rejectsAnEmptyKeyBeforeConstructingAnyRange() {
    var failure = assertThrows(KvFailure.class, () -> RangeRequestMapper.fromWire(
        Rpc.RangeRequest.getDefaultInstance(), RangeCommand.Context.STANDALONE));
    assertEquals(KvFailure.Reason.EMPTY_KEY, failure.reason());
  }

  private static Rpc.RangeRequest request(ByteString key, ByteString end) {
    return Rpc.RangeRequest.newBuilder().setKey(key).setRangeEnd(end).build();
  }

  private static ByteKey key(int... values) { return ByteKey.copyOf(raw(values)); }
  private static ByteString bytes(int... values) { return ByteString.copyFrom(raw(values)); }
  private static byte[] raw(int... values) {
    var result = new byte[values.length];
    for (int i = 0; i < values.length; i++) result[i] = (byte) values[i];
    return result;
  }
}
