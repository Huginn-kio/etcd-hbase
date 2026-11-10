package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.kv.CompareEvaluator;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.TxnCommand;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TxnRequestMapperTest {
  @Test
  void preservesBinaryCompareRangeAndNativeUnionDefaults() {
    var compare = Rpc.Compare.newBuilder().setKey(binary(0)).setRangeEnd(binary(0))
        .setTarget(Rpc.Compare.CompareTarget.VERSION).setValue(binary(0xff))
        .setResult(Rpc.Compare.CompareResult.EQUAL).build();
    var mapped = TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder().addCompare(compare).build());
    var condition = mapped.compares().getFirst();
    assertTrue(condition.range().contains(ByteKey.copyOf(new byte[] {(byte) 0xff, 0})));
    assertTrue(new CompareEvaluator(condition).finish(), "a mismatched version union defaults to zero");

    var unknown = Rpc.Compare.newBuilder().setKey(binary(0xff)).setTargetValue(99)
        .setResultValue(99).setLease(Long.MIN_VALUE).build();
    var nativeDefault = TxnRequestMapper.fromWire(
        Rpc.TxnRequest.newBuilder().addCompare(unknown).build()).compares().getFirst();
    assertTrue(new CompareEvaluator(nativeDefault).finish());
  }

  @Test
  void preservesPotentialWritesInUnselectedBranches() {
    var complexRange = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder()
        .setKey(binary('a')).setRangeEnd(binary('z'))).build();
    var delete = Rpc.RequestOp.newBuilder().setRequestDeleteRange(
        Rpc.DeleteRangeRequest.newBuilder().setKey(binary('x'))).build();
    var mapped = TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder()
        .addSuccess(complexRange).addFailure(delete).build());
    var range = assertInstanceOf(TxnCommand.Range.class, mapped.success().getFirst());
    assertTrue(range.command().range().contains(ByteKey.copyOf(new byte[] {'b'})));
    assertInstanceOf(TxnCommand.Delete.class, mapped.failure().getFirst());
    assertTrue(mapped.mayWrite());
    assertTrue(TxnRequestMapper.requiresNativeRaftLimit(Rpc.TxnRequest.newBuilder()
        .addSuccess(complexRange).addFailure(delete).build()));
    assertFalse(TxnRequestMapper.requiresNativeRaftLimit(Rpc.TxnRequest.newBuilder()
        .addSuccess(complexRange).build()));
  }

  @Test
  void mapsDeletePointFullSpaceAndEmptyRangesWithoutLosingPreviousValueFlag() {
    var point = Rpc.RequestOp.newBuilder().setRequestDeleteRange(
        Rpc.DeleteRangeRequest.newBuilder().setKey(binary(0xff)).setPrevKv(true)).build();
    var all = delete(0, 0);
    var empty = delete('z', 'a');
    var command = TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder()
        .addSuccess(point).addSuccess(all).addSuccess(empty).build());
    var exact = assertInstanceOf(TxnCommand.Delete.class, command.success().get(0));
    assertTrue(exact.previousRequested());
    assertTrue(exact.range().contains(ByteKey.copyOf(new byte[] {(byte) 0xff})));
    assertFalse(exact.range().contains(ByteKey.copyOf(new byte[] {(byte) 0xff, 0})));
    var full = assertInstanceOf(TxnCommand.Delete.class, command.success().get(1));
    assertTrue(full.range().contains(ByteKey.copyOf(new byte[] {(byte) 0xff, 0})));
    assertTrue(assertInstanceOf(TxnCommand.Delete.class, command.success().get(2)).range().isEmpty());
  }

  @Test
  void validatesBothBranchesAndNestedPotentialWritesBeforeChoosingAPath() {
    var invalidRange = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.getDefaultInstance()).build();
    assertEquals(KvFailure.Reason.EMPTY_KEY, assertThrows(KvFailure.class, () ->
        TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder().addFailure(invalidRange).build())).reason());
    var invalidOp = Rpc.RequestOp.getDefaultInstance();
    assertEquals(KvFailure.Reason.KEY_NOT_FOUND, assertThrows(KvFailure.class, () ->
        TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder().addFailure(invalidOp).build())).reason());

    var put = put('a');
    var nested = Rpc.TxnRequest.newBuilder().addSuccess(put).addFailure(put).build();
    var nestedOp = Rpc.RequestOp.newBuilder().setRequestTxn(nested).build();
    var allowed = TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder().addSuccess(nestedOp).build());
    var mappedNested = assertInstanceOf(TxnCommand.Nested.class, allowed.success().getFirst());
    assertTrue(mappedNested.command().mayWrite());
    var duplicate = Rpc.TxnRequest.newBuilder().addSuccess(nestedOp).addSuccess(put).build();
    assertEquals(KvFailure.Reason.DUPLICATE_KEY,
        assertThrows(KvFailure.class, () -> TxnRequestMapper.fromWire(duplicate)).reason());

    var tooMany = Rpc.TxnRequest.newBuilder().addAllSuccess(Collections.nCopies(129,
        Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder().setKey(binary('a'))).build()));
    assertEquals(KvFailure.Reason.TOO_MANY_OPS,
        assertThrows(KvFailure.class, () -> TxnRequestMapper.fromWire(tooMany.build())).reason());
    Rpc.TxnRequest deep = Rpc.TxnRequest.getDefaultInstance();
    for (int depth = 0; depth < 17; depth++) {
      deep = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestTxn(deep)).build();
    }
    Rpc.TxnRequest oversizedTree = deep;
    assertEquals(KvFailure.Reason.REQUEST_BUDGET_EXHAUSTED,
        assertThrows(KvFailure.class, () -> TxnRequestMapper.fromWire(oversizedTree)).reason());
  }

  @Test
  void matchesNativeNestedPotentialWriteIntervalsEvenInUnselectedBranches() {
    var childPuts = nested(List.of(put('a')), List.of(put('a')));
    var exclusivePutDelete = nested(List.of(put('a')), List.of(delete('a', -1)));
    assertAccepted(childPuts);
    assertAccepted(exclusivePutDelete);
    assertDuplicate(nested(List.of(put('a')), List.of()), nested(List.of(put('a')), List.of()));
    var childPut = nested(List.of(put('a')), List.of());
    assertDuplicate(delete('a', 'z'), childPut);
    assertDuplicate(childPut, delete('a', 'z'));
    assertDuplicate(nested(List.of(delete('a', 'z')), List.of()),
        nested(List.of(put('m')), List.of()));
    assertAccepted(delete('a', 'z'), put('z'));
    assertAccepted(delete('a', 'z'), delete('m', 0xff));
    assertAccepted(delete('z', 'a'), put('m'));
    assertAccepted(delete('a', 0), put('z'));
    assertDuplicate(delete(0, 0xff), put(0));
    assertAccepted(delete(0, 0xff), put(0xff));
  }

  @Test
  void depthBudgetIncludesLeafResponseBeforeAnyTransactionStarts() {
    var leaf = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
        .setRequestRange(Rpc.RangeRequest.newBuilder().setKey(binary('a')))).build();
    Rpc.TxnRequest safe = leaf;
    for (int index = 0; index < 14; index++) {
      safe = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestTxn(safe)).build();
    }
    TxnRequestMapper.fromWire(safe);
    var tooDeep = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestTxn(safe)).build();
    assertEquals(KvFailure.Reason.REQUEST_BUDGET_EXHAUSTED,
        assertThrows(KvFailure.class, () -> TxnRequestMapper.fromWire(tooDeep)).reason());
  }

  @Test
  void usesNativeTxnEnvelopeFieldForRequestSize() {
    var request = Rpc.TxnRequest.newBuilder().addSuccess(put('a')).build();
    int size = TxnProtocolLimit.encodedSize(request, 123);
    TxnProtocolLimit.requireWithin(request, 123, size);
    assertEquals(KvFailure.Reason.REQUEST_TOO_LARGE,
        assertThrows(KvFailure.class, () -> TxnProtocolLimit.requireWithin(request, 123, size - 1)).reason());
  }

  @Test
  void matchesPinnedGoEtcdInternalTxnEnvelopeSizeAcrossVarintBoundaries() {
    // Produced by v3.5.23 InternalRaftRequest.Size() with the same Txn and request IDs.
    long[] ids = {1L, 1L << 48, Long.MIN_VALUE};
    int[][] vectors = {{0, 25, 31, 34}, {127, 157, 163, 166},
        {128, 159, 165, 168}, {16384, 16419, 16425, 16428}};
    for (int[] vector : vectors) {
      var put = Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("m3-probe-key"))
          .setValue(ByteString.copyFrom(new byte[vector[0]]));
      var request = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(put)).build();
      for (int index = 0; index < ids.length; index++) {
        assertEquals(vector[index + 1], TxnProtocolLimit.encodedSize(request, ids[index]));
      }
    }
  }

  private static Rpc.RequestOp put(int key) {
    return Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder().setKey(binary(key))).build();
  }
  private static Rpc.RequestOp delete(int start, int end) {
    var request = Rpc.DeleteRangeRequest.newBuilder().setKey(binary(start));
    if (end >= 0) request.setRangeEnd(binary(end));
    return Rpc.RequestOp.newBuilder().setRequestDeleteRange(request).build();
  }
  private static Rpc.RequestOp nested(List<Rpc.RequestOp> success, List<Rpc.RequestOp> failure) {
    return Rpc.RequestOp.newBuilder().setRequestTxn(Rpc.TxnRequest.newBuilder()
        .addAllSuccess(success).addAllFailure(failure)).build();
  }
  private static void assertAccepted(Rpc.RequestOp... operations) {
    TxnRequestMapper.fromWire(Rpc.TxnRequest.newBuilder().addAllFailure(List.of(operations)).build());
  }
  private static void assertDuplicate(Rpc.RequestOp... operations) {
    var request = Rpc.TxnRequest.newBuilder().addAllFailure(List.of(operations)).build();
    assertEquals(KvFailure.Reason.DUPLICATE_KEY,
        assertThrows(KvFailure.class, () -> TxnRequestMapper.fromWire(request)).reason());
  }
  private static ByteString binary(int value) { return ByteString.copyFrom(new byte[] {(byte) value}); }
}
