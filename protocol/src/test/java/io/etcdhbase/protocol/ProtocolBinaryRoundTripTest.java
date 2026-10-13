package io.etcdhbase.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import mvccpb.Kv;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolBinaryRoundTripTest {
  private static final byte[] KEY = {0, 1, (byte) 0xff, 0, 42};
  private static final long LARGE_REVISION = 9_007_199_254_740_993L;

  @Test
  void preservesBinaryKeysLargeInt64AndRangeResponseMetadata() throws Exception {
    Rpc.RangeRequest request = Rpc.RangeRequest.newBuilder()
        .setKey(ByteString.copyFrom(KEY))
        .setRangeEnd(ByteString.copyFrom(new byte[] {0, (byte) 0xff}))
        .setRevision(LARGE_REVISION)
        .setLimit(Long.MAX_VALUE)
        .setMinModRevision(LARGE_REVISION)
        .setMaxCreateRevision(Long.MIN_VALUE + 3)
        .build();
    assertEquals(request, Rpc.RangeRequest.parseFrom(request.toByteArray()));

    Kv.KeyValue kv = Kv.KeyValue.newBuilder()
        .setKey(ByteString.copyFrom(KEY))
        .setValue(ByteString.copyFrom(new byte[] {(byte) 0xff, 0, 7}))
        .setCreateRevision(LARGE_REVISION)
        .setModRevision(LARGE_REVISION + 1)
        .setVersion(2)
        .setLease(Long.MIN_VALUE + 1)
        .build();
    Rpc.RangeResponse response = Rpc.RangeResponse.newBuilder()
        .setHeader(Rpc.ResponseHeader.newBuilder().setRevision(LARGE_REVISION))
        .addKvs(kv)
        .setMore(true)
        .setCount(LARGE_REVISION)
        .build();
    assertEquals(response, Rpc.RangeResponse.parseFrom(response.toByteArray()));
  }

  @Test
  void preservesTxnNestedOneofsAndPrevKvBytes() throws Exception {
    Rpc.Compare compare = Rpc.Compare.newBuilder()
        .setKey(ByteString.copyFrom(KEY))
        .setTarget(Rpc.Compare.CompareTarget.VALUE)
        .setValue(ByteString.copyFrom(new byte[] {0, (byte) 0xff}))
        .build();
    Rpc.RequestOp put = Rpc.RequestOp.newBuilder()
        .setRequestPut(Rpc.PutRequest.newBuilder()
            .setKey(ByteString.copyFrom(KEY))
            .setValue(ByteString.copyFrom(new byte[] {(byte) 0xff, 0, 9}))
            .setLease(LARGE_REVISION)
            .setPrevKv(true))
        .build();
    Rpc.RequestOp nestedTransaction = Rpc.RequestOp.newBuilder()
        .setRequestTxn(Rpc.TxnRequest.newBuilder()
            .addCompare(compare)
            .addSuccess(put))
        .build();
    Rpc.TxnRequest txn = Rpc.TxnRequest.newBuilder()
        .addCompare(compare)
        .addSuccess(put)
        .addSuccess(nestedTransaction)
        .addFailure(Rpc.RequestOp.newBuilder().setRequestRange(
            Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFrom(KEY))))
        .build();
    Rpc.TxnRequest decodedTxn = Rpc.TxnRequest.parseFrom(txn.toByteArray());
    assertEquals(txn, decodedTxn);
    assertEquals(Rpc.Compare.TargetUnionCase.VALUE, decodedTxn.getCompare(0).getTargetUnionCase());
    assertEquals(Rpc.RequestOp.RequestCase.REQUEST_PUT,
        decodedTxn.getSuccess(0).getRequestCase());
    assertEquals(Rpc.RequestOp.RequestCase.REQUEST_RANGE,
        decodedTxn.getFailure(0).getRequestCase());
    Rpc.RequestOp decodedNestedTransaction = decodedTxn.getSuccess(1);
    assertEquals(Rpc.RequestOp.RequestCase.REQUEST_TXN, decodedNestedTransaction.getRequestCase());
    assertEquals(Rpc.RequestOp.RequestCase.REQUEST_PUT,
        decodedNestedTransaction.getRequestTxn().getSuccess(0).getRequestCase());

    Kv.KeyValue previous = Kv.KeyValue.newBuilder()
        .setKey(ByteString.copyFrom(KEY))
        .setValue(ByteString.copyFrom(new byte[] {(byte) 0xff, 0, 9}))
        .setCreateRevision(LARGE_REVISION)
        .setModRevision(LARGE_REVISION + 1)
        .setVersion(1)
        .build();
    Kv.Event event = Kv.Event.newBuilder()
        .setType(Kv.Event.EventType.DELETE)
        .setKv(previous.toBuilder().setValue(ByteString.EMPTY).setVersion(0))
        .setPrevKv(previous)
        .build();
    assertEquals(event, Kv.Event.parseFrom(event.toByteArray()));
  }

  @Test
  void preservesWatchRequestOneofsAndStreamRevisionFields() throws Exception {
    Rpc.WatchCreateRequest create = Rpc.WatchCreateRequest.newBuilder()
        .setKey(ByteString.copyFrom(KEY))
        .setRangeEnd(ByteString.copyFrom(new byte[] {0, (byte) 0xff}))
        .setStartRevision(LARGE_REVISION)
        .setProgressNotify(true)
        .addFilters(Rpc.WatchCreateRequest.FilterType.NODELETE)
        .setPrevKv(true)
        .setWatchId(LARGE_REVISION + 1)
        .setFragment(true)
        .build();
    Rpc.WatchRequest watch = Rpc.WatchRequest.newBuilder().setCreateRequest(create).build();
    Rpc.WatchRequest decoded = Rpc.WatchRequest.parseFrom(watch.toByteArray());
    assertEquals(watch, decoded);
    assertEquals(Rpc.WatchRequest.RequestUnionCase.CREATE_REQUEST,
        decoded.getRequestUnionCase());

    Rpc.WatchRequest cancel = Rpc.WatchRequest.newBuilder()
        .setCancelRequest(Rpc.WatchCancelRequest.newBuilder().setWatchId(LARGE_REVISION))
        .build();
    Rpc.WatchRequest progress = Rpc.WatchRequest.newBuilder()
        .setProgressRequest(Rpc.WatchProgressRequest.getDefaultInstance())
        .build();
    assertEquals(cancel, Rpc.WatchRequest.parseFrom(cancel.toByteArray()));
    assertEquals(progress, Rpc.WatchRequest.parseFrom(progress.toByteArray()));

    Rpc.LeaseKeepAliveRequest keepAlive = Rpc.LeaseKeepAliveRequest.newBuilder()
        .setID(LARGE_REVISION)
        .build();
    assertEquals(keepAlive, Rpc.LeaseKeepAliveRequest.parseFrom(keepAlive.toByteArray()));
  }

  @Test
  void preservesAuthenticateRequestAndResponseFields() throws Exception {
    Rpc.AuthenticateRequest request = Rpc.AuthenticateRequest.newBuilder()
        .setName("fixture-user")
        .setPassword("fixture-password-value")
        .build();
    assertEquals(request, Rpc.AuthenticateRequest.parseFrom(request.toByteArray()));

    Rpc.AuthenticateResponse response = Rpc.AuthenticateResponse.newBuilder()
        .setHeader(Rpc.ResponseHeader.newBuilder().setRevision(LARGE_REVISION))
        .setToken("fixture-token-value")
        .build();
    assertEquals(response, Rpc.AuthenticateResponse.parseFrom(response.toByteArray()));
  }
}
