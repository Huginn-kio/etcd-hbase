package io.etcdhbase.grpc.service;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;

/** KV service surface with explicitly installed handlers and unimplemented remaining methods. */
public final class KvService extends KVGrpc.KVImplBase {
  private final BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler;
  private final BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler;

  /** Keeps the protocol-only skeleton explicitly unimplemented. */
  public KvService() {
    this.putHandler = null;
    this.txnHandler = null;
  }

  /** Installs a fully assembled Put handler; other KV methods remain unimplemented. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = null;
  }

  /** Installs both fully assembled handlers. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler,
      BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = Objects.requireNonNull(txnHandler, "txnHandler");
  }

  @Override
  public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Range");
  }

  @Override
  public void put(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> responseObserver) {
    if (putHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Put");
    else putHandler.accept(request, responseObserver);
  }

  @Override
  public void deleteRange(
      Rpc.DeleteRangeRequest request, StreamObserver<Rpc.DeleteRangeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.DeleteRange");
  }

  @Override
  public void txn(Rpc.TxnRequest request, StreamObserver<Rpc.TxnResponse> responseObserver) {
    if (txnHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Txn");
    else txnHandler.accept(request, responseObserver);
  }

  @Override
  public void compact(
      Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Compact");
  }
}
