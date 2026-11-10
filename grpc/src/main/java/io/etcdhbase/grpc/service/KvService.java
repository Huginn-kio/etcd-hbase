package io.etcdhbase.grpc.service;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;

/** M0 KV service surface; operations remain explicitly unimplemented. */
public final class KvService extends KVGrpc.KVImplBase {
  private final BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler;

  /** Keeps the protocol-only skeleton explicitly unimplemented. */
  public KvService() {
    this.putHandler = null;
  }

  /** Installs a fully assembled Put handler; other KV methods remain unimplemented. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
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
    UnimplementedStreams.failUnary(responseObserver, "KV.Txn");
  }

  @Override
  public void compact(
      Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Compact");
  }
}
