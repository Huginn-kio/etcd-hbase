package io.etcdhbase.grpc.service;

import etcdserverpb.Rpc;
import etcdserverpb.WatchGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.Function;

/** Watch RPC surface whose stream handler is installed by a capable table-group runtime. */
public final class WatchService extends WatchGrpc.WatchImplBase {
  private final Function<StreamObserver<Rpc.WatchResponse>, StreamObserver<Rpc.WatchRequest>> watch;

  /** Protocol-only mode retains an explicit UNIMPLEMENTED response. */
  public WatchService() {
    watch = null;
  }

  public WatchService(
      Function<StreamObserver<Rpc.WatchResponse>, StreamObserver<Rpc.WatchRequest>> watch) {
    this.watch = Objects.requireNonNull(watch, "watch");
  }

  @Override
  public StreamObserver<Rpc.WatchRequest> watch(
      StreamObserver<Rpc.WatchResponse> responseObserver) {
    Objects.requireNonNull(responseObserver, "responseObserver");
    return watch == null
        ? UnimplementedStreams.failAfterRequest(responseObserver, "Watch.Watch")
        : Objects.requireNonNull(watch.apply(responseObserver), "watch stream handler");
  }
}
