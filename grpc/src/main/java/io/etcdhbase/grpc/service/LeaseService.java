package io.etcdhbase.grpc.service;

import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Lease RPC surface whose unary handlers are installed by a capable table-group runtime. */
public final class LeaseService extends LeaseGrpc.LeaseImplBase {
  private final BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant;
  private final BiConsumer<Rpc.LeaseRevokeRequest, StreamObserver<Rpc.LeaseRevokeResponse>> revoke;
  private final BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl;
  private final BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list;
  private final Function<StreamObserver<Rpc.LeaseKeepAliveResponse>,
      StreamObserver<Rpc.LeaseKeepAliveRequest>> keepAlive;

  /** Protocol-only mode retains explicit UNIMPLEMENTED responses. */
  public LeaseService() {
    grant = null;
    revoke = null;
    ttl = null;
    list = null;
    keepAlive = null;
  }

  public LeaseService(
      BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant,
      BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl,
      BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list) {
    this(grant, ttl, list, null, null);
  }

  public LeaseService(
      BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant,
      BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl,
      BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list,
      Function<StreamObserver<Rpc.LeaseKeepAliveResponse>,
          StreamObserver<Rpc.LeaseKeepAliveRequest>> keepAlive) {
    this(grant, ttl, list, keepAlive, null);
  }

  public LeaseService(
      BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant,
      BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl,
      BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list,
      Function<StreamObserver<Rpc.LeaseKeepAliveResponse>,
          StreamObserver<Rpc.LeaseKeepAliveRequest>> keepAlive,
      BiConsumer<Rpc.LeaseRevokeRequest, StreamObserver<Rpc.LeaseRevokeResponse>> revoke) {
    this.grant = Objects.requireNonNull(grant, "grant");
    this.revoke = revoke;
    this.ttl = Objects.requireNonNull(ttl, "ttl");
    this.list = Objects.requireNonNull(list, "list");
    this.keepAlive = keepAlive;
  }

  @Override
  public void leaseGrant(
      Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> responseObserver) {
    if (grant == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseGrant");
    else grant.accept(request, responseObserver);
  }

  @Override
  public void leaseRevoke(
      Rpc.LeaseRevokeRequest request, StreamObserver<Rpc.LeaseRevokeResponse> responseObserver) {
    if (revoke == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseRevoke");
    else revoke.accept(request, responseObserver);
  }

  @Override
  public StreamObserver<Rpc.LeaseKeepAliveRequest> leaseKeepAlive(
      StreamObserver<Rpc.LeaseKeepAliveResponse> responseObserver) {
    if (keepAlive == null) {
      return UnimplementedStreams.failAfterRequest(responseObserver, "Lease.LeaseKeepAlive");
    }
    return keepAlive.apply(responseObserver);
  }

  @Override
  public void leaseTimeToLive(
      Rpc.LeaseTimeToLiveRequest request,
      StreamObserver<Rpc.LeaseTimeToLiveResponse> responseObserver) {
    if (ttl == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseTimeToLive");
    else ttl.accept(request, responseObserver);
  }

  @Override
  public void leaseLeases(
      Rpc.LeaseLeasesRequest request, StreamObserver<Rpc.LeaseLeasesResponse> responseObserver) {
    if (list == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseLeases");
    else list.accept(request, responseObserver);
  }
}
