package wire

import (
	"context"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"io"
	"time"
)

func probeSkeletonWatch(ctx context.Context, options Options, connection *clientv3.Client, output io.Writer) error {
	watchRequest := &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: binaryKey, StartRevision: protocolRevision},
		},
	}
	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	watchStream, err := client.RawWatch(connection).Watch(rpcContext)
	if err == nil {
		err = watchStream.Send(watchRequest)
	}
	if err == nil {
		_, err = watchStream.Recv()
	}
	if watchStream != nil {
		_ = watchStream.CloseSend()
	}
	err = clientv3.ContextError(rpcContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-skeleton-watch",
		"transport",
		"etcdserverpb.Watch/Watch",
		watchRequest,
		err,
		grpcTerminal(err),
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if err := expectError(err, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("Watch.Watch did not terminate as UNIMPLEMENTED: %w", err)
	}

	return nil
}

func probeSkeletonLease(ctx context.Context, options Options, connection *clientv3.Client, output io.Writer) error {

	leaseRequest := &etcdserverpb.LeaseKeepAliveRequest{ID: protocolRevision}
	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	leaseStream, err := etcdserverpb.NewLeaseClient(connection.ActiveConnection()).LeaseKeepAlive(rpcContext)
	if err == nil {
		err = leaseStream.Send(leaseRequest)
	}
	if err == nil {
		_, err = leaseStream.Recv()
	}
	if leaseStream != nil {
		_ = leaseStream.CloseSend()
	}
	err = clientv3.ContextError(rpcContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-skeleton-lease-keepalive",
		"transport",
		"etcdserverpb.Lease/LeaseKeepAlive",
		leaseRequest,
		err,
		grpcTerminal(err),
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if err := expectError(err, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("Lease.LeaseKeepAlive did not terminate as UNIMPLEMENTED: %w", err)
	}

	return nil
}

func probeWatchDeadline(ctx context.Context, options Options, connection *clientv3.Client, output io.Writer) error {
	watchRequest := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: binaryKey, StartRevision: protocolRevision}}}

	cancelTimeout := min(options.Timeout, time.Second)
	cancelContext, cancel := context.WithTimeout(ctx, cancelTimeout)
	started := time.Now()
	cancelStream, err := client.RawWatch(connection).Watch(cancelContext)
	if err == nil {
		err = cancelStream.Send(watchRequest)
	}
	if err == nil {
		_, err = cancelStream.Recv()
	}
	if cancelStream != nil {
		_ = cancelStream.CloseSend()
	}
	err = clientv3.ContextError(cancelContext, err)
	cancel()
	if recordErr := writeStreamRecord(
		output,
		"wire-fixture-watch-cancel",
		"transport-fixture",
		"etcdserverpb.Watch/Watch",
		watchRequest,
		err,
		trace.StreamTerminal{Kind: "context-deadline"},
		time.Since(started),
	); recordErr != nil {
		return recordErr
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("Watch did not observe caller deadline: grpc_code=%s", errorCode(err))
	}
	return nil
}
