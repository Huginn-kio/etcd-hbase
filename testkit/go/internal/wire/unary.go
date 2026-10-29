package wire

import (
	"context"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"io"
	"time"
)

func probeSkeletonUnary(
	ctx context.Context,
	options Options,
	connection *clientv3.Client,
	output io.Writer,
) error {
	request := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: protocolRevision}

	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	_, callErr := connection.KV.Get(rpcContext, string(binaryKey), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-skeleton-clientv3", "transport", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("clientv3 KV.Get did not reach the skeleton Range handler: %w", err)
	}

	rawRequest := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: protocolRevision}
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, rawRequest)
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-skeleton-raw-range", "transport", rawRequest, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unimplemented, nil); err != nil {
		return fmt.Errorf("raw apiv3 KV.Range did not reach the skeleton handler: %w", err)
	}
	return nil
}

func probeFixtureErrors(
	ctx context.Context,
	options Options,
	connection *clientv3.Client,
	output io.Writer,
) error {
	requestFor := func(marker byte) *etcdserverpb.RangeRequest {
		return &etcdserverpb.RangeRequest{Key: probeKeys[marker], Revision: protocolRevision}
	}

	request := requestFor('a')
	rpcContext, cancel := context.WithTimeout(ctx, options.Timeout)
	started := time.Now()
	_, callErr := connection.KV.Get(rpcContext, string(request.Key), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-clientv3-auth", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.InvalidArgument, rpctypes.ErrAuthFailed); err != nil {
		return fmt.Errorf("clientv3 authentication sentinel mismatch: %w", err)
	}

	request = requestFor('p')
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = rpctypes.Error(callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-permission", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.PermissionDenied, rpctypes.ErrPermissionDenied); err != nil {
		return fmt.Errorf("raw apiv3 permission sentinel mismatch: %w", err)
	}

	request = requestFor('u')
	rpcContext, cancel = context.WithTimeout(ctx, options.Timeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = rpctypes.Error(callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-unavailable", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectError(callErr, codes.Unavailable, nil); err != nil {
		return fmt.Errorf("raw apiv3 unavailable mapping mismatch: %w", err)
	}

	request = requestFor('r')
	boundedRetryTimeout := min(options.Timeout, 300*time.Millisecond)
	rpcContext, cancel = context.WithTimeout(ctx, boundedRetryTimeout)
	started = time.Now()
	_, callErr = connection.KV.Get(rpcContext, string(request.Key), clientv3.WithRev(protocolRevision))
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-clientv3-retry-deadline", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectDeadline(callErr); err != nil {
		return fmt.Errorf("clientv3 retry did not stop at its deadline: %w", err)
	}

	request = requestFor('d')
	requestTimeout := min(options.Timeout, 300*time.Millisecond)
	rpcContext, cancel = context.WithTimeout(ctx, requestTimeout)
	started = time.Now()
	_, callErr = client.RawKV(connection).Range(rpcContext, request)
	callErr = clientv3.ContextError(rpcContext, callErr)
	cancel()
	if err := writeUnaryRecord(output, "wire-fixture-raw-deadline-cancel", "transport-fixture", request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := expectDeadline(callErr); err != nil {
		return fmt.Errorf("raw apiv3 deadline did not cancel the request: %w", err)
	}
	return nil
}
