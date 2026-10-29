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

type fixtureErrorProbe struct {
	caseID   string
	request  *etcdserverpb.RangeRequest
	timeout  time.Duration
	invoke   func(context.Context, *etcdserverpb.RangeRequest) error
	validate func(error) error
	mismatch string
}

func probeFixtureErrors(ctx context.Context, options Options, connection *clientv3.Client, output io.Writer) error {
	for _, probe := range fixtureErrorProbes(connection, options.Timeout) {
		if err := runFixtureErrorProbe(ctx, output, probe); err != nil {
			return err
		}
	}
	return nil
}

func fixtureErrorProbes(connection *clientv3.Client, timeout time.Duration) []fixtureErrorProbe {
	requestFor := func(marker byte) *etcdserverpb.RangeRequest {
		return &etcdserverpb.RangeRequest{Key: probeKeys[marker], Revision: protocolRevision}
	}
	clientRange := func(ctx context.Context, request *etcdserverpb.RangeRequest) error {
		_, err := connection.KV.Get(ctx, string(request.Key), clientv3.WithRev(protocolRevision))
		return clientv3.ContextError(ctx, err)
	}
	mappedRawRange := func(ctx context.Context, request *etcdserverpb.RangeRequest) error {
		_, err := client.RawKV(connection).Range(ctx, request)
		return rpctypes.Error(err)
	}
	rawDeadlineRange := func(ctx context.Context, request *etcdserverpb.RangeRequest) error {
		_, err := client.RawKV(connection).Range(ctx, request)
		return clientv3.ContextError(ctx, err)
	}
	return []fixtureErrorProbe{
		{
			caseID: "wire-fixture-clientv3-auth", request: requestFor('a'), timeout: timeout, invoke: clientRange,
			validate: func(err error) error { return expectError(err, codes.InvalidArgument, rpctypes.ErrAuthFailed) },
			mismatch: "clientv3 authentication sentinel mismatch",
		},
		{
			caseID: "wire-fixture-raw-permission", request: requestFor('p'), timeout: timeout, invoke: mappedRawRange,
			validate: func(err error) error { return expectError(err, codes.PermissionDenied, rpctypes.ErrPermissionDenied) },
			mismatch: "raw apiv3 permission sentinel mismatch",
		},
		{
			caseID: "wire-fixture-raw-unavailable", request: requestFor('u'), timeout: timeout, invoke: mappedRawRange,
			validate: func(err error) error { return expectError(err, codes.Unavailable, nil) },
			mismatch: "raw apiv3 unavailable mapping mismatch",
		},
		{
			caseID: "wire-fixture-clientv3-retry-deadline", request: requestFor('r'), timeout: min(timeout, 300*time.Millisecond),
			invoke: clientRange, validate: expectDeadline, mismatch: "clientv3 retry did not stop at its deadline",
		},
		{
			caseID: "wire-fixture-raw-deadline-cancel", request: requestFor('d'), timeout: min(timeout, 300*time.Millisecond),
			invoke: rawDeadlineRange, validate: expectDeadline, mismatch: "raw apiv3 deadline did not cancel the request",
		},
	}
}

func runFixtureErrorProbe(ctx context.Context, output io.Writer, probe fixtureErrorProbe) error {
	rpcContext, cancel := context.WithTimeout(ctx, probe.timeout)
	started := time.Now()
	// Each invocation retains its raw/clientv3 mapping while the context is live.
	callErr := probe.invoke(rpcContext, probe.request)
	cancel()
	if err := writeUnaryRecord(output, probe.caseID, "transport-fixture", probe.request, callErr, time.Since(started)); err != nil {
		return err
	}
	if err := probe.validate(callErr); err != nil {
		return fmt.Errorf("%s: %w", probe.mismatch, err)
	}
	return nil
}
