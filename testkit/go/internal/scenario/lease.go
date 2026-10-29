package scenario

import (
	"context"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/status"
	"io"
	"time"
)

func (runner *runner) leases(ctx context.Context, kv etcdserverpb.KVClient) error {
	lease := etcdserverpb.NewLeaseClient(runner.client.ActiveConnection())
	leaseGrantReq := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	leaseGrantResp, err := runner.call(ctx, "C15", "etcdserverpb.Lease/LeaseGrant", leaseGrantReq, func(requestCtx context.Context) (protoMessage, error) {
		return lease.LeaseGrant(requestCtx, leaseGrantReq)
	})
	if err != nil {
		return err
	}
	grant, ok := leaseGrantResp.(*etcdserverpb.LeaseGrantResponse)
	if !ok || grant.ID == 0 || grant.TTL != leaseGrantReq.TTL {
		return errors.New("native LeaseGrant returned an invalid ID or TTL")
	}
	leasedKey := []byte("m0/lease/value")
	leasedPut := &etcdserverpb.PutRequest{Key: leasedKey, Value: []byte("leased"), Lease: grant.ID}
	if _, err := runner.call(ctx, "C16", "etcdserverpb.KV/Put", leasedPut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, leasedPut)
	}); err != nil {
		return err
	}
	if err := runner.keepAlive(ctx, lease, grant.ID); err != nil {
		return err
	}
	revokeReq := &etcdserverpb.LeaseRevokeRequest{ID: grant.ID}
	if _, err := runner.call(ctx, "C18", "etcdserverpb.Lease/LeaseRevoke", revokeReq, func(requestCtx context.Context) (protoMessage, error) {
		return lease.LeaseRevoke(requestCtx, revokeReq)
	}); err != nil {
		return err
	}
	leasedRange := &etcdserverpb.RangeRequest{Key: leasedKey}
	leasedResp, err := runner.call(ctx, "C18", "etcdserverpb.KV/Range", leasedRange, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, leasedRange)
	})
	if err != nil {
		return err
	}
	if response, ok := leasedResp.(*etcdserverpb.RangeResponse); !ok || len(response.Kvs) != 0 {
		return errors.New("native LeaseRevoke did not remove the attached key")
	}

	return nil
}

func (runner *runner) keepAlive(ctx context.Context, lease etcdserverpb.LeaseClient, id int64) error {
	streamCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	stream, err := lease.LeaseKeepAlive(streamCtx)
	if err != nil {
		return fmt.Errorf("open native LeaseKeepAlive stream: %w", err)
	}
	request := &etcdserverpb.LeaseKeepAliveRequest{ID: id}
	keepAliveStarted := time.Now()
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send native LeaseKeepAlive request: %w", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive native LeaseKeepAlive response: %w", err)
	}
	if response.ID != id || response.TTL <= 0 {
		return errors.New("native LeaseKeepAlive returned an invalid ID or TTL")
	}
	if err := runner.record("C17", "etcdserverpb.Lease/LeaseKeepAlive", request, response, nil, nil, time.Since(keepAliveStarted)); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close native LeaseKeepAlive request stream: %w", err)
	}
	terminalStarted := time.Now()
	terminalErr := error(nil)
	_, terminalErr = stream.Recv()
	if !errors.Is(terminalErr, io.EOF) {
		return fmt.Errorf("native LeaseKeepAlive stream did not end cleanly: %s", status.Code(terminalErr))
	}
	terminal := &trace.StreamTerminal{Kind: "eof"}
	return runner.record("C17", "etcdserverpb.Lease/LeaseKeepAlive", request, nil, nil, terminal, time.Since(terminalStarted))
}
