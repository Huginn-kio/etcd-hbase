package scenario

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const m4KeepAliveManagedTTLSeconds = int64(12)

// RunM4NativeLeaseKeepAlive runs the pinned J42 scenario against a fresh embedded etcd.
func RunM4NativeLeaseKeepAlive(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || output == nil || dataDir == "" {
		return errors.New("native M4 KeepAlive run requires context, data directory, and trace output")
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return fmt.Errorf("create isolated native data directory: %w", err)
	}
	server, err := startEmbeddedWithElectionTimeout(ctx, dataDir, m4NativeElectionTimeoutMs)
	if err != nil {
		return err
	}
	defer server.close()
	return RunM4LeaseKeepAlive(ctx, server.client, "native-etcd", prefix, output)
}

// RunM4LeaseKeepAlive covers raw bidi KeepAlive exchanges and the pinned Go TTL/3 client loop.
func RunM4LeaseKeepAlive(ctx context.Context, client *clientv3.Client, evidenceScope, prefix string,
	output io.Writer) error {
	if err := ValidateM4Lease(evidenceScope, prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("M4 KeepAlive scenario requires context, client, and trace output")
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: evidenceScope}
	lease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	kv := etcdserverpb.NewKVClient(client.ActiveConnection())

	baselineRequest := &etcdserverpb.LeaseLeasesRequest{}
	baselineResult, err := runner.call(ctx, "native-baseline", "etcdserverpb.Lease/LeaseLeases",
		baselineRequest, func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseLeases(requestContext, baselineRequest)
		})
	if err != nil {
		return err
	}
	baseline, ok := baselineResult.(*etcdserverpb.LeaseLeasesResponse)
	if !ok || baseline.Header == nil || baseline.Header.Revision < 1 || len(baseline.Leases) != 0 {
		return errors.New("M4 KeepAlive scenario requires a fresh empty table group")
	}

	leaseA, err := grantLease(ctx, runner, lease, "keepalive-grant-a",
		leaseIDForPrefix(prefix, "keepalive-a"), 30)
	if err != nil {
		return err
	}
	leaseB, err := grantLease(ctx, runner, lease, "keepalive-grant-b",
		leaseIDForPrefix(prefix, "keepalive-b"), 30)
	if err != nil {
		return err
	}
	managedA, err := grantLease(ctx, runner, lease, "keepalive-grant-managed-a",
		leaseIDForPrefix(prefix, "keepalive-managed-a"), m4KeepAliveManagedTTLSeconds)
	if err != nil {
		return err
	}
	managedB, err := grantLease(ctx, runner, lease, "keepalive-grant-managed-b",
		leaseIDForPrefix(prefix, "keepalive-managed-b"), m4KeepAliveManagedTTLSeconds)
	if err != nil {
		return err
	}
	if leaseA.ID == leaseB.ID || leaseA.ID == managedA.ID || leaseA.ID == managedB.ID ||
		leaseB.ID == managedA.ID || leaseB.ID == managedB.ID || managedA.ID == managedB.ID {
		return errors.New("KeepAlive scenario generated colliding Lease IDs")
	}

	probeKey := []byte(prefix + "revision-probe")
	beforeRequest := &etcdserverpb.RangeRequest{Key: probeKey}
	beforeResult, err := runner.call(ctx, "keepalive-range-before", "etcdserverpb.KV/Range",
		beforeRequest, func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, beforeRequest)
		})
	if err != nil {
		return err
	}
	before, ok := beforeResult.(*etcdserverpb.RangeResponse)
	if !ok || before.Header == nil || len(before.Kvs) != 0 {
		return errors.New("KeepAlive revision probe did not return an empty range with a header")
	}

	missingID := leaseIDForPrefix(prefix, "keepalive-missing")
	if missingID == leaseA.ID || missingID == leaseB.ID || missingID == managedA.ID || missingID == managedB.ID {
		return errors.New("KeepAlive scenario generated a missing ID that is already granted")
	}
	requests := []*etcdserverpb.LeaseKeepAliveRequest{
		{ID: leaseA.ID},
		{ID: leaseB.ID},
		{ID: missingID},
		{ID: leaseA.ID},
	}
	caseIDs := []string{
		"keepalive-stream-a", "keepalive-stream-b", "keepalive-stream-missing", "keepalive-stream-a-repeat",
	}
	streamContext, cancelStream := context.WithTimeout(ctx, 30*time.Second)
	stream, err := lease.LeaseKeepAlive(streamContext)
	if err != nil {
		cancelStream()
		return fmt.Errorf("open M4 KeepAlive stream: %w", err)
	}
	requestTimes := make([]time.Time, len(requests))
	for index, request := range requests {
		requestTimes[index] = time.Now()
		if err := stream.Send(request); err != nil {
			cancelStream()
			return fmt.Errorf("send M4 KeepAlive request %d: %w", index+1, err)
		}
	}
	responses := make([]*etcdserverpb.LeaseKeepAliveResponse, len(requests))
	for index, request := range requests {
		response, err := stream.Recv()
		if err != nil {
			cancelStream()
			return fmt.Errorf("receive M4 KeepAlive response %d: %w", index+1, err)
		}
		if response == nil || response.Header == nil || response.ID != request.ID ||
			response.Header.Revision != before.Header.Revision {
			cancelStream()
			return fmt.Errorf("KeepAlive response %d changed request order or KV revision", index+1)
		}
		if index == 2 && response.TTL != 0 {
			cancelStream()
			return errors.New("unknown KeepAlive ID did not return TTL=0")
		}
		if index != 2 && response.TTL <= 0 {
			cancelStream()
			return errors.New("active KeepAlive ID did not return a positive TTL")
		}
		responses[index] = response
	}
	if err := stream.CloseSend(); err != nil {
		cancelStream()
		return fmt.Errorf("half-close M4 KeepAlive stream: %w", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		cancelStream()
		return fmt.Errorf("M4 KeepAlive stream did not end cleanly: %v", err)
	}
	cancelStream()
	for index, request := range requests {
		var terminal = (*trace.StreamTerminal)(nil)
		if index == len(requests)-1 {
			terminal = &trace.StreamTerminal{Kind: "eof"}
		}
		if err := runner.record(caseIDs[index], "etcdserverpb.Lease/LeaseKeepAlive", request,
			responses[index], nil, terminal, time.Since(requestTimes[index])); err != nil {
			return err
		}
	}

	for _, observation := range []struct {
		caseID string
		id     int64
	}{{"keepalive-ttl-a", leaseA.ID}, {"keepalive-ttl-b", leaseB.ID}} {
		ttl, err := readLeaseTTL(ctx, runner, lease, observation.caseID, observation.id, true)
		if err != nil {
			return err
		}
		if ttl.TTL <= 0 || ttl.TTL > 30 || ttl.GrantedTTL != 30 || len(ttl.Keys) != 0 {
			return fmt.Errorf("KeepAlive did not preserve the active Lease state for ID %d", observation.id)
		}
	}

	if err := verifyManagedKeepAlive(ctx, runner, client, managedA.ID, managedB.ID,
		m4KeepAliveManagedTTLSeconds); err != nil {
		return err
	}

	afterRequest := &etcdserverpb.RangeRequest{Key: probeKey}
	afterResult, err := runner.call(ctx, "keepalive-range-after", "etcdserverpb.KV/Range",
		afterRequest, func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, afterRequest)
		})
	if err != nil {
		return err
	}
	after, ok := afterResult.(*etcdserverpb.RangeResponse)
	if !ok || after.Header == nil || len(after.Kvs) != 0 ||
		after.Header.Revision != before.Header.Revision {
		return errors.New("pure KeepAlive changed the KV revision or revision-probe contents")
	}
	return nil
}

type managedKeepAliveObservation struct {
	response *clientv3.LeaseKeepAliveResponse
	at       time.Time
}

func verifyManagedKeepAlive(ctx context.Context, runner *runner, client *clientv3.Client,
	leaseA, leaseB int64, grantedTTL int64) error {
	streamContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	started := time.Now()
	channelA, err := client.KeepAlive(streamContext, clientv3.LeaseID(leaseA))
	if err != nil {
		return fmt.Errorf("start Go KeepAlive client for first ID: %w", err)
	}
	channelB, err := client.KeepAlive(streamContext, clientv3.LeaseID(leaseB))
	if err != nil {
		return fmt.Errorf("start Go KeepAlive client for second ID: %w", err)
	}
	observationsA := observeManagedKeepAlive(streamContext, channelA)
	observationsB := observeManagedKeepAlive(streamContext, channelB)
	aFirst, err := receiveManagedKeepAlive(streamContext, observationsA)
	if err != nil {
		return fmt.Errorf("receive first Go KeepAlive response for ID %d: %w", leaseA, err)
	}
	bFirst, err := receiveManagedKeepAlive(streamContext, observationsB)
	if err != nil {
		return fmt.Errorf("receive first Go KeepAlive response for ID %d: %w", leaseB, err)
	}
	aSecond, err := receiveManagedKeepAlive(streamContext, observationsA)
	if err != nil {
		return fmt.Errorf("receive second Go KeepAlive response for ID %d: %w", leaseA, err)
	}
	bSecond, err := receiveManagedKeepAlive(streamContext, observationsB)
	if err != nil {
		return fmt.Errorf("receive second Go KeepAlive response for ID %d: %w", leaseB, err)
	}
	if err := validateManagedKeepAlive(leaseA, grantedTTL, aFirst, aSecond); err != nil {
		return err
	}
	if err := validateManagedKeepAlive(leaseB, grantedTTL, bFirst, bSecond); err != nil {
		return err
	}
	for _, item := range []struct {
		caseID string
		id     int64
		value  managedKeepAliveObservation
	}{{"keepalive-managed-a-first", leaseA, aFirst}, {"keepalive-managed-a-second", leaseA, aSecond},
		{"keepalive-managed-b-first", leaseB, bFirst}, {"keepalive-managed-b-second", leaseB, bSecond}} {
		request := &etcdserverpb.LeaseKeepAliveRequest{ID: item.id}
		response := &etcdserverpb.LeaseKeepAliveResponse{
			Header: item.value.response.ResponseHeader, ID: int64(item.value.response.ID),
			TTL: item.value.response.TTL,
		}
		if err := runner.record(item.caseID, "etcdserverpb.Lease/LeaseKeepAlive", request,
			response, nil, nil, item.value.at.Sub(started)); err != nil {
			return err
		}
	}
	return nil
}

func observeManagedKeepAlive(ctx context.Context,
	responses <-chan *clientv3.LeaseKeepAliveResponse) <-chan managedKeepAliveObservation {
	observed := make(chan managedKeepAliveObservation, 4)
	go func() {
		defer close(observed)
		for {
			select {
			case <-ctx.Done():
				return
			case response, ok := <-responses:
				if !ok {
					return
				}
				select {
				case observed <- managedKeepAliveObservation{response: response, at: time.Now()}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return observed
}

func receiveManagedKeepAlive(ctx context.Context,
	observations <-chan managedKeepAliveObservation) (managedKeepAliveObservation, error) {
	select {
	case observation, ok := <-observations:
		if !ok || observation.response == nil {
			return managedKeepAliveObservation{}, errors.New("Go KeepAlive response channel closed")
		}
		return observation, nil
	case <-ctx.Done():
		return managedKeepAliveObservation{}, ctx.Err()
	}
}

func validateManagedKeepAlive(leaseID, grantedTTL int64,
	first, second managedKeepAliveObservation) error {
	for _, observation := range []managedKeepAliveObservation{first, second} {
		if observation.response.ID != clientv3.LeaseID(leaseID) ||
			observation.response.TTL != grantedTTL || observation.response.ResponseHeader == nil {
			return fmt.Errorf("Go KeepAlive response for ID %d changed ID, TTL, or header", leaseID)
		}
	}
	interval := second.at.Sub(first.at)
	expected := time.Duration(grantedTTL) * time.Second / 3
	if interval < expected/2 || interval > expected*2 {
		return fmt.Errorf("Go KeepAlive interval for ID %d was %s, outside the TTL/3 window around %s",
			leaseID, interval, expected)
	}
	return nil
}
