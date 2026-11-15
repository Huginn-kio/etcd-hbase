package scenario

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const m4LeaseExpiryWait = 30 * time.Second

// RunM4NativeLeaseRevoke runs the pinned J43 scenario against a fresh embedded etcd.
func RunM4NativeLeaseRevoke(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || output == nil || dataDir == "" {
		return errors.New("native M4 Revoke run requires context, data directory, and trace output")
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return fmt.Errorf("create isolated native data directory: %w", err)
	}
	server, err := startEmbeddedWithElectionTimeout(ctx, dataDir, m4NativeElectionTimeoutMs)
	if err != nil {
		return err
	}
	defer server.close()
	return RunM4LeaseRevoke(ctx, server.client, "native-etcd", prefix, output)
}

// RunM4LeaseRevoke checks atomic explicit Revoke and durable automatic expiry.
func RunM4LeaseRevoke(ctx context.Context, client *clientv3.Client, evidenceScope, prefix string,
	output io.Writer) error {
	if err := ValidateM4Lease(evidenceScope, prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("M4 Lease Revoke scenario requires context, client, and trace output")
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: evidenceScope}
	lease := etcdserverpb.NewLeaseClient(client.ActiveConnection())
	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	key := func(name string) []byte { return []byte(prefix + name) }

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
		return errors.New("M4 Revoke scenario requires a fresh empty table group")
	}

	boundLeaseID := leaseIDForPrefix(prefix, "revoke-bound")
	if _, err := grantLease(ctx, runner, lease, "revoke-grant-bound", boundLeaseID, 60); err != nil {
		return err
	}
	boundKeys := []struct {
		key        []byte
		putCase    string
		beforeCase string
		afterCase  string
	}{
		{key: key("revoke/role/a"), putCase: "revoke-put-bound-key-a",
			beforeCase: "revoke-range-before-a", afterCase: "revoke-range-after-a"},
		{key: key("revoke/role/b"), putCase: "revoke-put-bound-key-b",
			beforeCase: "revoke-range-before-b", afterCase: "revoke-range-after-b"},
	}
	for _, bound := range boundKeys {
		put := &etcdserverpb.PutRequest{Key: bound.key, Value: []byte("registered"), Lease: boundLeaseID}
		putResult, err := runner.call(ctx, bound.putCase, "etcdserverpb.KV/Put", put,
			func(requestContext context.Context) (protoMessage, error) {
				return kv.Put(requestContext, put)
			})
		if err != nil {
			return err
		}
		putResponse, ok := putResult.(*etcdserverpb.PutResponse)
		if !ok || putResponse.Header == nil {
			return errors.New("bound-key Put response is missing its header")
		}
	}
	for _, bound := range boundKeys {
		if err := recordRange(ctx, runner, kv, bound.beforeCase, bound.key, true); err != nil {
			return err
		}
	}
	if _, err := revokeLease(ctx, runner, lease, "revoke-bound", boundLeaseID); err != nil {
		return err
	}
	for _, bound := range boundKeys {
		if err := recordRange(ctx, runner, kv, bound.afterCase, bound.key, false); err != nil {
			return err
		}
	}
	if _, err := readLeaseTTL(ctx, runner, lease, "revoke-ttl-after", boundLeaseID, true); err != nil {
		return err
	}
	if _, err := revokeLeaseWithError(ctx, runner, lease, "revoke-missing", boundLeaseID); status.Code(err) != codes.NotFound {
		return fmt.Errorf("Revoke of an unknown Lease returned grpc_code=%s, expected NotFound", status.Code(err))
	}

	emptyLeaseID := leaseIDForPrefix(prefix, "revoke-empty")
	if emptyLeaseID == boundLeaseID {
		return errors.New("deterministic J43 Lease IDs collided")
	}
	if _, err := grantLease(ctx, runner, lease, "revoke-grant-empty", emptyLeaseID, 60); err != nil {
		return err
	}
	if _, err := revokeLease(ctx, runner, lease, "revoke-empty", emptyLeaseID); err != nil {
		return err
	}
	if _, err := readLeaseTTL(ctx, runner, lease, "revoke-empty-ttl", emptyLeaseID, true); err != nil {
		return err
	}

	expiringLeaseID := leaseIDForPrefix(prefix, "expiry-bound")
	if expiringLeaseID == boundLeaseID || expiringLeaseID == emptyLeaseID {
		return errors.New("deterministic J43 expiry Lease ID collided")
	}
	if _, err := grantLease(ctx, runner, lease, "expiry-grant", expiringLeaseID, 5); err != nil {
		return err
	}
	expiringKeys := []struct {
		key        []byte
		putCase    string
		beforeCase string
		afterCase  string
	}{
		{key: key("expiry/role/a"), putCase: "expiry-put-bound-key-a",
			beforeCase: "expiry-range-before-a", afterCase: "expiry-range-after-a"},
		{key: key("expiry/role/b"), putCase: "expiry-put-bound-key-b",
			beforeCase: "expiry-range-before-b", afterCase: "expiry-range-after-b"},
	}
	expiringKeyBytes := make([][]byte, 0, len(expiringKeys))
	for _, expiring := range expiringKeys {
		expiringPut := &etcdserverpb.PutRequest{
			Key: expiring.key, Value: []byte("ephemeral"), Lease: expiringLeaseID,
		}
		if _, err := runner.call(ctx, expiring.putCase, "etcdserverpb.KV/Put", expiringPut,
			func(requestContext context.Context) (protoMessage, error) {
				return kv.Put(requestContext, expiringPut)
			}); err != nil {
			return err
		}
		expiringKeyBytes = append(expiringKeyBytes, expiring.key)
	}
	for _, expiring := range expiringKeys {
		if err := recordRange(ctx, runner, kv, expiring.beforeCase, expiring.key, true); err != nil {
			return err
		}
	}
	if err := waitForLeaseKeysRemoval(ctx, kv, expiringKeyBytes); err != nil {
		return err
	}
	if _, err := readLeaseTTL(ctx, runner, lease, "expiry-ttl-after", expiringLeaseID, true); err != nil {
		return err
	}
	for _, expiring := range expiringKeys {
		if err := recordRange(ctx, runner, kv, expiring.afterCase, expiring.key, false); err != nil {
			return err
		}
	}
	return nil
}

func revokeLease(ctx context.Context, runner *runner, lease etcdserverpb.LeaseClient,
	caseID string, id int64) (*etcdserverpb.LeaseRevokeResponse, error) {
	request := &etcdserverpb.LeaseRevokeRequest{ID: id}
	result, err := runner.call(ctx, caseID, "etcdserverpb.Lease/LeaseRevoke", request,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseRevoke(requestContext, request)
		})
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.LeaseRevokeResponse)
	if !ok || response.Header == nil {
		return nil, errors.New("LeaseRevoke response is missing its header")
	}
	return response, nil
}

func revokeLeaseWithError(ctx context.Context, runner *runner, lease etcdserverpb.LeaseClient,
	caseID string, id int64) (*etcdserverpb.LeaseRevokeResponse, error) {
	request := &etcdserverpb.LeaseRevokeRequest{ID: id}
	result, err := runner.callWithError(ctx, caseID, "etcdserverpb.Lease/LeaseRevoke", request,
		func(requestContext context.Context) (protoMessage, error) {
			return lease.LeaseRevoke(requestContext, request)
		})
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.LeaseRevokeResponse)
	if !ok || response.Header == nil {
		return nil, errors.New("LeaseRevoke response is missing its header")
	}
	return response, nil
}

func recordRange(ctx context.Context, runner *runner, kv etcdserverpb.KVClient,
	caseID string, key []byte, present bool) error {
	request := &etcdserverpb.RangeRequest{Key: key}
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/Range", request,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, request)
		})
	if err != nil {
		return err
	}
	response, ok := result.(*etcdserverpb.RangeResponse)
	if !ok || response.Header == nil || (len(response.Kvs) == 1) != present ||
		len(response.Kvs) > 1 {
		return fmt.Errorf("%s did not return the expected key presence", caseID)
	}
	if present && (!stringEqual(response.Kvs[0].Key, key) || response.Kvs[0].Lease == 0) {
		return fmt.Errorf("%s returned a key outside the expected Lease binding", caseID)
	}
	return nil
}

func waitForLeaseKeysRemoval(ctx context.Context, kv etcdserverpb.KVClient, keys [][]byte) error {
	deadline := time.NewTimer(m4LeaseExpiryWait)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	requests := make([]*etcdserverpb.RangeRequest, len(keys))
	for index, key := range keys {
		requests[index] = &etcdserverpb.RangeRequest{Key: key}
	}
	for {
		allAbsent := true
		for index, request := range requests {
			response, err := kv.Range(ctx, request)
			if err != nil {
				return fmt.Errorf("poll Lease expiry key: %w", err)
			}
			if response == nil || response.Header == nil {
				return errors.New("Lease expiry poll omitted its range header")
			}
			if len(response.Kvs) == 0 {
				continue
			}
			if len(response.Kvs) != 1 || !stringEqual(response.Kvs[0].Key, keys[index]) {
				return errors.New("Lease expiry poll returned an unexpected key set")
			}
			allAbsent = false
		}
		if allAbsent {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Lease expiry: %w", ctx.Err())
		case <-deadline.C:
			return errors.New("Lease-bound key was not removed before the expiry deadline")
		case <-ticker.C:
		}
	}
}

func stringEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
