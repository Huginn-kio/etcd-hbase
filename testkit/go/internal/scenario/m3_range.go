package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// RunM3NativeRange runs the registered Range scenarios on a disposable native server.
func RunM3NativeRange(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native Range requires a context, new data directory, and output")
	}
	if err := ValidateM3Put("native-etcd", prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return errors.New("M3 native data directory could not be created")
	}
	server, err := startEmbedded(ctx, dataDir)
	if err != nil {
		return errors.New("M3 native server could not be started")
	}
	defer server.close()
	return RunM3Range(ctx, server.client, "native-etcd", prefix, output)
}

// RunM3Range uses only a fresh, isolated prefix and records the real wire requests.
func RunM3Range(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 Range requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3RangeScenario(ctx, prefix)
}

func (runner *runner) m3RangeScenario(ctx context.Context, prefix string) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	group := []byte(prefix + "item/")
	end := []byte(clientv3.GetPrefixRangeEnd(string(group)))
	a := append(bytes.Clone(group), 'a')
	zero := append(bytes.Clone(a), 0)
	last := append(bytes.Clone(a), 0xff)
	base, err := runner.prepareRangeFixture(ctx, kv, a, zero, last)
	if err != nil {
		return err
	}
	if err := runner.m3OrdinaryRanges(ctx, kv, a, zero, last, group, end, base); err != nil {
		return err
	}
	if err := runner.m3FilteredCounts(ctx, kv, a, group, end, base); err != nil {
		return err
	}
	if err := runner.m3RangeCounts(ctx, kv, a, group, end, base); err != nil {
		return err
	}
	if err := runner.m3RangeTransaction(ctx, kv, group, end, a, base); err != nil {
		return err
	}
	if err := runner.m3RangeHistory(ctx, kv, a, base); err != nil {
		return err
	}
	if err := runner.m3ClientRange(ctx, kv, group); err != nil {
		return err
	}
	if err := runner.m3RangeCountAfterDelete(ctx, kv, a, group, end, base); err != nil {
		return err
	}
	return runner.m3RangeErrors(ctx, kv, a)
}

func (runner *runner) prepareRangeFixture(ctx context.Context, kv etcdserverpb.KVClient,
	a, zero, last []byte) (int64, error) {
	baseline, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{Key: a, Value: []byte("old")})
	if err != nil || baseline.Header == nil || baseline.Header.Revision < 1 {
		return 0, errors.New("Range baseline did not create a key")
	}
	base := baseline.Header.Revision
	seed, err := runner.m3Txn(ctx, kv, "J34-seed", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(zero, []byte("zero"), false), m3PutOp(last, []byte("last"), false),
	}})
	if err != nil || seed.Header == nil || seed.Header.Revision != base+1 {
		return 0, errors.New("Range seed did not commit once")
	}
	updated, err := runner.m3Put(ctx, kv, "J34-update", &etcdserverpb.PutRequest{Key: a, Value: []byte("new")})
	if err != nil || updated.Header == nil || updated.Header.Revision != base+2 {
		return 0, errors.New("Range update did not advance the revision")
	}
	return base, nil
}

func (runner *runner) m3OrdinaryRanges(ctx context.Context, kv etcdserverpb.KVClient,
	a, zero, last, group, end []byte, base int64) error {
	for _, test := range []struct {
		id      string
		request *etcdserverpb.RangeRequest
		keys    [][]byte
		values  []string
		more    bool
	}{
		{"J34-current", &etcdserverpb.RangeRequest{Key: a, Serializable: true}, [][]byte{a}, []string{"new"}, false},
		{"J34-historical", &etcdserverpb.RangeRequest{Key: a, Revision: base}, [][]byte{a}, []string{"old"}, false},
		{"J34-negative-revision", &etcdserverpb.RangeRequest{Key: a, Revision: -2}, [][]byte{a}, []string{"new"}, false},
		{"J34-prefix-page", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 2}, [][]byte{a, zero}, []string{"new", "zero"}, true},
		{"J34-keys-only", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 2, KeysOnly: true}, [][]byte{a, zero}, []string{"", ""}, true},
		{"J34-negative-limit", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: -1}, [][]byte{a, zero, last}, []string{"new", "zero", "last"}, false},
		{"J34-max-limit", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: math.MaxInt64}, [][]byte{a, zero, last}, []string{"new", "zero", "last"}, false},
		{"J34-min-mod", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MinModRevision: base + 2}, [][]byte{a}, []string{"new"}, false},
		{"J34-max-mod", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MaxModRevision: base + 1}, [][]byte{zero, last}, []string{"zero", "last"}, false},
		{"J34-min-create", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MinCreateRevision: base + 1}, [][]byte{zero, last}, []string{"zero", "last"}, false},
		{"J34-max-create", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MaxCreateRevision: base}, [][]byte{a}, []string{"new"}, false},
		{"J34-negative-min-mod", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MinModRevision: -1}, [][]byte{a, zero, last}, []string{"new", "zero", "last"}, false},
		{"J34-negative-max-mod", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MaxModRevision: -1}, nil, nil, false},
		{"J34-negative-min-create", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MinCreateRevision: -1}, [][]byte{a, zero, last}, []string{"new", "zero", "last"}, false},
		{"J34-negative-max-create", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Limit: 10, MaxCreateRevision: -1}, nil, nil, false},
	} {
		response, err := runner.m3Range(ctx, kv, test.id, test.request)
		if err != nil || response.Header == nil || response.Header.Revision != base+2 ||
			response.More != test.more || len(response.Kvs) != len(test.keys) {
			return fmt.Errorf("%s returned an incomplete Range", test.id)
		}
		for index, key := range test.keys {
			if !bytes.Equal(response.Kvs[index].Key, key) || string(response.Kvs[index].Value) != test.values[index] {
				return fmt.Errorf("%s returned incorrect ordered data", test.id)
			}
		}
	}
	return nil
}

func (runner *runner) m3RangeErrors(ctx context.Context, kv etcdserverpb.KVClient, a []byte) error {
	future := &etcdserverpb.RangeRequest{Key: a, Revision: math.MaxInt64}
	_, callErr := runner.callWithError(ctx, "J34-future", "etcdserverpb.KV/Range", future,
		func(callContext context.Context) (protoMessage, error) { return kv.Range(callContext, future) })
	if classified := trace.ClassifyError(callErr); classified == nil || classified.SemanticKind != "etcd/FutureRev" {
		return errors.New("future Range did not return the native error")
	}
	invalid := &etcdserverpb.RangeRequest{}
	_, callErr = runner.callWithError(ctx, "J34-empty-key", "etcdserverpb.KV/Range", invalid,
		func(callContext context.Context) (protoMessage, error) { return kv.Range(callContext, invalid) })
	if classified := trace.ClassifyError(callErr); classified == nil || classified.SemanticKind != "etcd/EmptyKey" {
		return errors.New("empty Range key did not return the native error")
	}
	return nil
}

type rangeFilter struct {
	id    string
	apply func(*etcdserverpb.RangeRequest)
}

func m3RangeFilters(base int64) []rangeFilter {
	return []rangeFilter{
		{"min-mod", func(request *etcdserverpb.RangeRequest) { request.MinModRevision = base + 9 }},
		{"max-mod", func(request *etcdserverpb.RangeRequest) { request.MaxModRevision = -1 }},
		{"min-create", func(request *etcdserverpb.RangeRequest) { request.MinCreateRevision = base + 9 }},
		{"max-create", func(request *etcdserverpb.RangeRequest) { request.MaxCreateRevision = -1 }},
	}
}

func (runner *runner) m3FilteredCounts(ctx context.Context, kv etcdserverpb.KVClient,
	a, group, end []byte, base int64) error {
	for _, filter := range m3RangeFilters(base) {
		for _, ranged := range []bool{false, true} {
			id := "J34-filtered-point-" + filter.id
			request := &etcdserverpb.RangeRequest{Key: a, Limit: 1}
			nativeCount := int64(1)
			if ranged {
				id = "J34-filtered-range-" + filter.id
				request.Key, request.RangeEnd, request.Limit = group, end, -1
				nativeCount = 3
			}
			filter.apply(request)
			response, err := runner.m3Range(ctx, kv, id, request)
			expected := int64(0)
			if runner.evidenceScope == "native-etcd" {
				expected = nativeCount
			}
			if err != nil || response.Count != expected || len(response.Kvs) != 0 || response.More {
				return fmt.Errorf("%s did not preserve filtered Count semantics", id)
			}
		}
	}
	if err := runner.m3PartiallyFilteredRanges(ctx, kv, a, group, end, base); err != nil {
		return err
	}
	return runner.m3FilteredNestedTransaction(ctx, kv, a, group, end, base)
}

func (runner *runner) m3PartiallyFilteredRanges(ctx context.Context, kv etcdserverpb.KVClient,
	a, group, end []byte, base int64) error {
	partial, err := runner.m3Range(ctx, kv, "J34-filtered-range-partial", &etcdserverpb.RangeRequest{
		Key: group, RangeEnd: end, MinModRevision: base + 2,
	})
	expected := int64(1)
	if runner.evidenceScope == "native-etcd" {
		expected = 3
	}
	if err != nil || !m3FilteredRangeMatches(partial, a, expected) {
		return errors.New("partially filtered Range returned an incorrect result")
	}
	matched, err := runner.m3Range(ctx, kv, "J34-filtered-point-match", &etcdserverpb.RangeRequest{
		Key: a, MinModRevision: base + 2,
	})
	if err != nil || !m3FilteredRangeMatches(matched, a, 1) {
		return errors.New("matching filtered point returned an incorrect result")
	}
	return nil
}

func m3FilteredRangeMatches(response *etcdserverpb.RangeResponse, key []byte, count int64) bool {
	return response.Count == count && len(response.Kvs) == 1 && bytes.Equal(response.Kvs[0].Key, key) && !response.More
}

func (runner *runner) m3FilteredNestedTransaction(ctx context.Context, kv etcdserverpb.KVClient,
	a, group, end []byte, base int64) error {
	point := &etcdserverpb.RangeRequest{Key: a, MinModRevision: base + 9}
	ranged := &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, MinModRevision: base + 9}
	request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: point}},
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: ranged}},
		}}},
	}}}
	response, err := runner.m3Txn(ctx, kv, "J34-filtered-nested-txn", request)
	if err != nil || len(response.Responses) != 1 || response.Responses[0].GetResponseTxn() == nil ||
		len(response.Responses[0].GetResponseTxn().Responses) != 2 {
		return errors.New("filtered nested transaction returned an incomplete result")
	}
	for index, op := range response.Responses[0].GetResponseTxn().Responses {
		result := op.GetResponseRange()
		want := int64(0)
		if runner.evidenceScope == "native-etcd" {
			want = []int64{1, 3}[index]
		}
		if result == nil || result.Count != want || len(result.Kvs) != 0 || result.More {
			return errors.New("filtered nested transaction returned an incorrect Count")
		}
	}
	return nil
}

func (runner *runner) m3RangeCounts(ctx context.Context, kv etcdserverpb.KVClient,
	a, group, end []byte, base int64) error {
	for _, filter := range m3RangeFilters(base) {
		for _, ranged := range []bool{false, true} {
			id := "J34-count-point-" + filter.id
			request := &etcdserverpb.RangeRequest{Key: a, CountOnly: true, KeysOnly: true}
			if ranged {
				id = "J34-count-range-" + filter.id
				request.Key, request.RangeEnd, request.Limit = group, end, 1
			}
			filter.apply(request)
			response, err := runner.m3Range(ctx, kv, id, request)
			if err != nil || response.Count < 1 || len(response.Kvs) != 0 || response.More {
				return fmt.Errorf("%s did not report an existing key", id)
			}
		}
	}
	missing, err := runner.m3Range(ctx, kv, "J34-count-missing", &etcdserverpb.RangeRequest{
		Key: append(bytes.Clone(group), 'z'), CountOnly: true, MinModRevision: base + 9,
	})
	if err != nil || missing.Count != 0 || len(missing.Kvs) != 0 || missing.More {
		return errors.New("CountOnly reported a missing key as existing")
	}
	missingPrefix := append(bytes.Clone(group), 'z', '/')
	empty, err := runner.m3Range(ctx, kv, "J34-count-empty-range", &etcdserverpb.RangeRequest{
		Key: missingPrefix, RangeEnd: []byte(clientv3.GetPrefixRangeEnd(string(missingPrefix))),
		Limit: 1, CountOnly: true, MaxCreateRevision: -1,
	})
	if err != nil || empty.Count != 0 || len(empty.Kvs) != 0 || empty.More {
		return errors.New("CountOnly reported an empty range as existing")
	}
	return nil
}

func (runner *runner) m3RangeTransaction(ctx context.Context, kv etcdserverpb.KVClient,
	group, end, key []byte, base int64) error {
	newKey := append(bytes.Clone(group), 'c')
	request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(newKey, []byte("txn"), false),
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: group, RangeEnd: end, Limit: 10, MinModRevision: base + 3,
		}}},
	}}
	response, err := runner.m3Txn(ctx, kv, "J34-txn-overlay", request)
	if err != nil || response.Header == nil || response.Header.Revision != base+3 || len(response.Responses) != 2 {
		return errors.New("Range transaction did not commit once")
	}
	rangeResult := response.Responses[1].GetResponseRange()
	if rangeResult == nil || len(rangeResult.Kvs) != 1 || !bytes.Equal(rangeResult.Kvs[0].Key, newKey) ||
		string(rangeResult.Kvs[0].Value) != "txn" {
		return errors.New("transaction Range did not see the prior Put")
	}
	old, recent := m3Range(key), m3Range(key)
	old.GetRequestRange().Revision, recent.GetRequestRange().Revision = base, base+2
	history, err := runner.m3Txn(ctx, kv, "J34-txn-history", &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{old, recent},
	})
	if err != nil || len(history.Responses) != 2 || history.Header == nil || history.Header.Revision != base+3 {
		return errors.New("transaction did not retain both historical Range views")
	}
	for index, expected := range []string{"old", "new"} {
		child := history.Responses[index].GetResponseRange()
		if child == nil || child.Header == nil || child.Header.Revision != base+3 ||
			len(child.Kvs) != 1 || string(child.Kvs[0].Value) != expected {
			return errors.New("transaction historical Range returned an incorrect view")
		}
	}
	negative := m3Range(key)
	negative.GetRequestRange().Revision = -2
	txn := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{negative}}
	_, callErr := runner.callWithError(ctx, "J34-txn-negative", "etcdserverpb.KV/Txn", txn,
		func(callContext context.Context) (protoMessage, error) { return kv.Txn(callContext, txn) })
	if classified := trace.ClassifyError(callErr); classified == nil || classified.SemanticKind != "etcd/Compacted" {
		return errors.New("negative transaction revision did not return the native error")
	}
	return nil
}

func (runner *runner) m3RangeHistory(ctx context.Context, kv etcdserverpb.KVClient, key []byte, base int64) error {
	deleted, err := runner.m3Delete(ctx, kv, "J34-delete", &etcdserverpb.DeleteRangeRequest{Key: key})
	if err != nil || deleted.Header == nil || deleted.Header.Revision != base+4 || deleted.Deleted != 1 {
		return errors.New("Range history fixture delete failed")
	}
	recreated, err := runner.m3Put(ctx, kv, "J34-recreate", &etcdserverpb.PutRequest{Key: key, Value: []byte("reborn")})
	if err != nil || recreated.Header == nil || recreated.Header.Revision != base+5 {
		return errors.New("Range history fixture recreation failed")
	}
	absent, err := runner.m3Range(ctx, kv, "J34-history-deleted", &etcdserverpb.RangeRequest{Key: key, Revision: base + 4})
	if err != nil || absent.Header == nil || absent.Header.Revision != base+5 || absent.Count != 0 || len(absent.Kvs) != 0 {
		return errors.New("historical Range did not preserve deletion")
	}
	deletedCount, err := runner.m3Range(ctx, kv, "J34-history-count-deleted", &etcdserverpb.RangeRequest{
		Key: key, Revision: base + 4, CountOnly: true, MinModRevision: base + 9,
	})
	if err != nil || deletedCount.Count != 0 || len(deletedCount.Kvs) != 0 || deletedCount.More {
		return errors.New("historical CountOnly did not preserve deletion")
	}
	liveCount, err := runner.m3Range(ctx, kv, "J34-history-count-live", &etcdserverpb.RangeRequest{
		Key: key, Revision: base + 2, CountOnly: true, MaxModRevision: -1,
	})
	if err != nil || liveCount.Count != 1 || len(liveCount.Kvs) != 0 || liveCount.More {
		return errors.New("historical CountOnly lost a live key")
	}
	current, err := runner.m3Range(ctx, kv, "J34-history-recreated", &etcdserverpb.RangeRequest{Key: key})
	if err != nil || !m3RecreatedMetadataMatches(current, base+5) || string(current.Kvs[0].Value) != "reborn" {
		return errors.New("recreated key metadata is incorrect")
	}
	return nil
}

func m3RecreatedMetadataMatches(response *etcdserverpb.RangeResponse, revision int64) bool {
	if len(response.Kvs) != 1 {
		return false
	}
	record := response.Kvs[0]
	return record.CreateRevision == revision && record.ModRevision == revision && record.Version == 1
}

func (runner *runner) m3RangeCountAfterDelete(ctx context.Context, kv etcdserverpb.KVClient,
	key, group, end []byte, base int64) error {
	point := &etcdserverpb.RangeRequest{Key: key, CountOnly: true, MaxModRevision: -1}
	ranged := &etcdserverpb.RangeRequest{
		Key: group, RangeEnd: end, Limit: 1, CountOnly: true, MaxCreateRevision: -1,
	}
	request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: point}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: ranged}},
	}}
	response, err := runner.m3Txn(ctx, kv, "J34-txn-count-after-delete", request)
	if err != nil || response.Header == nil || response.Header.Revision != base+6 || len(response.Responses) != 3 {
		return errors.New("CountOnly transaction did not commit once")
	}
	deleted := response.Responses[0].GetResponseDeleteRange()
	pointResult := response.Responses[1].GetResponseRange()
	rangeResult := response.Responses[2].GetResponseRange()
	if !m3DeletedOneKey(deleted) || !m3PointCountIsAbsent(pointResult) || !m3RangeCountIsPresent(rangeResult) {
		return errors.New("CountOnly did not see the transaction's prior delete")
	}
	return nil
}

func m3DeletedOneKey(response *etcdserverpb.DeleteRangeResponse) bool {
	return response != nil && response.Deleted == 1
}

func m3PointCountIsAbsent(response *etcdserverpb.RangeResponse) bool {
	return response != nil && response.Count == 0 && len(response.Kvs) == 0 && !response.More
}

func m3RangeCountIsPresent(response *etcdserverpb.RangeResponse) bool {
	return response != nil && response.Count >= 1 && len(response.Kvs) == 0 && !response.More
}

type capturingRangeClient struct {
	etcdserverpb.KVClient
	request *etcdserverpb.RangeRequest
}

func (client *capturingRangeClient) Range(ctx context.Context, request *etcdserverpb.RangeRequest,
	opts ...grpc.CallOption) (*etcdserverpb.RangeResponse, error) {
	client.request = proto.Clone(request).(*etcdserverpb.RangeRequest)
	return client.KVClient.Range(ctx, request, opts...)
}

func (runner *runner) m3ClientRange(ctx context.Context, kv etcdserverpb.KVClient, group []byte) error {
	capture := &capturingRangeClient{KVClient: kv}
	client := clientv3.NewKVFromKVClient(capture, nil)
	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	started := time.Now()
	response, callErr := client.Get(requestContext, string(group), clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(1))
	elapsed := time.Since(started)
	cancel()
	if capture.request == nil {
		return errors.New("clientv3 did not dispatch a Range request")
	}
	var wire *etcdserverpb.RangeResponse
	if response != nil {
		wire = (*etcdserverpb.RangeResponse)(response)
	}
	if err := runner.record("J34-client-prefix", "etcdserverpb.KV/Range", capture.request, wire, callErr, nil, elapsed); err != nil {
		return err
	}
	if callErr != nil || wire == nil || len(wire.Kvs) != 1 || !wire.More || len(wire.Kvs[0].Value) != 0 {
		return errors.New("clientv3 prefix Range returned an incorrect page")
	}
	return nil
}

func (runner *runner) m3Range(ctx context.Context, kv etcdserverpb.KVClient,
	caseID string, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/Range", request,
		func(callContext context.Context) (protoMessage, error) { return kv.Range(callContext, request) })
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.RangeResponse)
	if !ok || response == nil {
		return nil, fmt.Errorf("%s: Range returned no response", caseID)
	}
	return response, nil
}
