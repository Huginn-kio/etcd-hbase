package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// RunM3NativeSort uses an isolated pinned native server as the oracle.
func RunM3NativeSort(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native sort requires a context, new data directory, and output")
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
	return RunM3Sort(ctx, server.client, "native-etcd", prefix, output)
}

// RunM3Sort records ordered Range requests and checks every returned key and page boundary.
func RunM3Sort(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 sort requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3SortScenario(ctx, prefix)
}

func (runner *runner) m3SortScenario(ctx context.Context, prefix string) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	group := []byte(prefix + "sort/")
	end := []byte(clientv3.GetPrefixRangeEnd(string(group)))
	a := append(bytes.Clone(group), 'a')
	zero := append(bytes.Clone(a), 0)
	last := append(bytes.Clone(a), 0xff)
	baseline, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{Key: a, Value: []byte("old")})
	if err != nil || baseline.Header == nil || baseline.Header.Revision < 1 {
		return errors.New("sort baseline did not create a key")
	}
	base := baseline.Header.Revision
	seed, err := runner.m3Txn(ctx, kv, "J35-seed", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(zero, []byte("zero"), false), m3PutOp(last, []byte("last"), false),
	}})
	if err != nil || seed.Header == nil || seed.Header.Revision != base+1 {
		return errors.New("sort seed did not commit once")
	}
	updated, err := runner.m3Put(ctx, kv, "J35-update", &etcdserverpb.PutRequest{Key: a, Value: []byte("new")})
	if err != nil || updated.Header == nil || updated.Header.Revision != base+2 {
		return errors.New("sort update did not commit once")
	}
	for _, test := range []struct {
		id      string
		request *etcdserverpb.RangeRequest
		keys    [][]byte
		more    bool
	}{
		{"J35-none", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end}, [][]byte{a, zero, last}, false},
		{"J35-asc", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_ASCEND}, [][]byte{a, zero, last}, false},
		{"J35-desc", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_DESCEND}, [][]byte{last, zero, a}, false},
		{"J35-desc-first", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Revision: base + 2, SortOrder: etcdserverpb.RangeRequest_DESCEND, Limit: 2}, [][]byte{last, zero}, true},
		{"J35-desc-next", &etcdserverpb.RangeRequest{Key: group, RangeEnd: zero, Revision: base + 2, SortOrder: etcdserverpb.RangeRequest_DESCEND, Limit: 2}, [][]byte{a}, false},
		{"J35-desc-filter", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_DESCEND, Limit: 1, MinCreateRevision: base + 1}, [][]byte{last}, true},
		{"J35-desc-history", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, Revision: base, SortOrder: etcdserverpb.RangeRequest_DESCEND}, [][]byte{a}, false},
		{"J35-desc-point", &etcdserverpb.RangeRequest{Key: a, SortOrder: etcdserverpb.RangeRequest_DESCEND}, [][]byte{a}, false},
		{"J35-desc-keys", &etcdserverpb.RangeRequest{Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_DESCEND, KeysOnly: true, Limit: 2}, [][]byte{last, zero}, true},
	} {
		response, err := runner.m3Range(ctx, kv, test.id, test.request)
		if err != nil || response.Header == nil || response.Header.Revision != base+2 || response.More != test.more || len(response.Kvs) != len(test.keys) {
			return fmt.Errorf("%s returned an incomplete sorted Range", test.id)
		}
		for index, key := range test.keys {
			if !bytes.Equal(response.Kvs[index].Key, key) || test.request.KeysOnly && len(response.Kvs[index].Value) != 0 {
				return fmt.Errorf("%s returned incorrect order or projection", test.id)
			}
		}
	}
	count, err := runner.m3Range(ctx, kv, "J35-desc-count", &etcdserverpb.RangeRequest{
		Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_DESCEND, Limit: 1, CountOnly: true,
	})
	wantCount := int64(1)
	if runner.evidenceScope == "native-etcd" {
		wantCount = 3
	}
	if err != nil || count.Count != wantCount || len(count.Kvs) != 0 || count.More {
		return errors.New("descending CountOnly did not preserve its approved existence semantics")
	}
	capture := &capturingRangeClient{KVClient: kv}
	client := clientv3.NewKVFromKVClient(capture, nil)
	callContext, cancel := context.WithTimeout(ctx, requestTimeout)
	started := time.Now()
	response, callErr := client.Get(callContext, string(group), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	elapsed := time.Since(started)
	cancel()
	if capture.request == nil {
		return errors.New("clientv3 did not dispatch the sorted Range")
	}
	var wire *etcdserverpb.RangeResponse
	if response != nil {
		wire = (*etcdserverpb.RangeResponse)(response)
	}
	if err := runner.record("J35-client-asc", "etcdserverpb.KV/Range", capture.request, wire, callErr, nil, elapsed); err != nil {
		return err
	}
	if callErr != nil || wire == nil || capture.request.SortOrder != etcdserverpb.RangeRequest_NONE || len(wire.Kvs) != 3 || !bytes.Equal(wire.Kvs[0].Key, a) {
		return errors.New("clientv3 ascending sort did not preserve its native request semantics")
	}
	newKey := append(bytes.Clone(group), 'b')
	transaction, err := runner.m3Txn(ctx, kv, "J35-txn-desc", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(newKey, []byte("txn"), false),
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: group, RangeEnd: end, SortOrder: etcdserverpb.RangeRequest_DESCEND, Limit: 2,
		}}},
	}})
	if err != nil || len(transaction.Responses) != 2 || transaction.Responses[1].GetResponseRange() == nil {
		return errors.New("sorted transaction returned an incomplete response")
	}
	child := transaction.Responses[1].GetResponseRange()
	if len(child.Kvs) != 2 || !bytes.Equal(child.Kvs[0].Key, newKey) || !bytes.Equal(child.Kvs[1].Key, last) || !child.More {
		return errors.New("sorted transaction did not merge its working set")
	}
	return nil
}
