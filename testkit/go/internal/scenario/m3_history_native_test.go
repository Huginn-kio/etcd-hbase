package scenario

import (
	"sync"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/history"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// The reference model is independent of the service implementation. Tampering
// with a real response must make this bounded history impossible.
func TestM3HistoryChecksRealNativeConcurrentPointOperations(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	client := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	recorder, err := history.NewRecorder(3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	failures := make(chan error, 2)
	for _, value := range []string{"one", "two"} {
		workers.Add(1)
		go func(value string) {
			defer workers.Done()
			_, err := recorder.Invoke(ctx, client, &etcdserverpb.PutRequest{Key: []byte("history-key"), Value: []byte(value), PrevKv: true})
			failures <- err
		}(value)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: []byte("history-key")}); err != nil {
		t.Fatal(err)
	}
	observations, err := recorder.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	initial, err := history.NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := history.Check(ctx, observations, history.KVModel{}, initial, 1000)
	if err != nil || result.Verdict != history.Linearizable {
		t.Fatalf("native history: %+v, %v", result, err)
	}
	var read etcdserverpb.RangeResponse
	if err := proto.Unmarshal(observations[2].Response, &read); err != nil {
		t.Fatal(err)
	}
	read.Kvs[0].Value = []byte("not-written")
	observations[2].Response, err = proto.Marshal(&read)
	if err != nil {
		t.Fatal(err)
	}
	result, err = history.Check(ctx, observations, history.KVModel{}, initial, 1000)
	if err != nil || result.Verdict != history.Invalid {
		t.Fatalf("tampered history: %+v, %v", result, err)
	}
}

// This history exercises native branch selection, working-view reads, one
// revision for multiple writes, and the independent response checks.
func TestM3HistoryChecksNativeNestedTransactions(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	client := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	recorder, err := history.NewRecorder(11, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	a, b := []byte("history-txn-a"), []byte("history-txn-b")
	version := func(key []byte, v int64) *etcdserverpb.Compare {
		return &etcdserverpb.Compare{Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: v}}
	}
	request := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{version(a, 0)}, Success: []*etcdserverpb.RequestOp{
		m3Range(a), m3PutOp(a, []byte("one"), true),
		{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{version(a, 0)}, Success: []*etcdserverpb.RequestOp{m3Range(a), m3PutOp(b, []byte("two"), false)},
		}}},
	}}
	if _, err := recorder.Invoke(ctx, client, request); err != nil {
		t.Fatal(err)
	}
	historical := m3Range(a)
	historical.GetRequestRange().Revision = 2
	second := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{version(a, 0)},
		Success: []*etcdserverpb.RequestOp{m3PutOp(a, []byte("unused"), false)},
		Failure: []*etcdserverpb.RequestOp{m3PutOp(a, []byte("next"), true), historical}}
	if _, err := recorder.Invoke(ctx, client, second); err != nil {
		t.Fatal(err)
	}
	rangeCompare := version([]byte("history-txn-"), 0)
	rangeCompare.RangeEnd = []byte("history-txn.")
	rangeCompare.Result = etcdserverpb.Compare_GREATER
	third := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{rangeCompare}, Success: []*etcdserverpb.RequestOp{m3Range(b)}}
	if _, err := recorder.Invoke(ctx, client, third); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: []byte("history-txn-"), RangeEnd: []byte("history-txn.")}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.DeleteRangeRequest{Key: b, PrevKv: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: a}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: b, Revision: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.PutRequest{Key: b, Value: []byte("recreated")}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: b, Revision: 4}); err != nil {
		t.Fatal(err)
	}
	deleteTxn := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("history-txn-"), RangeEnd: []byte("history-txn."), PrevKv: true},
	}}}}
	if _, err := recorder.Invoke(ctx, client, deleteTxn); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Invoke(ctx, client, &etcdserverpb.RangeRequest{Key: []byte("history-txn-"), RangeEnd: []byte("history-txn."), Revision: 2}); err != nil {
		t.Fatal(err)
	}
	observations, err := recorder.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	initial, err := history.NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	check := func(want history.Verdict) {
		t.Helper()
		result, err := history.Check(ctx, observations, history.KVModel{}, initial, 1000)
		if err != nil || result.Verdict != want {
			t.Fatalf("history: %+v, %v, want %s", result, err, want)
		}
	}
	check(history.Linearizable)
	for _, test := range []struct {
		name   string
		index  int
		mutate func(*etcdserverpb.TxnResponse)
	}{
		{"nested branch", 0, func(r *etcdserverpb.TxnResponse) { r.Responses[2].GetResponseTxn().Succeeded = false }},
		{"nested value", 0, func(r *etcdserverpb.TxnResponse) {
			r.Responses[2].GetResponseTxn().Responses[0].GetResponseRange().Kvs[0].Value = []byte("wrong")
		}},
		{"single revision", 0, func(r *etcdserverpb.TxnResponse) { r.Header.Revision++ }},
		{"else branch", 1, func(r *etcdserverpb.TxnResponse) { r.Succeeded = true }},
		{"previous value", 1, func(r *etcdserverpb.TxnResponse) { r.Responses[0].GetResponsePut().PrevKv.Value = []byte("wrong") }},
		{"historical value", 1, func(r *etcdserverpb.TxnResponse) { r.Responses[1].GetResponseRange().Kvs[0].Value = []byte("wrong") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := observations[test.index].Response
			defer func() { observations[test.index].Response = original }()
			var response etcdserverpb.TxnResponse
			if err := proto.Unmarshal(original, &response); err != nil {
				t.Fatal(err)
			}
			test.mutate(&response)
			observations[test.index].Response, err = proto.Marshal(&response)
			if err != nil {
				t.Fatal(err)
			}
			check(history.Invalid)
		})
	}
}
