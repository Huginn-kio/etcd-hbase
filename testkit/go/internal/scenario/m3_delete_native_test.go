package scenario

import (
	"bytes"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestM3NativeDeleteOrderingAndEmptyRevision(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp([]byte("a"), []byte("one"), false),
		m3PutOp([]byte("b"), []byte("two"), false),
		m3PutOp([]byte("c"), []byte("three"), false),
	}})
	if err != nil {
		t.Fatal(err)
	}
	deleteRange := func(start, end string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(start), RangeEnd: []byte(end), PrevKv: true},
		}}
	}
	response, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3Range([]byte("a")), deleteRange("a", "c"), m3Range([]byte("a")),
		deleteRange("a", "z"), deleteRange("a", "z"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Revision != 3 || response.Responses[1].GetResponseDeleteRange().Deleted != 2 ||
		response.Responses[3].GetResponseDeleteRange().Deleted != 1 ||
		response.Responses[4].GetResponseDeleteRange().Deleted != 0 {
		t.Fatal("native delete operations did not share one revision and one live working view")
	}
	before, first, after := response.Responses[0].GetResponseRange(),
		response.Responses[1].GetResponseDeleteRange(), response.Responses[2].GetResponseRange()
	second, empty := response.Responses[3].GetResponseDeleteRange(), response.Responses[4].GetResponseDeleteRange()
	if before.Header.Revision != 2 || first.Header.Revision != 3 || after.Header.Revision != 3 ||
		second.Header.Revision != 3 || empty.Header.Revision != 3 || len(before.Kvs) != 1 || len(after.Kvs) != 0 {
		t.Fatal("native per-operation headers or reads do not match execution order")
	}
	if len(first.PrevKvs) != 2 || !bytes.Equal(first.PrevKvs[0].Key, []byte("a")) ||
		!bytes.Equal(first.PrevKvs[1].Key, []byte("b")) || len(second.PrevKvs) != 1 ||
		!bytes.Equal(second.PrevKvs[0].Key, []byte("c")) || len(empty.PrevKvs) != 0 {
		t.Fatal("native previous values are not the ordered live records before each delete")
	}
	standalone, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("z")})
	if err != nil || standalone.Deleted != 0 || standalone.Header.Revision != 3 {
		t.Fatalf("native empty delete changed revision or failed: %v", err)
	}
}

func TestM3NativeZeroEndAllowsDeleteAndPutOfSameKey(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	key := []byte("z")
	if _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	del := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, PrevKv: true},
	}}
	first, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		del, m3PutOp(key, []byte("new"), true), m3Range(key),
	}})
	if err != nil {
		t.Fatalf("native rejected zero-end delete then Put: %v", err)
	}
	read := first.Responses[2].GetResponseRange().Kvs[0]
	if first.Header.Revision != 3 || first.Responses[0].GetResponseDeleteRange().Deleted != 1 ||
		first.Responses[1].GetResponsePut().PrevKv != nil || read.CreateRevision != 3 ||
		read.ModRevision != 3 || read.Version != 1 || !bytes.Equal(read.Value, []byte("new")) {
		t.Fatal("native delete then Put did not recreate the key in the same revision")
	}
	second, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(key, []byte("next"), true), del, m3Range(key),
	}})
	if err != nil {
		t.Fatalf("native rejected Put then zero-end delete: %v", err)
	}
	previous := second.Responses[1].GetResponseDeleteRange().PrevKvs[0]
	if second.Header.Revision != 4 || second.Responses[1].GetResponseDeleteRange().Deleted != 1 ||
		previous.CreateRevision != 3 || previous.ModRevision != 4 || previous.Version != 2 ||
		!bytes.Equal(previous.Value, []byte("next")) || len(second.Responses[2].GetResponseRange().Kvs) != 0 {
		t.Fatal("native Put then delete did not use the previously written working value")
	}
}

func TestM3NativeDeleteEnvelopeSizes(t *testing.T) {
	for _, vector := range [][]int{{0, 21, 27, 30}, {127, 151, 157, 160}, {128, 153, 159, 162}, {16384, 16411, 16417, 16420}} {
		request := &etcdserverpb.DeleteRangeRequest{Key: []byte("m3-probe-key"), RangeEnd: make([]byte, vector[0])}
		for index, id := range []uint64{1, 1 << 48, 1 << 63} {
			internal := &etcdserverpb.InternalRaftRequest{Header: &etcdserverpb.RequestHeader{ID: id}, DeleteRange: request}
			if got := internal.Size(); got != vector[index+1] {
				t.Fatalf("native DeleteRange envelope size = %d, want %d", got, vector[index+1])
			}
		}
	}
}

func TestM3NativeFullKeyspaceDeleteOnDisposableServer(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	for _, key := range []string{"a", "b"} {
		if _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	request := &etcdserverpb.DeleteRangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, PrevKv: true}
	removed, err := kv.DeleteRange(ctx, request)
	if err != nil || removed.Deleted != 2 || len(removed.PrevKvs) != 2 ||
		!bytes.Equal(removed.PrevKvs[0].Key, []byte("a")) || !bytes.Equal(removed.PrevKvs[1].Key, []byte("b")) {
		t.Fatalf("native full keyspace delete returned the wrong result: %v", err)
	}
	empty, err := kv.DeleteRange(ctx, request)
	if err != nil || empty.Deleted != 0 || empty.Header.Revision != removed.Header.Revision {
		t.Fatalf("native empty full keyspace delete changed revision: %v", err)
	}
}
