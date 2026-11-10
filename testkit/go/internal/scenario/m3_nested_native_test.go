package scenario

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestM3NativeNestedCompareUsesBaseAndChildHeaderIsEmpty(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	key, value := []byte("nested-native-key"), []byte{0, 0xff}
	request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(key, value, false),
		{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{Key: key, Target: etcdserverpb.Compare_VERSION,
				Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}},
			Success: []*etcdserverpb.RequestOp{m3Range(key)},
		}}},
	}}
	response, err := kv.Txn(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Header == nil || response.Header.Revision != 2 || len(response.Responses) != 2 {
		t.Fatal("native outer transaction did not commit once")
	}
	child := response.Responses[1].GetResponseTxn()
	if child == nil || !child.Succeeded || child.Header == nil || child.Header.Revision != 0 ||
		child.Header.ClusterId != 0 || child.Header.MemberId != 0 || child.Header.RaftTerm != 0 || len(child.Responses) != 1 {
		t.Fatal("native nested branch or header differs")
	}
	read := child.Responses[0].GetResponseRange()
	if read == nil || read.Header == nil || read.Header.Revision != 2 || len(read.Kvs) != 1 ||
		!bytes.Equal(read.Kvs[0].Value, value) {
		t.Fatal("native nested read did not see the earlier Put")
	}
}

func TestM3NativeNestedHistoricalReadAndRevisionPrecheck(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	key := []byte("nested-history-key")
	first, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("base")}); err != nil {
		t.Fatal(err)
	}
	historical := m3Range(key)
	historical.GetRequestRange().Revision = first.Header.Revision
	request := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(key, []byte("new"), false),
		{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{m3Range(key), historical},
		}}},
	}}
	response, err := kv.Txn(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	child := response.Responses[1].GetResponseTxn()
	current := child.Responses[0].GetResponseRange()
	old := child.Responses[1].GetResponseRange()
	if response.Header.Revision != 4 || current.Header.Revision != 4 || old.Header.Revision != 4 ||
		len(current.Kvs) != 1 || len(old.Kvs) != 1 || string(current.Kvs[0].Value) != "new" || string(old.Kvs[0].Value) != "old" {
		t.Fatal("native nested historical read or response header differs")
	}
	assertNativeRevisionPrecheck(t, ctx, kv, key)
}

func assertNativeRevisionPrecheck(t *testing.T, ctx context.Context, kv etcdserverpb.KVClient, key []byte) {
	t.Helper()
	for _, test := range []struct {
		revision int64
		semantic string
	}{
		{-1, ""}, {-2, "etcd/Compacted"}, {math.MaxInt64, "etcd/FutureRev"},
	} {
		rangeOp := m3Range(key)
		rangeOp.GetRequestRange().Revision = test.revision
		_, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeOp}})
		if test.semantic == "" {
			if callErr != nil {
				t.Fatalf("native revision %d unexpectedly failed", test.revision)
			}
		} else if got := trace.ClassifyError(callErr); got == nil || got.SemanticKind != test.semantic {
			t.Fatalf("native revision %d has a different error", test.revision)
		}
	}
}

func TestM3NativeNestedStaticOverlapAndDynamicErrorOrder(t *testing.T) {
	ctx := testContext(t)
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	kv := etcdserverpb.NewKVClient(server.client.ActiveConnection())
	put := func(key string) *etcdserverpb.RequestOp { return m3PutOp([]byte(key), nil, false) }
	del := func(start, end string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(start), RangeEnd: []byte(end)},
		}}
	}

	assertNativeStaticOverlaps(t, ctx, kv, put, del)
	assertNativeDynamicOrder(t, ctx, kv)
}

func m3NativeNested(then, otherwise []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: then, Failure: otherwise},
	}}
}

func assertNativeStaticOverlaps(t *testing.T, ctx context.Context, kv etcdserverpb.KVClient,
	put func(string) *etcdserverpb.RequestOp, del func(string, string) *etcdserverpb.RequestOp) {
	t.Helper()
	for _, test := range []struct {
		name     string
		ops      []*etcdserverpb.RequestOp
		semantic string
	}{
		{"exclusive-child-puts", []*etcdserverpb.RequestOp{m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, []*etcdserverpb.RequestOp{put("a")})}, ""},
		{"exclusive-child-put-delete", []*etcdserverpb.RequestOp{m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, []*etcdserverpb.RequestOp{del("a", "")})}, ""},
		{"siblings-put-duplicate", []*etcdserverpb.RequestOp{m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, nil), m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, nil)}, "etcd/DuplicateKey"},
		{"parent-delete-child-put", []*etcdserverpb.RequestOp{del("a", "z"), m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, nil)}, "etcd/DuplicateKey"},
		{"child-put-parent-delete-reversed-order", []*etcdserverpb.RequestOp{m3NativeNested([]*etcdserverpb.RequestOp{put("a")}, nil), del("a", "z")}, "etcd/DuplicateKey"},
		{"sibling-delete-put", []*etcdserverpb.RequestOp{m3NativeNested([]*etcdserverpb.RequestOp{del("a", "z")}, nil), m3NativeNested([]*etcdserverpb.RequestOp{put("m")}, nil)}, "etcd/DuplicateKey"},
		{"upper-bound-exclusive", []*etcdserverpb.RequestOp{del("a", "z"), put("z")}, ""},
		{"overlapping-deletes", []*etcdserverpb.RequestOp{del("a", "z"), del("m", "zz")}, ""},
		{"reversed-delete", []*etcdserverpb.RequestOp{del("z", "a"), put("m")}, ""},
		{"zero-end-raw-interval", []*etcdserverpb.RequestOp{del("a", "\x00"), put("z")}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The branch is unselected. Static validation must still inspect it without writes.
			_, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Failure: test.ops})
			if test.semantic == "" {
				if callErr != nil {
					t.Fatalf("native rejected a valid request: %v", callErr)
				}
			} else if got := trace.ClassifyError(callErr); got == nil || got.SemanticKind != test.semantic {
				t.Fatalf("native error = %v, want %s", callErr, test.semantic)
			}
		})
	}
}

func assertNativeDynamicOrder(t *testing.T, ctx context.Context, kv etcdserverpb.KVClient) {
	t.Helper()
	future := m3Range([]byte("missing"))
	future.GetRequestRange().Revision = math.MaxInt64
	ignoreMissing := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: []byte("missing-ignore"), IgnoreValue: true},
	}}
	_, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		future, m3NativeNested([]*etcdserverpb.RequestOp{ignoreMissing}, nil),
	}})
	if got := trace.ClassifyError(err); got == nil || got.SemanticKind != "etcd/KeyNotFound" {
		t.Fatalf("native validates selected Put before Range revision, got %v", err)
	}
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{
		m3NativeNested([]*etcdserverpb.RequestOp{future, ignoreMissing}, nil),
	}})
	if err != nil {
		t.Fatalf("native evaluated an unselected dynamic operation: %v", err)
	}
}
