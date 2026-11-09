package compare

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM3DeleteComparisonKeepsPreviousValuesAndMethodsStrict(t *testing.T) {
	native := deleteTrace(t, "native-etcd", 3)
	target := deleteTrace(t, "target-hbase", 0)
	left, right := encodeTrace(t, native), encodeTrace(t, target)
	if err := CompareM3DeleteTraces(bytes.NewReader(left), bytes.NewReader(right)); err != nil {
		t.Fatalf("matching delete traces differ: %v", err)
	}
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(right)); err == nil {
		t.Fatal("Txn-only policy accepted standalone DeleteRange")
	}
	bad := deleteTrace(t, "target-hbase", 0)
	bad[1].Response = replace(bad[1].Response, `"value":"AQ=="`, `"value":"Ag=="`)
	if err := CompareM3DeleteTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
		t.Fatal("delete policy hid a previous-value mismatch")
	}
}

func deleteTrace(t *testing.T, scope string, term uint64) []trace.Record {
	t.Helper()
	records := m3TxnTrace(t, scope, term)
	request, err := trace.SnapshotProto(&etcdserverpb.DeleteRangeRequest{Key: []byte("k"), PrevKv: true})
	if err != nil {
		t.Fatal(err)
	}
	response, err := trace.SnapshotProto(&etcdserverpb.DeleteRangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 3, RaftTerm: term}, Deleted: 1,
		PrevKvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	records[1].Method = "etcdserverpb.KV/DeleteRange"
	records[1].Request, records[1].Response = request, response
	return records
}

func TestM3DeleteComparisonKeepsTxnCountOneofsStrict(t *testing.T) {
	for _, nested := range []bool{false, true} {
		native, target := malformedCountTraces(t, nested, true)
		if err := CompareM3DeleteTraces(bytes.NewReader(encodeTrace(t, native)),
			bytes.NewReader(encodeTrace(t, target))); err != nil {
			t.Fatalf("valid Count allowance rejected by delete policy (nested=%t): %v", nested, err)
		}
	}
	cases := []struct{ nested, request, strictSibling bool }{
		{false, false, false}, {false, false, true}, {false, true, false}, {false, true, true},
		{true, false, false}, {true, false, true}, {true, true, false}, {true, true, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("nested=%t/request=%t/strictSibling=%t", tc.nested, tc.request, tc.strictSibling), func(t *testing.T) {
			assertDeletePolicyRejectsMalformed(t, tc.nested, tc.request, tc.strictSibling)
		})
	}
}

func assertDeletePolicyRejectsMalformed(t *testing.T, nested, request, strictSibling bool) {
	t.Helper()
	native, target := malformedCountTraces(t, nested, strictSibling)
	mutateCountOneof(native, request, strictSibling)
	mutateCountOneof(target, request, strictSibling)
	for _, pair := range [][2][]trace.Record{{native, target}, {target, native}} {
		if err := CompareM3DeleteTraces(bytes.NewReader(encodeTrace(t, pair[0])),
			bytes.NewReader(encodeTrace(t, pair[1]))); err == nil {
			t.Fatal("delete policy accepted malformed selected Txn oneof")
		}
	}
}
