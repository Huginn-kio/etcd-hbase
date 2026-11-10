package compare

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM3RangeComparisonKeepsTxnCountOneofsStrict(t *testing.T) {
	for _, nested := range []bool{false, true} {
		native, target := malformedCountTraces(t, nested, true)
		if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)),
			bytes.NewReader(encodeTrace(t, target))); err != nil {
			t.Fatalf("valid Count allowance rejected by range policy (nested=%t): %v", nested, err)
		}
	}
	cases := []struct{ nested, request, strictSibling bool }{
		{false, false, false}, {false, false, true}, {false, true, false}, {false, true, true},
		{true, false, false}, {true, false, true}, {true, true, false}, {true, true, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("nested=%t/request=%t/strictSibling=%t", tc.nested, tc.request, tc.strictSibling), func(t *testing.T) {
			assertRangePolicyRejectsMalformed(t, tc.nested, tc.request, tc.strictSibling)
		})
	}
}

func assertRangePolicyRejectsMalformed(t *testing.T, nested, request, strictSibling bool) {
	t.Helper()
	native, target := malformedCountTraces(t, nested, strictSibling)
	mutateCountOneof(native, request, strictSibling)
	mutateCountOneof(target, request, strictSibling)
	for _, pair := range [][2][]trace.Record{{native, target}, {target, native}} {
		if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, pair[0])),
			bytes.NewReader(encodeTrace(t, pair[1]))); err == nil {
			t.Fatal("range policy accepted malformed selected Txn oneof")
		}
	}
}

func TestM3RangeComparisonKeepsWireFieldsAndMethodsStrict(t *testing.T) {
	request := &etcdserverpb.RangeRequest{Key: []byte("k")}
	value := &mvccpb.KeyValue{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}
	left := encodeTrace(t, m3RangeTrace(t, "native-etcd", 3, request, 1, false, value))
	target := m3RangeTrace(t, "target-hbase", 0, request, 1, false, value)
	if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, target))); err != nil {
		t.Fatalf("matching point Range differs: %v", err)
	}
	if err := CompareM3DeleteTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, target))); err == nil {
		t.Fatal("DeleteRange policy accepted a standalone Range")
	}
	badValue := *value
	badValue.Value = []byte{2}
	for _, bad := range [][]trace.Record{
		m3RangeTrace(t, "target-hbase", 0, request, 1, false, &badValue),
		m3RangeTrace(t, "target-hbase", 0, request, 1, true, value),
		m3RangeTrace(t, "target-hbase", 0, request, 0, false, value),
	} {
		if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
			t.Fatal("Range comparison hid a value, More, or point Count mismatch")
		}
	}
	badHeader := m3RangeTrace(t, "target-hbase", 0, request, 1, false, value)
	badHeader[1].Response = replace(badHeader[1].Response, `"revision":"2"`, `"revision":"3"`)
	if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, badHeader))); err == nil {
		t.Fatal("Range comparison hid a response header revision mismatch")
	}
	unknown := m3RangeTrace(t, "target-hbase", 0, request, 1, false, value)
	unknown[1].Method = "etcdserverpb.KV/Unknown"
	if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, unknown))); err == nil {
		t.Fatal("Range policy accepted an unregistered method")
	}
}

func TestM3RangeComparisonAllowsOnlyFilteredResultCounts(t *testing.T) {
	for _, request := range []*etcdserverpb.RangeRequest{
		{Key: []byte("k"), MinModRevision: 3},
		{Key: []byte("k"), Limit: 1, MinModRevision: 3},
		{Key: []byte("k"), MaxModRevision: -1},
		{Key: []byte("k"), MinCreateRevision: 3},
		{Key: []byte("k"), MaxCreateRevision: -1},
		{Key: []byte("a"), RangeEnd: []byte("z"), MinModRevision: 3},
		{Key: []byte("a"), RangeEnd: []byte("z"), Limit: -1, MaxModRevision: -1},
	} {
		native := m3RangeTrace(t, "native-etcd", 3, request, 1, false)
		target := m3RangeTrace(t, "target-hbase", 0, request, 0, false)
		for _, pair := range [][2][]trace.Record{{native, target}, {target, native}} {
			if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, pair[0])), bytes.NewReader(encodeTrace(t, pair[1]))); err != nil {
				t.Fatalf("filtered result Count rejected for request %v: %v", request, err)
			}
		}
		bad := m3RangeTrace(t, "target-hbase", 0, request, 1, false)
		if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)), bytes.NewReader(encodeTrace(t, bad))); err == nil {
			t.Fatalf("target Count not equal to result count was accepted for request %v", request)
		}
	}
	for _, request := range []*etcdserverpb.RangeRequest{
		{Key: []byte("k")},
		{Key: []byte("a"), RangeEnd: []byte("z")},
		{Key: []byte("k"), CountOnly: true, MinModRevision: 3},
	} {
		native := m3RangeTrace(t, "native-etcd", 3, request, 1, false)
		target := m3RangeTrace(t, "target-hbase", 0, request, 0, false)
		if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)), bytes.NewReader(encodeTrace(t, target))); err == nil {
			t.Fatalf("unfiltered or CountOnly mismatch was hidden for request %v", request)
		}
	}
	point := &etcdserverpb.RangeRequest{Key: []byte("k"), MinModRevision: 3}
	for _, count := range []int64{2, -1} {
		native := m3RangeTrace(t, "native-etcd", 3, point, count, false)
		target := m3RangeTrace(t, "target-hbase", 0, point, 0, false)
		if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)), bytes.NewReader(encodeTrace(t, target))); err == nil {
			t.Fatalf("impossible native point Count %d was accepted", count)
		}
	}
}

func TestM3RangeFilteredCountRejectsMalformedFilterAndImpossibleMore(t *testing.T) {
	request := &etcdserverpb.RangeRequest{Key: []byte("k"), MinModRevision: 3}
	native := m3RangeTrace(t, "native-etcd", 3, request, 1, false)
	target := m3RangeTrace(t, "target-hbase", 0, request, 0, false)
	for _, records := range [][]trace.Record{native, target} {
		records[1].Request = replace(records[1].Request, `"minModRevision":"3"`, `"minModRevision":"03"`)
	}
	if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)), bytes.NewReader(encodeTrace(t, target))); err == nil {
		t.Fatal("malformed revision filter was accepted")
	}
	native = m3RangeTrace(t, "native-etcd", 3, request, 1, true)
	target = m3RangeTrace(t, "target-hbase", 0, request, 0, true)
	if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, native)), bytes.NewReader(encodeTrace(t, target))); err == nil {
		t.Fatal("point Range with More=true was accepted")
	}
}

func TestM3RangeComparisonAppliesOnlyExistingPageCountAllowance(t *testing.T) {
	request := &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}
	value := &mvccpb.KeyValue{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}
	native := m3RangeTrace(t, "native-etcd", 3, request, 2, true, value)
	target := m3RangeTrace(t, "target-hbase", 0, request, 1, true, value)
	left := encodeTrace(t, native)
	if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, target))); err != nil {
		t.Fatalf("approved page Count allowance rejected: %v", err)
	}
	bad := m3RangeTrace(t, "target-hbase", 0, request, 1, false, value)
	if err := CompareM3RangeTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
		t.Fatal("page Count allowance hid a More mismatch")
	}
	leftTxn := m3CountTxnTrace(t, "native-etcd", 3, 2, 1, true)
	rightTxn := m3CountTxnTrace(t, "target-hbase", 0, 1, 1, true)
	if err := CompareM3RangeTraces(bytes.NewReader(encodeTrace(t, leftTxn)), bytes.NewReader(encodeTrace(t, rightTxn))); err != nil {
		t.Fatalf("registered Txn Range policy was not preserved: %v", err)
	}
}

func m3RangeTrace(t *testing.T, scope string, term uint64, request *etcdserverpb.RangeRequest,
	count int64, more bool, values ...*mvccpb.KeyValue) []trace.Record {
	t.Helper()
	records := m3TxnTrace(t, scope, term)
	encodedRequest, err := trace.SnapshotProto(request)
	if err != nil {
		t.Fatal(err)
	}
	encodedResponse, err := trace.SnapshotProto(&etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 2, RaftTerm: term},
		Count:  count, More: more, Kvs: values,
	})
	if err != nil {
		t.Fatal(err)
	}
	records[1].Method = "etcdserverpb.KV/Range"
	records[1].Request, records[1].Response = encodedRequest, encodedResponse
	return records
}
