package compare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM3TxnComparisonKeepsChildResponsesAndMethodsStrict(t *testing.T) {
	native := m3TxnTrace(t, "native-etcd", 3)
	target := m3TxnTrace(t, "target-hbase", 0)
	left, right := encodeTrace(t, native), encodeTrace(t, target)
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(right)); err != nil {
		t.Fatalf("valid transaction traces differ: %v", err)
	}
	if err := CompareM3PutTraces(bytes.NewReader(left), bytes.NewReader(right)); err == nil {
		t.Fatal("Put-only policy accepted a transaction")
	}
	for _, replacements := range [][2]string{
		{`"count":"1"`, `"count":"2"`},
		{`"value":"AQ=="`, `"value":"Ag=="`},
		{`"version":"1"`, `"version":"2"`},
	} {
		bad := m3TxnTrace(t, "target-hbase", 0)
		bad[1].Response = replace(bad[1].Response, replacements[0], replacements[1])
		if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
			t.Fatalf("nested transaction mismatch %s was ignored", replacements[0])
		}
	}
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(left)); err == nil {
		t.Fatal("two native traces were accepted as native and target evidence")
	}
}

func TestM3TxnCountValidationDoesNotPartiallyMutateOnMalformedSibling(t *testing.T) {
	left, err := normalize(m3CountTxnTrace(t, "native-etcd", 3, 2, 1, true))
	if err != nil {
		t.Fatal(err)
	}
	right, err := normalize(m3CountTxnTrace(t, "target-hbase", 0, 1, 1, true))
	if err != nil {
		t.Fatal(err)
	}
	right[1]["evidence_scope"] = left[1]["evidence_scope"]
	for _, record := range []map[string]any{left[1], right[1]} {
		request := record["request"].(map[string]any)["fields"].(map[string]any)
		operations := request["success"].([]any)
		operations[1].(map[string]any)["fields"] = "malformed"
	}
	beforeLeft, _ := json.Marshal(left[1])
	beforeRight, _ := json.Marshal(right[1])
	applied, equal := equalTxnWithCountAllowance(left[1], right[1], "native-etcd")
	afterLeft, _ := json.Marshal(left[1])
	afterRight, _ := json.Marshal(right[1])
	if applied || equal || !bytes.Equal(beforeLeft, afterLeft) || !bytes.Equal(beforeRight, afterRight) {
		t.Fatal("invalid sibling partially changed the comparison input")
	}
}

func TestM3TxnCountAllowanceMatchesOnlySelectedRangePosition(t *testing.T) {
	native := m3CountTxnTrace(t, "native-etcd", 3, 2, 1, true)
	target := m3CountTxnTrace(t, "target-hbase", 0, 1, 1, true)
	left := encodeTrace(t, native)
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, target))); err != nil {
		t.Fatalf("selected limited Range Count allowance was rejected: %v", err)
	}
	for _, bad := range [][]trace.Record{
		m3CountTxnTrace(t, "target-hbase", 0, 0, 1, true),
		m3CountTxnTrace(t, "target-hbase", 0, 1, 2, true),
		m3CountTxnTrace(t, "target-hbase", 0, 1, 1, false),
	} {
		if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
			t.Fatal("allowance hid an invalid Count, strict sibling, or branch mismatch")
		}
	}
	bad := m3CountTxnTrace(t, "target-hbase", 0, 1, 1, true)
	bad[1].Response = replace(bad[1].Response, `"response":"response_range"`, `"response":"response_put"`)
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
		t.Fatal("allowance hid a mismatched response oneof")
	}
}

func TestM3NestedTxnCountAllowanceIsRestrictedToItsSelectedRequestPath(t *testing.T) {
	native := m3NestedCountTrace(t, "native-etcd", 3, 2, 1, true)
	target := m3NestedCountTrace(t, "target-hbase", 0, 1, 1, true)
	left := encodeTrace(t, native)
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, target))); err != nil {
		t.Fatalf("selected nested limited Range Count allowance was rejected: %v", err)
	}
	if err := CompareM3TxnTraces(bytes.NewReader(encodeTrace(t, target)), bytes.NewReader(left)); err != nil {
		t.Fatalf("reversing native and target traces changed the nested Count policy: %v", err)
	}
	for _, bad := range [][]trace.Record{
		m3NestedCountTrace(t, "target-hbase", 0, 0, 1, true),
		m3NestedCountTrace(t, "target-hbase", 0, 2, 1, true),
		m3NestedCountTrace(t, "target-hbase", 0, 1, 2, true),
		m3NestedCountTrace(t, "target-hbase", 0, 1, 1, false),
	} {
		if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
			t.Fatal("nested allowance hid an invalid count, strict sibling, or branch mismatch")
		}
	}
	bad := m3NestedCountTrace(t, "target-hbase", 0, 1, 1, true)
	bad[1].Response = replace(bad[1].Response, `"response":"response_txn"`, `"response":"response_put"`)
	if err := CompareM3TxnTraces(bytes.NewReader(left), bytes.NewReader(encodeTrace(t, bad))); err == nil {
		t.Fatal("nested allowance hid a response oneof mismatch")
	}
}

func m3NestedCountTrace(t *testing.T, scope string, term uint64, limited, point int64, succeeded bool) []trace.Record {
	t.Helper()
	records := m3TxnTrace(t, scope, term)
	limitedRequest := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}}}
	pointRequest := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("k")}}}
	childRequest := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{limitedRequest, pointRequest},
		Failure: []*etcdserverpb.RequestOp{pointRequest}}
	request, err := trace.SnapshotProto(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: childRequest},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	responseOp := func(count int64) *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{
			ResponseRange: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}, Count: count,
				Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}}}}}
	}
	childResponse := &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{}, Succeeded: succeeded,
		Responses: []*etcdserverpb.ResponseOp{responseOp(limited), responseOp(point)}}
	response, err := trace.SnapshotProto(&etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 2, RaftTerm: term}, Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: childResponse}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	records[1].Request, records[1].Response = request, response
	return records
}

func m3CountTxnTrace(t *testing.T, scope string, term uint64, limited, point int64, succeeded bool) []trace.Record {
	t.Helper()
	records := m3TxnTrace(t, scope, term)
	limitedRequest := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}}}
	pointRequest := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("k")}}}
	request, err := trace.SnapshotProto(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{limitedRequest, pointRequest}, Failure: []*etcdserverpb.RequestOp{pointRequest}})
	if err != nil {
		t.Fatal(err)
	}
	responseOp := func(count int64) *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{
			ResponseRange: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}, Count: count,
				Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}}}}}
	}
	response, err := trace.SnapshotProto(&etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 2, RaftTerm: term},
		Succeeded: succeeded, Responses: []*etcdserverpb.ResponseOp{responseOp(limited), responseOp(point)},
	})
	if err != nil {
		t.Fatal(err)
	}
	records[1].Request, records[1].Response = request, response
	return records
}

func m3TxnTrace(t *testing.T, scope string, term uint64) []trace.Record {
	t.Helper()
	records := m3PutTrace(t, scope, term)[:1]
	request, err := trace.SnapshotProto(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte("k")}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := trace.SnapshotProto(&etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 101, MemberId: 202, Revision: 2, RaftTerm: term}, Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{
			ResponseRange: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}, Count: 1,
				Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte{1}, CreateRevision: 2, ModRevision: 2, Version: 1}}},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return append(records, trace.Record{Schema: trace.SchemaVersion, CaseID: "J31-client-read", Ordinal: 1,
		Method: "etcdserverpb.KV/Txn", Request: request, Response: response, EvidenceScope: scope, ElapsedNS: "1"})
}

func TestM3TxnCountAllowanceRejectsMatchingMalformedOneof(t *testing.T) {
	runMalformedCountMatrix(t, false)
}

func TestM3TxnCountAllowanceRejectsMalformedStrictSibling(t *testing.T) {
	runMalformedCountMatrix(t, true)
}

func runMalformedCountMatrix(t *testing.T, strictSibling bool) {
	t.Helper()
	for _, nested := range []bool{false, true} {
		for _, request := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested=%t/request=%t", nested, request), func(t *testing.T) {
				assertMalformedCountRejected(t, nested, request, strictSibling)
			})
		}
	}
}

func assertMalformedCountRejected(t *testing.T, nested, request, strictSibling bool) {
	t.Helper()
	native, target := malformedCountTraces(t, nested, strictSibling)
	mutateCountOneof(native, request, strictSibling)
	mutateCountOneof(target, request, strictSibling)
	assertCountTraceRejected(t, native, target)
	if strictSibling {
		assertCountTraceRejected(t, target, native)
	}
}

func malformedCountTraces(t *testing.T, nested, strictSibling bool) ([]trace.Record, []trace.Record) {
	t.Helper()
	nativeCount := int64(1)
	if strictSibling {
		nativeCount = 2
	}
	if nested {
		return m3NestedCountTrace(t, "native-etcd", 3, nativeCount, 1, true),
			m3NestedCountTrace(t, "target-hbase", 0, 1, 1, true)
	}
	return m3CountTxnTrace(t, "native-etcd", 3, nativeCount, 1, true),
		m3CountTxnTrace(t, "target-hbase", 0, 1, 1, true)
}

func mutateCountOneof(records []trace.Record, request, strictSibling bool) {
	occurrence := 1
	if request {
		occurrence = 2
		if strictSibling {
			occurrence = 3
		}
		records[1].Request = replaceNth(records[1].Request, `"request":"request_range"`, `"request":"request_put"`, occurrence)
		return
	}
	if strictSibling {
		occurrence = 2
	}
	records[1].Response = replaceNth(records[1].Response, `"response":"response_range"`, `"response":"response_put"`, occurrence)
}

func assertCountTraceRejected(t *testing.T, left, right []trace.Record) {
	t.Helper()
	if err := CompareM3TxnTraces(bytes.NewReader(encodeTrace(t, left)), bytes.NewReader(encodeTrace(t, right))); err == nil {
		t.Fatal("malformed oneof was accepted during Count comparison")
	}
}

func replaceNth(raw json.RawMessage, old, replacement string, occurrence int) json.RawMessage {
	text, offset := string(raw), 0
	for i := 0; i < occurrence; i++ {
		index := strings.Index(text[offset:], old)
		if index < 0 {
			return raw
		}
		offset += index
		if i == occurrence-1 {
			return json.RawMessage(text[:offset] + replacement + text[offset+len(old):])
		}
		offset += len(old)
	}
	return raw
}
