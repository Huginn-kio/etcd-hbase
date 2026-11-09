package compare

import (
	"bytes"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM3PutComparisonAllowsOnlyTheDeclaredNonRaftTerm(t *testing.T) {
	native := m3PutTrace(t, "native-etcd", 3)
	target := m3PutTrace(t, "target-hbase", 0)
	nativeBytes := encodeTrace(t, native)
	targetBytes := encodeTrace(t, target)
	if err := CompareM3PutTraces(bytes.NewReader(nativeBytes), bytes.NewReader(targetBytes)); err != nil {
		t.Fatalf("approved M3 term projection rejected: %v", err)
	}
	if err := CompareM3PutTraces(bytes.NewReader(targetBytes), bytes.NewReader(nativeBytes)); err != nil {
		t.Fatalf("reversed comparison rejected: %v", err)
	}
	if err := CompareTraces(bytes.NewReader(nativeBytes), bytes.NewReader(targetBytes)); err == nil {
		t.Fatal("M0 comparison unexpectedly allowed a different Raft term")
	}

	badTerm := m3PutTrace(t, "target-hbase", 3)
	if err := CompareM3PutTraces(bytes.NewReader(nativeBytes), bytes.NewReader(encodeTrace(t, badTerm))); err == nil {
		t.Fatal("nonzero target term was accepted")
	}
	badValue := m3PutTrace(t, "target-hbase", 0)
	badValue[1].Response = replace(badValue[1].Response, `"value":"AQ=="`, `"value":"Ag=="`)
	if err := CompareM3PutTraces(bytes.NewReader(nativeBytes), bytes.NewReader(encodeTrace(t, badValue))); err == nil {
		t.Fatal("term allowance hid a previous value mismatch")
	}
	if err := CompareM3PutTraces(bytes.NewReader(nativeBytes), bytes.NewReader(nativeBytes)); err == nil {
		t.Fatal("two native traces were accepted as native versus target evidence")
	}
}

func m3PutTrace(t *testing.T, scope string, term uint64) []trace.Record {
	t.Helper()
	request, err := trace.SnapshotProto(&etcdserverpb.PutRequest{Key: []byte{'k'}, PrevKv: true})
	if err != nil {
		t.Fatal(err)
	}
	var records []trace.Record
	for index, id := range []string{"native-baseline", "J30-update"} {
		response := &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{
			ClusterId: 101, MemberId: 202, Revision: int64(index + 2), RaftTerm: term,
		}}
		if index == 1 {
			response.PrevKv = &mvccpb.KeyValue{Key: []byte{'k'}, Value: []byte{1},
				CreateRevision: 2, ModRevision: 2, Version: 1}
		}
		snapshot, err := trace.SnapshotProto(response)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, trace.Record{Schema: trace.SchemaVersion, CaseID: id, Ordinal: 1,
			Method: "etcdserverpb.KV/Put", Request: request, Response: snapshot,
			EvidenceScope: scope, ElapsedNS: "1"})
	}
	return records
}
