package history

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestRangeUsesApprovedPageCountAndByteOrder(t *testing.T) {
	values := []*mvccpb.KeyValue{
		{Key: []byte{'a', 0}, Value: []byte("one"), CreateRevision: 2, ModRevision: 2, Version: 1},
		{Key: []byte{'a', 0xff}, Value: []byte("two"), CreateRevision: 3, ModRevision: 3, Version: 1},
	}
	state, err := NewKVState(3, values)
	if err != nil {
		t.Fatal(err)
	}
	request := &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("b"), Limit: 1, SortOrder: etcdserverpb.RangeRequest_DESCEND, KeysOnly: true}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response := &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 3}, Kvs: []*mvccpb.KeyValue{{Key: []byte{'a', 0xff}, CreateRevision: 3, ModRevision: 3, Version: 1}}, Count: 1, More: true}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	operation := Observation{Method: "Range", Code: "OK", Request: requestBytes, Response: responseBytes}
	_, matches, err := (KVModel{}).Apply(state, operation)
	if err != nil || !matches {
		t.Fatalf("valid page rejected: %v", err)
	}
	response.Count = 2
	operation.Response, err = proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	_, matches, err = (KVModel{}).Apply(state, operation)
	if err != nil || matches {
		t.Fatalf("total Count must not match the approved page contract: %v", err)
	}
}

func TestRangeCountOnlyIgnoresRevisionFilter(t *testing.T) {
	values := []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 2, ModRevision: 2, Version: 1}}
	state, err := NewKVState(2, values)
	if err != nil {
		t.Fatal(err)
	}
	request := &etcdserverpb.RangeRequest{Key: []byte("a"), CountOnly: true, MinModRevision: 3}
	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := proto.Marshal(&etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}, Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, matches, err := (KVModel{}).Apply(state, Observation{Method: "Range", Code: "OK", Request: encoded, Response: response})
	if err != nil || !matches {
		t.Fatalf("existing key must count despite filter: %v", err)
	}
}
