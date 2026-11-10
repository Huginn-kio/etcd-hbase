package history

import (
	"context"
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type integerModel struct{}

func (integerModel) CanOmit(_ any, operation Observation) (bool, error) {
	if operation.Code == "DeadlineExceeded" {
		return true, nil
	}
	return false, errors.New("unclassified failure")
}

func (integerModel) Apply(state any, operation Observation) (any, bool, error) {
	value := state.(byte)
	switch operation.Method {
	case "put":
		return operation.Request[0], true, nil
	case "get":
		return value, len(operation.Response) == 1 && operation.Response[0] == value, nil
	default:
		return nil, false, errors.New("unsupported operation")
	}
}

func TestCheckHonorsKnownRealTimeOrder(t *testing.T) {
	operations := []Observation{
		{ID: 1, Method: "put", Begin: 1, End: 2, Request: []byte{2}, Code: "OK"},
		{ID: 2, Method: "get", Begin: 3, End: 4, Response: []byte{1}, Code: "OK"},
	}
	result, err := Check(context.Background(), operations, integerModel{}, byte(1), 100)
	if err != nil || result.Verdict != Invalid {
		t.Fatalf("got %+v, %v", result, err)
	}
	operations[1].Response[0] = 2
	result, err = Check(context.Background(), operations, integerModel{}, byte(1), 100)
	if err != nil || result.Verdict != Linearizable {
		t.Fatalf("got %+v, %v", result, err)
	}
}

func TestUnknownWriteCanTakeEffectAfterErrorReturn(t *testing.T) {
	operations := []Observation{
		{ID: 1, Method: "put", Begin: 1, End: 2, Request: []byte{2}, Code: "DeadlineExceeded"},
		{ID: 2, Method: "get", Begin: 3, End: 4, Response: []byte{1}, Code: "OK"},
		{ID: 3, Method: "get", Begin: 5, End: 6, Response: []byte{2}, Code: "OK"},
	}
	result, err := Check(context.Background(), operations, integerModel{}, byte(1), 100)
	if err != nil || result.Verdict != Linearizable {
		t.Fatalf("got %+v, %v", result, err)
	}
}

func TestIncompleteSearchCannotPassOrReject(t *testing.T) {
	operations := []Observation{{ID: 1, Method: "unsupported", Begin: 1, End: 2, Code: "OK"}}
	result, err := Check(context.Background(), operations, integerModel{}, byte(1), 100)
	if err != nil || result.Verdict != Inconclusive {
		t.Fatalf("got %+v, %v", result, err)
	}
	operations[0].Method, operations[0].Request = "put", []byte{2}
	result, err = Check(context.Background(), operations, integerModel{}, byte(1), 1)
	if err != nil || result.Verdict != Inconclusive {
		t.Fatalf("got %+v, %v", result, err)
	}
}

func TestInvalidStatusCannotHideASuccessfulOperation(t *testing.T) {
	operations := []Observation{{ID: 1, Method: "put", Begin: 1, End: 2, Request: []byte{2}, Code: "ok"}}
	if _, err := Check(context.Background(), operations, integerModel{}, byte(1), 100); err == nil {
		t.Fatal("noncanonical status must not be treated as an unknown failure")
	}
}

func TestUnknownUnsupportedOperationRemainsInconclusive(t *testing.T) {
	request, err := proto.Marshal(&etcdserverpb.TxnRequest{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	operations := []Observation{{ID: 1, Method: "Txn", Begin: 1, End: 2, Request: request, Code: "ResourceExhausted"}}
	result, err := Check(context.Background(), operations, KVModel{}, state, 100)
	if err != nil || result.Verdict != Inconclusive {
		t.Fatalf("got %+v, %v", result, err)
	}
}

func TestUnknownWireFieldsRemainInconclusive(t *testing.T) {
	request, err := proto.Marshal(&etcdserverpb.PutRequest{Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	// Field 99 is deliberately unknown to the pinned etcd protocol.
	request = append(request, 0x98, 0x06, 0x01)
	state, err := NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	operations := []Observation{{ID: 1, Method: "Put", Begin: 1, End: 2, Request: request, Code: "Unavailable"}}
	result, err := Check(context.Background(), operations, KVModel{}, state, 100)
	if err != nil || result.Verdict != Inconclusive {
		t.Fatalf("unknown field: %+v, %v", result, err)
	}
}

func TestUnknownTxnWithConflictingSiblingWritesIsInconclusive(t *testing.T) {
	put := func() *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("k")}}}
	}
	child := func() *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{put()}}}}
	}
	request, err := proto.Marshal(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{child(), child()}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	operations := []Observation{{ID: 1, Method: "Txn", Begin: 1, End: 2, Request: request, Code: "Unavailable"}}
	result, err := Check(context.Background(), operations, KVModel{}, state, 100)
	if err != nil || result.Verdict != Inconclusive {
		t.Fatalf("conflicting writes: %+v, %v", result, err)
	}
}
