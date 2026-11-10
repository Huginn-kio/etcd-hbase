package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/history"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unavailableServer struct {
	etcdserverpb.UnimplementedKVServer
	calls atomic.Int64
}

func TestRestartHandshakeReadsOnlyACompletedMarker(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "after-restart"), []byte("127.0.0.1:1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	endpoint, err := awaitRestart(context.Background(), directory)
	if err != nil || endpoint != "127.0.0.1:1234" {
		t.Fatalf("unexpected restart marker: %q, %v", endpoint, err)
	}
	if _, err := awaitRestart(context.Background(), directory); err == nil {
		t.Fatal("duplicate before-restart marker was accepted")
	}
}

func TestRestartHandshakeHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := awaitRestart(ctx, t.TempDir()); err == nil {
		t.Fatal("canceled restart wait was accepted")
	}
}

func TestEvidenceRecheckRejectsFixedSuccessAndLinkedFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "fake.json")
	if err := os.WriteFile(path, []byte(`{"schema":"etcd-hbase-j28-point-txn-history/v1","result":{"Verdict":"linearizable"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkEvidence(path); err == nil {
		t.Fatal("fixed successful result must not be accepted")
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := checkEvidence(link); err == nil {
		t.Fatal("linked evidence must not be accepted")
	}
}

func TestEvidenceRecheckAcceptsKnownHistoryAndRejectsTamperedValue(t *testing.T) {
	encode := func(message proto.Message) []byte {
		t.Helper()
		wire, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	key := []byte("model-key")
	first := &mvccpb.KeyValue{Key: key, Value: []byte("first"), CreateRevision: 2, ModRevision: 2, Version: 1}
	last := &mvccpb.KeyValue{Key: key, Value: []byte("last"), CreateRevision: 2, ModRevision: 3, Version: 2}
	put := &etcdserverpb.PutRequest{Key: key, Value: first.Value}
	change := &etcdserverpb.PutRequest{Key: key, Value: last.Value, PrevKv: true}
	txn := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{Key: key, Target: etcdserverpb.Compare_VERSION,
		Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: change}}}}
	response := &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{Revision: 3}, Responses: []*etcdserverpb.ResponseOp{
		{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 3}, PrevKv: first}}},
	}}
	operations := []history.Observation{
		{ID: 1, Worker: 1, Begin: 1, End: 2, Method: "Put", Request: encode(put), Response: encode(&etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}}), Code: "OK"},
		{ID: 2, Worker: 2, Begin: 3, End: 4, Method: "Txn", Request: encode(txn), Response: encode(response), Code: "OK"},
	}
	for i := 0; i < 8; i++ {
		operations = append(operations, history.Observation{ID: uint64(i + 3), Worker: 3,
			Begin: uint64(5 + 2*i), End: uint64(6 + 2*i), Method: "Range", Code: "OK",
			Request:  encode(&etcdserverpb.RangeRequest{Key: key}),
			Response: encode(&etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 3}, Kvs: []*mvccpb.KeyValue{last}, Count: 1})})
	}
	initial, err := history.NewKVState(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := history.Check(context.Background(), operations, history.KVModel{}, initial, 10000)
	if err != nil || result.Verdict != history.Linearizable {
		t.Fatalf("known history: %+v, %v", result, err)
	}
	evidence := historyEvidence{Schema: evidenceSchema, Schedule: evidenceSchedule, Seed: evidenceSeed,
		ModelScope: evidenceScope, RestartMode: "none", BaseRevision: 1, SuccessfulWrites: 2, Observations: operations, Result: result}
	path := filepath.Join(t.TempDir(), "history.json")
	write := func() {
		data, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if err := checkEvidence(path); err != nil {
		t.Fatal(err)
	}
	last.Value = []byte("tampered")
	evidence.Observations[9].Response = encode(&etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 3}, Kvs: []*mvccpb.KeyValue{last}, Count: 1})
	write()
	if err := checkEvidence(path); err == nil {
		t.Fatal("tampered observed value must fail recheck")
	}
}

func (server *unavailableServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	server.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "unavailable")
}

func TestHistoryClientDoesNotReplayMutableRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	backend := &unavailableServer{}
	etcdserverpb.RegisterKVServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	connection, err := newHistoryClient(client.Options{Endpoint: listener.Addr().String(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.RawKV(connection).Put(ctx, &etcdserverpb.PutRequest{Key: []byte("test")})
	if status.Code(err) != codes.Unavailable || backend.calls.Load() != 1 {
		t.Fatalf("unexpected result: code=%s calls=%d", status.Code(err), backend.calls.Load())
	}
}
