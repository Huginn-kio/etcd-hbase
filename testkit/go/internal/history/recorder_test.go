package history

import (
	"context"
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type putClient struct {
	etcdserverpb.KVClient
	put func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)
}

func TestOversizedResponsePreservesRPCResultButInvalidatesHistory(t *testing.T) {
	recorder, _ := NewRecorder(1, 256)
	client := &putClient{put: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
		return &etcdserverpb.PutResponse{PrevKv: &mvccpb.KeyValue{Value: make([]byte, 1024)}}, nil
	}}
	response, err := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("k")})
	if err != nil || response == nil {
		t.Fatal("recording limits must not change the RPC result")
	}
	if _, err := recorder.Snapshot(); err == nil {
		t.Fatal("oversized response must invalidate history")
	}
}

func (client *putClient) Put(ctx context.Context, request *etcdserverpb.PutRequest, _ ...grpc.CallOption) (*etcdserverpb.PutResponse, error) {
	return client.put(ctx, request)
}

func TestRecorderRetainsActualRPCAndOrdersOverlappingCalls(t *testing.T) {
	recorder, err := NewRecorder(3, 4096)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	client := &putClient{put: func(_ context.Context, request *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
		if string(request.Key) == "first" {
			close(started)
			<-release
		}
		return &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 7}}, nil
	}}
	first := &etcdserverpb.PutRequest{Key: []byte("first"), Value: []byte("value")}
	done := make(chan error, 1)
	go func() { _, err := recorder.InvokeWorker(context.Background(), client, 1, first); done <- err }()
	<-started
	if _, err := recorder.Snapshot(); err == nil {
		t.Fatal("in-flight history must be incomplete")
	}
	if _, err := recorder.InvokeWorker(context.Background(), client, 2, &etcdserverpb.PutRequest{Key: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	first.Key[0] = 'X'
	records, err := recorder.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Worker != 1 || records[1].Worker != 2 || !(records[0].Begin < records[1].Begin && records[1].End < records[0].End) {
		t.Fatalf("unexpected intervals: %+v", records)
	}
	var captured etcdserverpb.PutRequest
	if err := proto.Unmarshal(records[0].Request, &captured); err != nil {
		t.Fatal(err)
	}
	if string(captured.Key) != "first" || records[0].Code != "OK" {
		t.Fatal("request or status was not captured")
	}
	records[0].Request[0] ^= 0xff
	again, err := recorder.Snapshot()
	if err != nil || proto.Unmarshal(again[0].Request, &captured) != nil {
		t.Fatal("snapshot aliases recorder memory")
	}
}

func TestRecorderRejectsConcurrentCallBeforeDispatchWhenCapacityIsFull(t *testing.T) {
	recorder, _ := NewRecorder(1, 256)
	started, release := make(chan struct{}), make(chan struct{})
	client := &putClient{put: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
		close(started)
		<-release
		return &etcdserverpb.PutResponse{}, nil
	}}
	done := make(chan error, 1)
	go func() {
		_, err := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("first")})
		done <- err
	}()
	<-started
	if _, err := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("second")}); err == nil {
		t.Fatal("expected capacity rejection before dispatch")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Snapshot(); err == nil {
		t.Fatal("truncated history must not pass")
	}
}

func TestRecorderPreservesUnknownFailureAndRejectsTruncatedHistory(t *testing.T) {
	recorder, _ := NewRecorder(1, 256)
	client := &putClient{put: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
		return nil, status.Error(codes.Unavailable, "private backend detail")
	}}
	_, callErr := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("k")})
	if status.Code(callErr) != codes.Unavailable {
		t.Fatal("RPC error changed")
	}
	records, err := recorder.Snapshot()
	if err != nil || len(records) != 1 || records[0].Code != "Unavailable" || records[0].Response != nil {
		t.Fatal("unknown result was not preserved")
	}
	if _, err := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("x")}); err == nil {
		t.Fatal("expected call limit")
	}
	if _, err := recorder.Snapshot(); err == nil {
		t.Fatal("truncated history must not pass")
	}
}

func TestRecorderRejectsNilSuccessAndClassifiesContextCancellation(t *testing.T) {
	recorder, _ := NewRecorder(2, 256)
	client := &putClient{put: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) { return nil, nil }}
	if _, err := recorder.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Snapshot(); err == nil {
		t.Fatal("nil successful response must invalidate history")
	}
	cancelled, _ := NewRecorder(1, 256)
	client.put = func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
		return nil, context.Canceled
	}
	_, err := cancelled.Invoke(context.Background(), client, &etcdserverpb.PutRequest{Key: []byte("k")})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation was not preserved")
	}
	records, err := cancelled.Snapshot()
	if err != nil || records[0].Code != "Canceled" {
		t.Fatal("cancellation was not classified")
	}
}
