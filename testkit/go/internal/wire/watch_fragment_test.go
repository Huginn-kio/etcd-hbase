package wire

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	fragmentWatchRevision    int64 = 42
	fragmentWatchID                = 1
	fragmentWatchEventCount        = 3
	fragmentWatchValueBytes        = 64 * 1024
	fragmentWatchBufferBytes       = 1 << 20
)

func TestClientv3ReassemblesFragmentsAndResumesIncompleteRevision(t *testing.T) {
	service := &fragmentWatchTestService{
		createRequests: make(chan etcdserverpb.WatchCreateRequest, 2),
	}
	listener := bufconn.Listen(fragmentWatchBufferBytes)
	grpcServer := grpc.NewServer()
	etcdserverpb.RegisterWatchServer(grpcServer, service)
	serverDone := make(chan error, 1)
	go func() { serverDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-serverDone
	})

	connection, err := grpc.NewClient(
		"passthrough:///watch-fragment-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gRPC client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	client := clientv3.NewCtxClient(context.Background())
	t.Cleanup(func() { _ = client.Close() })
	firstFragmentReceived := make(chan struct{})
	remoteWatchClient := &fragmentObservingWatchClient{
		WatchClient:           etcdserverpb.NewWatchClient(connection),
		firstFragmentReceived: firstFragmentReceived,
	}
	watcher := clientv3.NewWatchFromWatchClient(remoteWatchClient, client)
	t.Cleanup(func() { _ = watcher.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	responses := watcher.Watch(ctx, "fragmented-key",
		clientv3.WithRev(fragmentWatchRevision), clientv3.WithFragment())
	firstCreateRequest := receiveWatchCreateRequest(t, ctx, service.createRequests)
	if firstCreateRequest.StartRevision != fragmentWatchRevision || !firstCreateRequest.Fragment {
		t.Fatalf("initial Watch request = (start_revision=%d, fragment=%t), want (%d, true)",
			firstCreateRequest.StartRevision, firstCreateRequest.Fragment, fragmentWatchRevision)
	}

	select {
	case <-firstFragmentReceived:
	case <-ctx.Done():
		t.Fatalf("Go client did not receive the first incomplete fragment: %v", ctx.Err())
	}

	resumedCreateRequest := receiveWatchCreateRequest(t, ctx, service.createRequests)
	if resumedCreateRequest.StartRevision != fragmentWatchRevision || !resumedCreateRequest.Fragment {
		t.Fatalf("resumed Watch request = (start_revision=%d, fragment=%t), want (%d, true)",
			resumedCreateRequest.StartRevision, resumedCreateRequest.Fragment,
			fragmentWatchRevision)
	}

	response := receiveWatchResponse(t, ctx, responses)
	if response.Canceled {
		t.Fatalf("watch canceled before the revision completed: %v", response.Err())
	}
	if response.Created {
		t.Fatalf("unexpected Created notification: %+v", response)
	}
	if response.Header.Revision != fragmentWatchRevision {
		t.Fatalf("response header revision = %d, want %d",
			response.Header.Revision, fragmentWatchRevision)
	}
	if len(response.Events) != fragmentWatchEventCount {
		t.Fatalf("reassembled event count = %d, want all %d events from one revision",
			len(response.Events), fragmentWatchEventCount)
	}
	for index, event := range response.Events {
		wantKey := []byte{byte('a' + index)}
		if !bytes.Equal(event.Kv.Key, wantKey) {
			t.Errorf("event %d key = %q, want %q", index, event.Kv.Key, wantKey)
		}
		if event.Kv.ModRevision != fragmentWatchRevision {
			t.Errorf("event %d mod_revision = %d, want %d",
				index, event.Kv.ModRevision, fragmentWatchRevision)
		}
		wantValue := bytes.Repeat(wantKey, fragmentWatchValueBytes)
		if !bytes.Equal(event.Kv.Value, wantValue) {
			t.Errorf("event %d value changed while the Go client reassembled the revision", index)
		}
	}
}

type fragmentWatchTestService struct {
	etcdserverpb.UnimplementedWatchServer
	attempts       atomic.Int32
	createRequests chan etcdserverpb.WatchCreateRequest
}

func (service *fragmentWatchTestService) Watch(stream etcdserverpb.Watch_WatchServer) error {
	attempt := service.attempts.Add(1)
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	createRequest := request.GetCreateRequest()
	if createRequest == nil {
		return status.Error(codes.InvalidArgument, "expected a Watch create request")
	}
	select {
	case service.createRequests <- *createRequest:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if !createRequest.Fragment {
		return status.Error(codes.InvalidArgument, "client did not request fragment support")
	}
	if attempt > 2 {
		return status.Error(codes.Internal, "client opened an unexpected extra Watch stream")
	}

	if err := stream.Send(createdWatchResponse()); err != nil {
		return err
	}
	if attempt == 1 {
		if err := stream.Send(fragmentWatchResponse(watchTestEvent('a'), true)); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "interrupt after an incomplete revision")
	}

	events := []mvccpb.Event{
		watchTestEvent('a'),
		watchTestEvent('b'),
		watchTestEvent('c'),
	}
	for index, event := range events {
		if err := stream.Send(fragmentWatchResponse(event, index < len(events)-1)); err != nil {
			return err
		}
	}
	<-stream.Context().Done()
	return nil
}

type fragmentObservingWatchClient struct {
	etcdserverpb.WatchClient
	firstFragmentReceived chan struct{}
	firstFragmentOnce     sync.Once
}

func (client *fragmentObservingWatchClient) Watch(
	ctx context.Context, options ...grpc.CallOption,
) (etcdserverpb.Watch_WatchClient, error) {
	stream, err := client.WatchClient.Watch(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &fragmentObservingWatchStream{
		Watch_WatchClient: stream,
		onFragment: func() {
			client.firstFragmentOnce.Do(func() { close(client.firstFragmentReceived) })
		},
	}, nil
}

type fragmentObservingWatchStream struct {
	etcdserverpb.Watch_WatchClient
	onFragment func()
}

func (stream *fragmentObservingWatchStream) Recv() (*etcdserverpb.WatchResponse, error) {
	response, err := stream.Watch_WatchClient.Recv()
	if response != nil && response.Fragment {
		stream.onFragment()
	}
	return response, err
}

func createdWatchResponse() *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: fragmentWatchRevision},
		WatchId: fragmentWatchID,
		Created: true,
	}
}

func fragmentWatchResponse(event mvccpb.Event, hasMoreFragments bool) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:   &etcdserverpb.ResponseHeader{Revision: fragmentWatchRevision},
		WatchId:  fragmentWatchID,
		Events:   []*mvccpb.Event{&event},
		Fragment: hasMoreFragments,
	}
}

func watchTestEvent(key byte) mvccpb.Event {
	keyBytes := []byte{key}
	return mvccpb.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:            keyBytes,
			Value:          bytes.Repeat(keyBytes, fragmentWatchValueBytes),
			CreateRevision: 1,
			ModRevision:    fragmentWatchRevision,
			Version:        1,
		},
	}
}

func receiveWatchCreateRequest(
	t *testing.T, ctx context.Context, requests <-chan etcdserverpb.WatchCreateRequest,
) etcdserverpb.WatchCreateRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Watch create request: %v", ctx.Err())
		return etcdserverpb.WatchCreateRequest{}
	}
}

func receiveWatchResponse(
	t *testing.T, ctx context.Context, responses clientv3.WatchChan,
) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, open := <-responses:
		if !open {
			t.Fatal("watch response channel closed before a complete revision arrived")
		}
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for a complete Watch revision: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}
