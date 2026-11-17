package wire

import (
	"bytes"
	"context"
	"net"
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
	fragmentWatchValueBytes        = 64 * 1024
	fragmentWatchBufferBytes       = 1 << 20
	fragmentWatchID          int64 = 1
	fragmentWatchResumedID   int64 = 2
	multiWatchFirstID        int64 = 11
	multiWatchSecondID       int64 = 22
	multiWatchFirstResumeID  int64 = 33
	multiWatchSecondResumeID int64 = 44
)

type fragmentWatchRequestObservation struct {
	attempt int32
	request etcdserverpb.WatchCreateRequest
}

type fragmentWatchObservation struct {
	watchID  int64
	revision int64
	more     bool
}

type expectedWatchEvent struct {
	key    string
	marker byte
}

type fragmentWatchHandler func(
	*fragmentWatchTestService, etcdserverpb.Watch_WatchServer, int32,
) error

func TestClientv3ResumesAtIncompleteRevisionAfterFragmentDisconnect(t *testing.T) {
	for _, scenario := range []struct {
		name                      string
		fragmentsBeforeDisconnect int
	}{
		{name: "after-first-fragment", fragmentsBeforeDisconnect: 1},
		{name: "after-middle-fragment", fragmentsBeforeDisconnect: 2},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			runIncompleteRevisionResumeScenario(t, scenario.fragmentsBeforeDisconnect)
		})
	}
}

func runIncompleteRevisionResumeScenario(t *testing.T, fragmentsBeforeDisconnect int) {
	t.Helper()
	replayRevision := make(chan struct{})
	service := &fragmentWatchTestService{
		createRequests: make(chan fragmentWatchRequestObservation, 4),
	}
	service.handlers = []fragmentWatchHandler{
		func(service *fragmentWatchTestService, stream etcdserverpb.Watch_WatchServer,
			attempt int32) error {
			if _, err := service.createWatcher(stream, attempt, fragmentWatchID); err != nil {
				return err
			}
			events := []mvccpb.Event{
				watchTestEventForKey("fragmented-a", 'a', fragmentWatchRevision),
				watchTestEventForKey("fragmented-b", 'b', fragmentWatchRevision),
			}
			for _, event := range events[:fragmentsBeforeDisconnect] {
				if err := stream.Send(fragmentWatchResponse(
					fragmentWatchID, fragmentWatchRevision, event, true)); err != nil {
					return err
				}
			}
			return status.Error(codes.Unavailable,
				"interrupt before the revision's final fragment")
		},
		func(service *fragmentWatchTestService, stream etcdserverpb.Watch_WatchServer,
			attempt int32) error {
			if _, err := service.createWatcher(stream, attempt, fragmentWatchResumedID); err != nil {
				return err
			}
			select {
			case <-replayRevision:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			events := []mvccpb.Event{
				watchTestEventForKey("fragmented-a", 'a', fragmentWatchRevision),
				watchTestEventForKey("fragmented-b", 'b', fragmentWatchRevision),
				watchTestEventForKey("fragmented-c", 'c', fragmentWatchRevision),
			}
			for index, event := range events {
				if err := stream.Send(fragmentWatchResponse(
					fragmentWatchResumedID, fragmentWatchRevision, event,
					index < len(events)-1)); err != nil {
					return err
				}
			}
			<-stream.Context().Done()
			return nil
		},
	}
	fixture := newFragmentWatchClientFixture(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	responses := fixture.watcher.Watch(ctx, "fragmented-", clientv3.WithRev(
		fragmentWatchRevision), clientv3.WithPrefix(), clientv3.WithFragment())
	assertWatchCreateRequests(t, ctx, service.createRequests, 1, map[string]int64{
		"fragmented-": fragmentWatchRevision,
	})

	for fragmentIndex := 0; fragmentIndex < fragmentsBeforeDisconnect; fragmentIndex++ {
		observation := receiveFragmentObservation(t, ctx, fixture.fragments)
		if observation.watchID != fragmentWatchID ||
			observation.revision != fragmentWatchRevision || !observation.more {
			t.Fatalf("incomplete fragment %d = %+v", fragmentIndex, observation)
		}
	}

	assertWatchCreateRequests(t, ctx, service.createRequests, 2, map[string]int64{
		"fragmented-": fragmentWatchRevision,
	})
	select {
	case response, open := <-responses:
		if !open {
			t.Fatal("watch closed before the revision was replayed")
		}
		t.Fatalf("client exposed an incomplete revision before its final fragment: %+v",
			response)
	default:
	}

	close(replayRevision)
	assertWatchResponse(t, receiveWatchResponse(t, ctx, responses), fragmentWatchRevision,
		expectedWatchEvent{key: "fragmented-a", marker: 'a'},
		expectedWatchEvent{key: "fragmented-b", marker: 'b'},
		expectedWatchEvent{key: "fragmented-c", marker: 'c'})
}

func TestClientv3ReassemblesMultipleWatchIDsAndResumesAfterFinalFragment(t *testing.T) {
	service := &fragmentWatchTestService{
		createRequests: make(chan fragmentWatchRequestObservation, 8),
	}
	service.handlers = []fragmentWatchHandler{
		func(service *fragmentWatchTestService, stream etcdserverpb.Watch_WatchServer,
			attempt int32) error {
			if _, err := service.createWatcher(stream, attempt, multiWatchFirstID); err != nil {
				return err
			}
			if _, err := service.createWatcher(stream, attempt, multiWatchSecondID); err != nil {
				return err
			}

			firstWatchEvents := []mvccpb.Event{
				watchTestEventForKey("watch-a/one", 'a', fragmentWatchRevision),
				watchTestEventForKey("watch-a/two", 'b', fragmentWatchRevision),
				watchTestEventForKey("watch-a/three", 'c', fragmentWatchRevision),
			}
			for index, event := range firstWatchEvents {
				if err := stream.Send(fragmentWatchResponse(
					multiWatchFirstID, fragmentWatchRevision, event,
					index < len(firstWatchEvents)-1)); err != nil {
					return err
				}
			}
			secondWatchEvent := watchTestEventForKey(
				"watch-b/one", 'x', fragmentWatchRevision)
			if err := stream.Send(fragmentWatchResponse(
				multiWatchSecondID, fragmentWatchRevision, secondWatchEvent, false)); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "disconnect after complete watch revisions")
		},
		func(service *fragmentWatchTestService, stream etcdserverpb.Watch_WatchServer,
			attempt int32) error {
			resumedWatchIDs := make(map[string]int64, 2)
			for _, watchID := range []int64{
				multiWatchFirstResumeID,
				multiWatchSecondResumeID,
			} {
				request, err := service.createWatcher(stream, attempt, watchID)
				if err != nil {
					return err
				}
				resumedWatchIDs[string(request.Key)] = watchID
			}

			resumedEvents := map[string]expectedWatchEvent{
				"watch-a/": {key: "watch-a/after", marker: 'd'},
				"watch-b/": {key: "watch-b/after", marker: 'y'},
			}
			for key, expected := range resumedEvents {
				watchID, ok := resumedWatchIDs[key]
				if !ok {
					return status.Errorf(codes.InvalidArgument,
						"unexpected resumed watch key %q", key)
				}
				event := watchTestEventForKey(expected.key, expected.marker,
					fragmentWatchRevision+1)
				if err := stream.Send(fragmentWatchResponse(
					watchID, fragmentWatchRevision+1, event, false)); err != nil {
					return err
				}
			}
			<-stream.Context().Done()
			return nil
		},
	}
	fixture := newFragmentWatchClientFixture(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	firstWatch := fixture.watcher.Watch(ctx, "watch-a/", clientv3.WithRev(
		fragmentWatchRevision), clientv3.WithPrefix(), clientv3.WithFragment())
	secondWatch := fixture.watcher.Watch(ctx, "watch-b/", clientv3.WithRev(
		fragmentWatchRevision), clientv3.WithPrefix(), clientv3.WithFragment())

	expectedRequests := map[string]int64{
		"watch-a/": fragmentWatchRevision,
		"watch-b/": fragmentWatchRevision,
	}
	assertWatchCreateRequests(t, ctx, service.createRequests, 1, expectedRequests)

	assertWatchResponse(t, receiveWatchResponse(t, ctx, firstWatch), fragmentWatchRevision,
		expectedWatchEvent{key: "watch-a/one", marker: 'a'},
		expectedWatchEvent{key: "watch-a/two", marker: 'b'},
		expectedWatchEvent{key: "watch-a/three", marker: 'c'})
	assertWatchResponse(t, receiveWatchResponse(t, ctx, secondWatch), fragmentWatchRevision,
		expectedWatchEvent{key: "watch-b/one", marker: 'x'})

	assertWatchCreateRequests(t, ctx, service.createRequests, 2, map[string]int64{
		"watch-a/": fragmentWatchRevision + 1,
		"watch-b/": fragmentWatchRevision + 1,
	})

	assertWatchResponse(t, receiveWatchResponse(t, ctx, firstWatch),
		fragmentWatchRevision+1,
		expectedWatchEvent{key: "watch-a/after", marker: 'd'})
	assertWatchResponse(t, receiveWatchResponse(t, ctx, secondWatch),
		fragmentWatchRevision+1,
		expectedWatchEvent{key: "watch-b/after", marker: 'y'})
}

type fragmentWatchTestService struct {
	etcdserverpb.UnimplementedWatchServer
	attempts       atomic.Int32
	createRequests chan fragmentWatchRequestObservation
	handlers       []fragmentWatchHandler
}

func (service *fragmentWatchTestService) Watch(
	stream etcdserverpb.Watch_WatchServer,
) error {
	attempt := service.attempts.Add(1)
	if attempt > int32(len(service.handlers)) {
		return status.Errorf(codes.Internal, "unexpected Watch stream attempt %d", attempt)
	}
	return service.handlers[attempt-1](service, stream, attempt)
}

func (service *fragmentWatchTestService) createWatcher(
	stream etcdserverpb.Watch_WatchServer,
	attempt int32,
	watchID int64,
) (etcdserverpb.WatchCreateRequest, error) {
	request, err := stream.Recv()
	if err != nil {
		return etcdserverpb.WatchCreateRequest{}, err
	}
	createRequest := request.GetCreateRequest()
	if createRequest == nil {
		return etcdserverpb.WatchCreateRequest{},
			status.Error(codes.InvalidArgument, "expected a Watch create request")
	}
	requestValue := *createRequest
	select {
	case service.createRequests <- fragmentWatchRequestObservation{
		attempt: attempt,
		request: requestValue,
	}:
	case <-stream.Context().Done():
		return etcdserverpb.WatchCreateRequest{}, stream.Context().Err()
	}
	if err := stream.Send(createdWatchResponse(watchID, fragmentWatchRevision)); err != nil {
		return etcdserverpb.WatchCreateRequest{}, err
	}
	return requestValue, nil
}

type fragmentWatchClientFixture struct {
	watcher   clientv3.Watcher
	fragments <-chan fragmentWatchObservation
}

func newFragmentWatchClientFixture(
	t *testing.T,
	service *fragmentWatchTestService,
) *fragmentWatchClientFixture {
	t.Helper()
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
	fragments := make(chan fragmentWatchObservation, 16)
	remoteWatchClient := &fragmentObservingWatchClient{
		WatchClient: etcdserverpb.NewWatchClient(connection),
		fragments:   fragments,
	}
	watcher := clientv3.NewWatchFromWatchClient(remoteWatchClient, client)
	t.Cleanup(func() { _ = watcher.Close() })
	return &fragmentWatchClientFixture{
		watcher:   watcher,
		fragments: fragments,
	}
}

type fragmentObservingWatchClient struct {
	etcdserverpb.WatchClient
	fragments chan<- fragmentWatchObservation
}

func (client *fragmentObservingWatchClient) Watch(
	ctx context.Context,
	options ...grpc.CallOption,
) (etcdserverpb.Watch_WatchClient, error) {
	stream, err := client.WatchClient.Watch(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &fragmentObservingWatchStream{
		Watch_WatchClient: stream,
		fragments:         client.fragments,
	}, nil
}

type fragmentObservingWatchStream struct {
	etcdserverpb.Watch_WatchClient
	fragments chan<- fragmentWatchObservation
}

func (stream *fragmentObservingWatchStream) Recv() (
	*etcdserverpb.WatchResponse,
	error,
) {
	response, err := stream.Watch_WatchClient.Recv()
	if response != nil && response.Fragment {
		observation := fragmentWatchObservation{
			watchID:  response.WatchId,
			revision: response.Header.GetRevision(),
			more:     response.Fragment,
		}
		select {
		case stream.fragments <- observation:
		default:
		}
	}
	return response, err
}

func createdWatchResponse(watchID, revision int64) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: revision},
		WatchId: watchID,
		Created: true,
	}
}

func fragmentWatchResponse(
	watchID int64,
	revision int64,
	event mvccpb.Event,
	hasMoreFragments bool,
) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:   &etcdserverpb.ResponseHeader{Revision: revision},
		WatchId:  watchID,
		Events:   []*mvccpb.Event{&event},
		Fragment: hasMoreFragments,
	}
}

func watchTestEventForKey(key string, marker byte, modRevision int64) mvccpb.Event {
	return mvccpb.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:            []byte(key),
			Value:          bytes.Repeat([]byte{marker}, fragmentWatchValueBytes),
			CreateRevision: 1,
			ModRevision:    modRevision,
			Version:        1,
		},
	}
}

func assertWatchCreateRequests(
	t *testing.T,
	ctx context.Context,
	requests <-chan fragmentWatchRequestObservation,
	attempt int32,
	expected map[string]int64,
) {
	t.Helper()
	seen := make(map[string]bool, len(expected))
	for requestIndex := 0; requestIndex < len(expected); requestIndex++ {
		observation := receiveWatchCreateRequest(t, ctx, requests)
		key := string(observation.request.Key)
		expectedRevision, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected Watch create request key %q on attempt %d",
				key, observation.attempt)
		}
		if seen[key] {
			t.Fatalf("duplicate Watch create request for key %q on attempt %d",
				key, observation.attempt)
		}
		seen[key] = true
		if observation.attempt != attempt {
			t.Errorf("Watch create request for %q used stream attempt %d, want %d",
				key, observation.attempt, attempt)
		}
		if observation.request.StartRevision != expectedRevision {
			t.Errorf("Watch create request for %q starts at revision %d, want %d",
				key, observation.request.StartRevision, expectedRevision)
		}
		if !observation.request.Fragment {
			t.Errorf("Watch create request for %q did not request fragments", key)
		}
	}
	for key := range expected {
		if !seen[key] {
			t.Errorf("Watch create request for %q was not observed on attempt %d",
				key, attempt)
		}
	}
}

func receiveWatchCreateRequest(
	t *testing.T,
	ctx context.Context,
	requests <-chan fragmentWatchRequestObservation,
) fragmentWatchRequestObservation {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Watch create request: %v", ctx.Err())
		return fragmentWatchRequestObservation{}
	}
}

func receiveFragmentObservation(
	t *testing.T,
	ctx context.Context,
	fragments <-chan fragmentWatchObservation,
) fragmentWatchObservation {
	t.Helper()
	select {
	case observation := <-fragments:
		return observation
	case <-ctx.Done():
		t.Fatalf("timed out waiting for a Watch fragment: %v", ctx.Err())
		return fragmentWatchObservation{}
	}
}

func receiveWatchResponse(
	t *testing.T,
	ctx context.Context,
	responses clientv3.WatchChan,
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

func assertWatchResponse(
	t *testing.T,
	response clientv3.WatchResponse,
	revision int64,
	expectedEvents ...expectedWatchEvent,
) {
	t.Helper()
	if response.Canceled {
		t.Fatalf("watch canceled before the revision completed: %v", response.Err())
	}
	if response.Created {
		t.Fatalf("unexpected Created notification: %+v", response)
	}
	if response.Header.Revision != revision {
		t.Errorf("response header revision = %d, want %d",
			response.Header.Revision, revision)
	}
	if len(response.Events) != len(expectedEvents) {
		t.Fatalf("response event count = %d, want %d",
			len(response.Events), len(expectedEvents))
	}
	for index, expected := range expectedEvents {
		event := response.Events[index]
		if event == nil || event.Kv == nil {
			t.Errorf("event %d has no key-value payload", index)
			continue
		}
		if !bytes.Equal(event.Kv.Key, []byte(expected.key)) {
			t.Errorf("event %d key = %q, want %q", index, event.Kv.Key, expected.key)
		}
		if event.Kv.ModRevision != revision {
			t.Errorf("event %d mod_revision = %d, want %d",
				index, event.Kv.ModRevision, revision)
		}
		expectedValue := bytes.Repeat([]byte{expected.marker}, fragmentWatchValueBytes)
		if !bytes.Equal(event.Kv.Value, expectedValue) {
			t.Errorf("event %d value changed while the Go client reassembled it", index)
		}
	}
}
