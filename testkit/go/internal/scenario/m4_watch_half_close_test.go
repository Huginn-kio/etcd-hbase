package scenario

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestM4WatchHalfCloseMatchesPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	native, err := startEmbedded(ctx, filepath.Join(t.TempDir(), "native"))
	if err != nil {
		t.Fatalf("start pinned etcd v3.5.23: %v", err)
	}
	defer native.close()

	watchClient := etcdserverpb.NewWatchClient(native.client.ActiveConnection())
	kvClient := etcdserverpb.NewKVClient(native.client.ActiveConnection())
	streamContext, cancelStream := context.WithCancel(ctx)
	stream, err := watchClient.Watch(streamContext)
	if err != nil {
		cancelStream()
		t.Fatalf("open native Watch stream: %v", err)
	}
	defer cancelStream()

	key := []byte("etcd-hbase-m4-test/watch/half-close")
	const watchID int64 = 4701
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: key, WatchId: watchID,
	}); err != nil {
		t.Fatalf("create native Watch before half-close: %v", err)
	}
	if err := requireM4WatchCreated(stream, watchID); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("half-close native Watch request stream: %v", err)
	}

	put, err := kvClient.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-half-close")})
	if err != nil {
		t.Fatalf("write native Watch event after half-close: %v", err)
	}
	event, err := receiveM4Watch(stream, "native event after half-close")
	if err != nil {
		t.Fatal(err)
	}
	if event.Header == nil || event.WatchId != watchID || event.Created || event.Canceled ||
		len(event.Events) != 1 || event.Events[0].Type != mvccpb.PUT || event.Events[0].Kv == nil ||
		!bytes.Equal(event.Events[0].Kv.Key, key) ||
		!bytes.Equal(event.Events[0].Kv.Value, []byte("after-half-close")) ||
		event.Header.Revision != put.Header.Revision ||
		event.Events[0].Kv.ModRevision != put.Header.Revision {
		t.Fatalf("native Watch did not keep its response side open after half-close: %v", event)
	}

	requireM4WatchOpenUntilContextCancellation(t, stream, cancelStream, "native active Watch")

	emptyContext, cancelEmpty := context.WithCancel(ctx)
	emptyStream, err := watchClient.Watch(emptyContext)
	if err != nil {
		cancelEmpty()
		t.Fatalf("open native Watch stream with no active watches: %v", err)
	}
	if err := emptyStream.CloseSend(); err != nil {
		cancelEmpty()
		t.Fatalf("half-close native empty Watch stream: %v", err)
	}
	requireM4WatchOpenUntilContextCancellation(t, emptyStream, cancelEmpty, "native empty Watch")

	assertNativeCompactedLastWatchKeepsHalfClosedStreamOpen(t, ctx, watchClient, kvClient)
}

func assertNativeCompactedLastWatchKeepsHalfClosedStreamOpen(t *testing.T, parent context.Context,
	watchClient etcdserverpb.WatchClient, kvClient etcdserverpb.KVClient) {
	t.Helper()

	baseline, err := kvClient.Range(parent, &etcdserverpb.RangeRequest{Key: []byte("m4/half-close/compact-baseline")})
	if err != nil || baseline == nil || baseline.Header == nil || baseline.Header.Revision < 1 {
		t.Fatalf("read native revision before compacted Watch: response=%v err=%v", baseline, err)
	}
	put, err := kvClient.Put(parent, &etcdserverpb.PutRequest{
		Key: []byte("m4/half-close/unwatched-compaction-trigger"), Value: []byte("advance-revision"),
	})
	if err != nil || put == nil || put.Header == nil {
		t.Fatalf("advance native revision for Watch compaction: response=%v err=%v", put, err)
	}
	if _, err := kvClient.Compact(parent, &etcdserverpb.CompactionRequest{Revision: put.Header.Revision}); err != nil {
		t.Fatalf("compact native revision before stale Watch creation: %v", err)
	}

	streamContext, cancelStream := context.WithCancel(parent)
	stream, err := watchClient.Watch(streamContext)
	if err != nil {
		cancelStream()
		t.Fatalf("open native Watch stream after compaction: %v", err)
	}
	defer cancelStream()
	const watchID int64 = 4702
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: []byte("m4/half-close/compacted-watch"), StartRevision: baseline.Header.Revision, WatchId: watchID,
	}); err != nil {
		t.Fatalf("create native Watch below compact revision: %v", err)
	}
	if err := requireM4WatchCreated(stream, watchID); err != nil {
		t.Fatal(err)
	}
	response, err := receiveM4Watch(stream, "compacted Watch cancellation")
	if err != nil {
		t.Fatal(err)
	}
	if response.WatchId != watchID || !response.Canceled || response.CompactRevision != put.Header.Revision {
		t.Fatalf("native compaction response changed: watch_id=%d canceled=%t compact_revision=%d want watch_id=%d compact_revision=%d",
			response.WatchId, response.Canceled, response.CompactRevision, watchID, put.Header.Revision)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("half-close native stream after its last Watch was compacted: %v", err)
	}
	requireM4WatchOpenUntilContextCancellation(t, stream, cancelStream, "native stream after last Watch compaction")
}

func requireM4WatchOpenUntilContextCancellation(t *testing.T, stream etcdserverpb.Watch_WatchClient,
	cancel context.CancelFunc, description string) {
	t.Helper()

	result := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		result <- err
	}()
	observation := time.NewTimer(150 * time.Millisecond)
	defer observation.Stop()
	select {
	case err := <-result:
		cancel()
		if err == nil {
			t.Fatalf("%s completed before the client canceled its RPC context", description)
		}
		t.Fatalf("%s terminated before the client canceled its RPC context: %v", description, err)
	case <-observation.C:
	}

	cancel()
	select {
	case err := <-result:
		if err == nil || (status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled)) {
			t.Fatalf("%s did not stop after full client cancellation: %v", description, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not unblock after full client cancellation", description)
	}
}
