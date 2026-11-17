package scenario

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestM4WatchProgressRequestMatchesPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server, err := startEmbedded(ctx, filepath.Join(t.TempDir(), "native"))
	if err != nil {
		t.Fatalf("start pinned etcd v3.5.23: %v", err)
	}
	defer server.close()

	key := []byte(fmt.Sprintf("etcd-hbase-m4-test/watch-progress/%s", t.Name()))
	watchClient := etcdserverpb.NewWatchClient(server.client.ActiveConnection())
	stream, err := watchClient.Watch(ctx)
	if err != nil {
		t.Fatalf("open native Watch stream: %v", err)
	}
	defer stream.CloseSend()
	const watchID int64 = 4602
	if err := stream.Send(watchCreateRequest(watchID, key, 0)); err != nil {
		t.Fatalf("send native Watch create: %v", err)
	}
	created, err := receiveWatchResponse(stream, "native Watch create")
	if err != nil {
		t.Fatal(err)
	}
	if created.Header == nil || created.WatchId != watchID || !created.Created || created.Canceled {
		t.Fatalf("native Watch Created response is malformed: %v", created)
	}

	putRequest := &etcdserverpb.PutRequest{Key: key, Value: []byte("progress-barrier")}
	put, err := etcdserverpb.NewKVClient(server.client.ActiveConnection()).Put(ctx, putRequest)
	if err != nil {
		t.Fatalf("write native Watch fixture: %v", err)
	}
	event, err := receiveWatchResponse(stream, "native Watch event before progress")
	if err != nil {
		t.Fatal(err)
	}
	if event.Header == nil || event.WatchId != watchID || len(event.Events) != 1 ||
		event.Header.Revision != put.Header.Revision || event.Events[0].Type != mvccpb.PUT ||
		event.Events[0].Kv == nil || !bytes.Equal(event.Events[0].Kv.Key, key) ||
		event.Events[0].Kv.ModRevision != put.Header.Revision {
		t.Fatalf("native Watch did not deliver the fixture before the barrier: %v", event)
	}

	progressRequest := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
		ProgressRequest: &etcdserverpb.WatchProgressRequest{},
	}}
	if err := stream.Send(progressRequest); err != nil {
		t.Fatalf("send native Watch ProgressRequest: %v", err)
	}
	progress, err := receiveWatchResponse(stream, "native Watch progress")
	if err != nil {
		t.Fatal(err)
	}
	if progress.Header == nil || progress.WatchId != clientv3.InvalidWatchID ||
		progress.Header.Revision != put.Header.Revision || progress.Created || progress.Canceled ||
		len(progress.Events) != 0 {
		t.Fatalf("native ProgressRequest response differs from the J46.2 barrier contract: %v", progress)
	}
}
