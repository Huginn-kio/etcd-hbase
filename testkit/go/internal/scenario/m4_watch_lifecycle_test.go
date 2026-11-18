package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestM4WatchLifecycleMatchesPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	native, err := startEmbedded(ctx, filepath.Join(t.TempDir(), "native"))
	if err != nil {
		t.Fatalf("start pinned native etcd: %v", err)
	}
	defer native.close()

	watchClient := etcdserverpb.NewWatchClient(native.client.ActiveConnection())
	kvClient := etcdserverpb.NewKVClient(native.client.ActiveConnection())
	if err := checkM4MultipleWatchLifecycle(ctx, watchClient, kvClient); err != nil {
		t.Fatal(err)
	}
	if err := checkM4WatchByteRanges(ctx, watchClient, kvClient); err != nil {
		t.Fatal(err)
	}
	if err := checkM4WatchFiltersAndPreviousValues(ctx, watchClient, kvClient); err != nil {
		t.Fatal(err)
	}
	if err := checkM4WatchStreamCloseAndRecreate(ctx, watchClient); err != nil {
		t.Fatal(err)
	}
	if err := checkM4WatchControlBoundaries(ctx, watchClient, kvClient); err != nil {
		t.Fatal(err)
	}
}

func checkM4WatchControlBoundaries(ctx context.Context, client etcdserverpb.WatchClient,
	kv etcdserverpb.KVClient) error {
	key := []byte("etcd-hbase-m4-test/j45-watch-lifecycle/control/no-response")
	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		return fmt.Errorf("read native Watch control boundary: %w", err)
	}
	if current == nil || current.Header == nil || current.Header.Revision < 1 ||
		current.Header.Revision == math.MaxInt64 {
		return errors.New("native Watch control boundary has an invalid revision")
	}

	stream, cancel, err := openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()
	startRevision := current.Header.Revision + 1
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: key, WatchId: 0, StartRevision: startRevision,
	}); err != nil {
		return err
	}
	created, err := receiveM4Watch(stream, "automatic Watch ID")
	if err != nil {
		return err
	}
	if created.Header == nil || created.WatchId != 0 || !created.Created || created.Canceled ||
		len(created.Events) != 0 {
		return errors.New("native Watch did not assign automatic Watch ID zero")
	}

	if err := stream.Send(&etcdserverpb.WatchRequest{}); err != nil {
		return fmt.Errorf("send native empty Watch request: %w", err)
	}
	if err := cancelM4Watch(stream, 0); err != nil {
		return fmt.Errorf("empty Watch request emitted a response or lost its watcher: %w", err)
	}

	const explicitWatchID = int64(601)
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: key, WatchId: explicitWatchID, StartRevision: startRevision,
	}); err != nil {
		return err
	}
	if err := requireM4WatchCreated(stream, explicitWatchID); err != nil {
		return err
	}
	if err := stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 602},
	}}); err != nil {
		return fmt.Errorf("send native unknown Watch cancellation: %w", err)
	}
	if err := cancelM4Watch(stream, explicitWatchID); err != nil {
		return fmt.Errorf("unknown Watch cancellation emitted a response or removed another watcher: %w", err)
	}

	const invalidRangeWatchID = int64(603)
	invalidRange := &etcdserverpb.WatchCreateRequest{
		Key: key, RangeEnd: key, WatchId: invalidRangeWatchID,
	}
	if err := sendM4WatchCreate(stream, invalidRange); err != nil {
		return err
	}
	rejected, err := receiveM4Watch(stream, "invalid Watch range")
	if err != nil {
		return err
	}
	if rejected.Header == nil || rejected.WatchId != -1 || !rejected.Created || !rejected.Canceled ||
		rejected.CancelReason != "mvcc: watcher range is empty" || len(rejected.Events) != 0 {
		return errors.New("native invalid Watch range response differs from the pinned contract")
	}
	return nil
}

func checkM4MultipleWatchLifecycle(ctx context.Context, client etcdserverpb.WatchClient,
	kv etcdserverpb.KVClient) error {
	stream, cancel, err := openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()

	keyA := []byte("etcd-hbase-m4-test/j45-watch-lifecycle/multi/a")
	keyB := []byte("etcd-hbase-m4-test/j45-watch-lifecycle/multi/b")
	for _, watch := range []struct {
		id  int64
		key []byte
	}{{101, keyA}, {102, keyB}} {
		if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
			Key: watch.key, WatchId: watch.id,
		}); err != nil {
			return err
		}
		if err := requireM4WatchCreated(stream, watch.id); err != nil {
			return err
		}
	}

	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: keyA, WatchId: 101,
	}); err != nil {
		return err
	}
	duplicate, err := receiveM4Watch(stream, "duplicate active Watch ID")
	if err != nil {
		return err
	}
	if duplicate.Header == nil || duplicate.WatchId != -1 || !duplicate.Created || !duplicate.Canceled ||
		duplicate.CancelReason != m4WatchDuplicateIDReason || len(duplicate.Events) != 0 {
		return errors.New("duplicate active Watch ID did not preserve the original subscription")
	}

	if err := cancelM4Watch(stream, 101); err != nil {
		return err
	}
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		Key: keyA, WatchId: 101,
	}); err != nil {
		return err
	}
	if err := requireM4WatchCreated(stream, 101); err != nil {
		return err
	}
	if err := putM4WatchKey(ctx, kv, keyA, []byte("reused")); err != nil {
		return err
	}
	response, err := receiveM4Watch(stream, "reused Watch event")
	if err != nil {
		return err
	}
	if err := requireM4WatchPut(response, 101, keyA, []byte("reused")); err != nil {
		return err
	}
	if err := cancelM4Watch(stream, 101); err != nil {
		return err
	}

	if err := putM4WatchKey(ctx, kv, keyA, []byte("after-cancel")); err != nil {
		return err
	}
	if err := putM4WatchKey(ctx, kv, keyB, []byte("still-active")); err != nil {
		return err
	}
	response, err = receiveM4Watch(stream, "remaining Watch event")
	if err != nil {
		return err
	}
	if err := requireM4WatchPut(response, 102, keyB, []byte("still-active")); err != nil {
		return err
	}
	return cancelM4Watch(stream, 102)
}

func checkM4WatchByteRanges(ctx context.Context, client etcdserverpb.WatchClient,
	kv etcdserverpb.KVClient) error {
	stream, cancel, err := openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{WatchId: 201}); err != nil {
		cancel()
		return err
	}
	if err := requireM4WatchCreated(stream, 201); err != nil {
		cancel()
		return err
	}
	emptyKey := []byte{0}
	if err := putM4WatchKey(ctx, kv, emptyKey, []byte("empty-key")); err != nil {
		cancel()
		return err
	}
	response, err := receiveM4Watch(stream, "normalized empty-key event")
	if err != nil {
		cancel()
		return err
	}
	if err := requireM4WatchPut(response, 201, emptyKey, []byte("empty-key")); err != nil {
		cancel()
		return err
	}
	if err := putM4WatchKey(ctx, kv, []byte{0, 1}, []byte("outside-empty-key")); err != nil {
		cancel()
		return err
	}
	if err := cancelM4Watch(stream, 201); err != nil {
		cancel()
		return err
	}
	cancel()

	stream, cancel, err = openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
		RangeEnd: []byte{0}, WatchId: 202,
	}); err != nil {
		cancel()
		return err
	}
	if err := requireM4WatchCreated(stream, 202); err != nil {
		cancel()
		return err
	}
	for _, key := range [][]byte{{0}, {0, 0xff, 0x10}, {0xff, 0x00}} {
		if err := putM4WatchKey(ctx, kv, key, []byte("from-key")); err != nil {
			cancel()
			return err
		}
		response, err := receiveM4Watch(stream, "FromKey range event")
		if err != nil {
			cancel()
			return err
		}
		if err := requireM4WatchPut(response, 202, key, []byte("from-key")); err != nil {
			cancel()
			return err
		}
	}
	if err := cancelM4Watch(stream, 202); err != nil {
		cancel()
		return err
	}
	cancel()

	stream, cancel, err = openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	key := []byte{0, 0xff, 0x10}
	if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 203}); err != nil {
		cancel()
		return err
	}
	if err := requireM4WatchCreated(stream, 203); err != nil {
		cancel()
		return err
	}
	if err := putM4WatchKey(ctx, kv, []byte{0, 0xff, 0x0f}, []byte("before-binary-key")); err != nil {
		cancel()
		return err
	}
	if err := putM4WatchKey(ctx, kv, key, []byte("binary-key")); err != nil {
		cancel()
		return err
	}
	response, err = receiveM4Watch(stream, "binary exact-key event")
	if err != nil {
		cancel()
		return err
	}
	if err := requireM4WatchPut(response, 203, key, []byte("binary-key")); err != nil {
		cancel()
		return err
	}
	if err := putM4WatchKey(ctx, kv, []byte{0, 0xff, 0x11}, []byte("after-binary-key")); err != nil {
		cancel()
		return err
	}
	if err := cancelM4Watch(stream, 203); err != nil {
		cancel()
		return err
	}
	cancel()
	return nil
}

func checkM4WatchFiltersAndPreviousValues(ctx context.Context, client etcdserverpb.WatchClient,
	kv etcdserverpb.KVClient) error {
	stream, cancel, err := openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()

	prefix := []byte("etcd-hbase-m4-test/j45-watch-lifecycle/filter/")
	rangeEnd := append([]byte(nil), prefix...)
	rangeEnd[len(rangeEnd)-1]++
	putKey := append(append([]byte(nil), prefix...), []byte("put")...)
	deleteKey := append(append([]byte(nil), prefix...), []byte("delete")...)
	putMarker := append(append([]byte(nil), prefix...), []byte("put-marker")...)
	deleteMarker := append(append([]byte(nil), prefix...), []byte("delete-marker")...)
	for _, entry := range []struct{ key, value []byte }{
		{putKey, []byte("put-before")}, {deleteKey, []byte("delete-before")},
		{deleteMarker, []byte("delete-marker-before")},
	} {
		if err := putM4WatchKey(ctx, kv, entry.key, entry.value); err != nil {
			return err
		}
	}

	for _, create := range []*etcdserverpb.WatchCreateRequest{
		{Key: prefix, RangeEnd: rangeEnd, WatchId: 301, PrevKv: true},
		{Key: prefix, RangeEnd: rangeEnd, WatchId: 302, PrevKv: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT}},
		{Key: prefix, RangeEnd: rangeEnd, WatchId: 303, PrevKv: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NODELETE}},
	} {
		if err := sendM4WatchCreate(stream, create); err != nil {
			return err
		}
		if err := requireM4WatchCreated(stream, create.WatchId); err != nil {
			return err
		}
	}

	var deleteKeyRevision int64
	var deleteMarkerRevision int64
	for _, change := range []struct {
		key, value []byte
		delete     bool
	}{
		{key: putKey, value: []byte("put-after")},
		{key: deleteKey, delete: true},
		{key: putMarker, value: []byte("put-marker")},
		{key: deleteMarker, delete: true},
	} {
		if change.delete {
			revision, err := deleteM4WatchKeyAtRevision(ctx, kv, change.key)
			if err != nil {
				return err
			}
			if bytes.Equal(change.key, deleteKey) {
				deleteKeyRevision = revision
			} else if bytes.Equal(change.key, deleteMarker) {
				deleteMarkerRevision = revision
			}
		} else if err := putM4WatchKey(ctx, kv, change.key, change.value); err != nil {
			return err
		}
	}

	expected := map[int64][]m4WatchEventExpectation{
		301: {
			{eventType: mvccpb.PUT, key: putKey, value: []byte("put-after"), previous: []byte("put-before")},
			{eventType: mvccpb.DELETE, key: deleteKey, previous: []byte("delete-before"), modRevision: deleteKeyRevision},
			{eventType: mvccpb.PUT, key: putMarker, value: []byte("put-marker")},
			{eventType: mvccpb.DELETE, key: deleteMarker, previous: []byte("delete-marker-before"), modRevision: deleteMarkerRevision},
		},
		302: {
			{eventType: mvccpb.DELETE, key: deleteKey, previous: []byte("delete-before"), modRevision: deleteKeyRevision},
			{eventType: mvccpb.DELETE, key: deleteMarker, previous: []byte("delete-marker-before"), modRevision: deleteMarkerRevision},
		},
		303: {
			{eventType: mvccpb.PUT, key: putKey, value: []byte("put-after"), previous: []byte("put-before")},
			{eventType: mvccpb.PUT, key: putMarker, value: []byte("put-marker")},
		},
	}
	positions := map[int64]int{301: 0, 302: 0, 303: 0}
	remaining := 8
	for remaining > 0 {
		response, err := receiveM4Watch(stream, "filtered Watch event")
		if err != nil {
			return fmt.Errorf("receive filtered Watch event at positions %v with %d remaining: %w",
				positions, remaining, err)
		}
		if response.Header == nil || len(response.Events) == 0 {
			return errors.New("native filtered Watch emitted a headerless or empty event response")
		}
		for _, event := range response.Events {
			want, ok := expected[response.WatchId]
			position := positions[response.WatchId]
			if !ok || position >= len(want) {
				return errors.New("native filtered Watch emitted an event for an unknown or completed watch")
			}
			if err := requireM4WatchEvent(event, want[position]); err != nil {
				return err
			}
			positions[response.WatchId] = position + 1
			remaining--
		}
	}
	for _, watchID := range []int64{301, 302, 303} {
		if positions[watchID] != len(expected[watchID]) {
			return errors.New("native filtered Watch event count differs from its filter contract")
		}
		if err := sendM4WatchCancel(stream, watchID); err != nil {
			return err
		}
	}
	for range expected {
		response, err := receiveM4Watch(stream, "filtered Watch cancellation")
		if err != nil {
			return err
		}
		if len(response.Events) != 0 || !response.Canceled || response.Created {
			return errors.New("native filtered Watch emitted an event after cancellation")
		}
		delete(positions, response.WatchId)
	}
	if len(positions) != 0 {
		return errors.New("native filtered Watch did not cancel every active watch ID")
	}
	return nil
}

type m4WatchEventExpectation struct {
	eventType   mvccpb.Event_EventType
	key         []byte
	value       []byte
	previous    []byte
	modRevision int64
}

func requireM4WatchEvent(event *mvccpb.Event, want m4WatchEventExpectation) error {
	if event == nil || event.Kv == nil || event.Type != want.eventType ||
		!bytes.Equal(event.Kv.Key, want.key) || !bytes.Equal(event.Kv.Value, want.value) {
		return errors.New("native Watch event key, type, or value differs from its expected revision")
	}
	if want.eventType == mvccpb.DELETE && (event.Kv.CreateRevision != 0 ||
		event.Kv.ModRevision != want.modRevision || event.Kv.Version != 0 || event.Kv.Lease != 0) {
		return errors.New("native DELETE Watch event metadata differs from the pinned etcd behavior")
	}
	if want.previous == nil {
		if event.PrevKv != nil {
			return errors.New("native Watch returned an unexpected previous value")
		}
		return nil
	}
	if event.PrevKv == nil || !bytes.Equal(event.PrevKv.Key, want.key) ||
		!bytes.Equal(event.PrevKv.Value, want.previous) {
		return errors.New("native Watch omitted or changed the requested previous value")
	}
	return nil
}

func checkM4WatchStreamCloseAndRecreate(ctx context.Context,
	client etcdserverpb.WatchClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.Watch(streamCtx)
	if err != nil {
		cancel()
		return err
	}
	for _, watchID := range []int64{401, 402} {
		if err := sendM4WatchCreate(stream, &etcdserverpb.WatchCreateRequest{
			Key: []byte("etcd-hbase-m4-test/j45-watch-lifecycle/close"), WatchId: watchID,
		}); err != nil {
			cancel()
			return err
		}
		if err := requireM4WatchCreated(stream, watchID); err != nil {
			cancel()
			return err
		}
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
		return errors.New("native Watch stream did not close after its context was canceled")
	}

	newStream, newCancel, err := openM4WatchStream(ctx, client)
	if err != nil {
		return err
	}
	defer newCancel()
	for _, watchID := range []int64{401, 402} {
		if err := sendM4WatchCreate(newStream, &etcdserverpb.WatchCreateRequest{
			Key: []byte("etcd-hbase-m4-test/j45-watch-lifecycle/recreated"), WatchId: watchID,
		}); err != nil {
			return err
		}
		if err := requireM4WatchCreated(newStream, watchID); err != nil {
			return err
		}
	}
	for _, watchID := range []int64{401, 402} {
		if err := cancelM4Watch(newStream, watchID); err != nil {
			return err
		}
	}
	return nil
}

func openM4WatchStream(ctx context.Context, client etcdserverpb.WatchClient) (
	etcdserverpb.Watch_WatchClient, context.CancelFunc, error) {
	return openM4WatchStreamWithTimeout(ctx, client, 10*time.Second)
}

func openM4WatchStreamWithTimeout(ctx context.Context, client etcdserverpb.WatchClient,
	timeout time.Duration) (etcdserverpb.Watch_WatchClient, context.CancelFunc, error) {
	streamCtx, cancel := context.WithTimeout(ctx, timeout)
	stream, err := client.Watch(streamCtx)
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	return stream, cancel, nil
}

func sendM4WatchCreate(stream etcdserverpb.Watch_WatchClient,
	create *etcdserverpb.WatchCreateRequest) error {
	return stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: create,
	}})
}

func requireM4WatchCreated(stream etcdserverpb.Watch_WatchClient, watchID int64) error {
	response, err := receiveM4Watch(stream, "Watch created acknowledgement")
	if err != nil {
		return err
	}
	if response.Header == nil || response.WatchId != watchID || !response.Created ||
		response.Canceled || len(response.Events) != 0 {
		return errors.New("native Watch create acknowledgement changed its ID or state")
	}
	return nil
}

func sendM4WatchCancel(stream etcdserverpb.Watch_WatchClient, watchID int64) error {
	return stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: watchID},
	}})
}

func cancelM4Watch(stream etcdserverpb.Watch_WatchClient, watchID int64) error {
	if err := sendM4WatchCancel(stream, watchID); err != nil {
		return err
	}
	response, err := receiveM4Watch(stream, "Watch cancellation acknowledgement")
	if err != nil {
		return err
	}
	if response.Header == nil || response.WatchId != watchID || !response.Canceled ||
		response.Created || len(response.Events) != 0 {
		return errors.New("native Watch cancellation acknowledgement changed its ID or state")
	}
	return nil
}

func receiveM4Watch(stream etcdserverpb.Watch_WatchClient, operation string) (
	*etcdserverpb.WatchResponse, error) {
	response, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receive %s: %w", operation, err)
	}
	if response == nil {
		return nil, errors.New("native Watch returned a nil " + operation)
	}
	return response, nil
}

func requireM4WatchPut(response *etcdserverpb.WatchResponse, watchID int64,
	key, value []byte) error {
	if response == nil || response.Header == nil || response.WatchId != watchID ||
		len(response.Events) != 1 || response.Events[0].Type != mvccpb.PUT ||
		response.Events[0].Kv == nil || !bytes.Equal(response.Events[0].Kv.Key, key) ||
		!bytes.Equal(response.Events[0].Kv.Value, value) {
		return errors.New("native Watch delivered an event to the wrong active watch or range")
	}
	return nil
}

func putM4WatchKey(ctx context.Context, client etcdserverpb.KVClient, key, value []byte) error {
	response, err := client.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if response == nil || response.Header == nil || response.Header.Revision < 1 {
		return errors.New("native Watch fixture Put did not return a revision")
	}
	return nil
}

func deleteM4WatchKey(ctx context.Context, client etcdserverpb.KVClient, key []byte) error {
	_, err := deleteM4WatchKeyAtRevision(ctx, client, key)
	return err
}

func deleteM4WatchKeyAtRevision(ctx context.Context, client etcdserverpb.KVClient, key []byte) (int64, error) {
	response, err := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	if err != nil {
		return 0, err
	}
	if response == nil || response.Header == nil || response.Deleted != 1 {
		return 0, errors.New("native Watch fixture Delete did not remove exactly one key")
	}
	return response.Header.Revision, nil
}
