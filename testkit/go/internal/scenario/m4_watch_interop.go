package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RunM4WatchLifecycleInterop verifies the Java WatchService with the pinned raw-proto and
// clientv3 Watch APIs. The fixture supplies already-selected events; this endpoint performs Watch
// RPCs only and does not claim KV, historical replay, or reconnect evidence.
func RunM4WatchLifecycleInterop(ctx context.Context, client *clientv3.Client, prefix string,
	output io.Writer) error {
	if err := ValidateM4Lease("local-service", prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("local Watch lifecycle interop requires context, client, and output")
	}
	watchClient := etcdserverpb.NewWatchClient(client.ActiveConnection())
	if err := checkInteropMultipleWatches(ctx, watchClient, prefix); err != nil {
		return err
	}
	if err := checkInteropByteRanges(ctx, watchClient, prefix); err != nil {
		return err
	}
	if err := checkInteropFiltersAndPreviousValues(ctx, watchClient, prefix); err != nil {
		return err
	}
	if err := checkInteropStreamCloseAndRecreate(ctx, watchClient, prefix); err != nil {
		return err
	}
	if err := checkInteropControlBoundaries(ctx, watchClient, prefix); err != nil {
		return err
	}
	if err := checkClientV3Watch(ctx, client, prefix); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(output, "M4 Watch lifecycle interop passed"); err != nil {
		return errors.New("local Watch lifecycle result could not be written")
	}
	return nil
}

func checkClientV3Watch(ctx context.Context, client *clientv3.Client, prefix string) error {
	watchContext, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	watchKey := string(interopMultiKey(prefix, 401))
	const concurrentWatchCount = 2
	start := make(chan struct{})
	started := make(chan clientv3.WatchChan, concurrentWatchCount)
	for range concurrentWatchCount {
		go func() {
			<-start
			started <- client.Watcher.Watch(watchContext, watchKey, clientv3.WithCreatedNotify())
		}()
	}
	close(start)

	responses := make([]clientv3.WatchChan, 0, concurrentWatchCount)
	for range concurrentWatchCount {
		select {
		case response := <-started:
			responses = append(responses, response)
		case <-ctx.Done():
			return fmt.Errorf("clientv3 Watch calls did not return their channels: %w", ctx.Err())
		}
	}

	results := make(chan error, concurrentWatchCount)
	for _, responseChannel := range responses {
		go func(responseChannel clientv3.WatchChan) {
			results <- verifyClientV3WatchResponse(ctx, responseChannel, watchKey)
		}(responseChannel)
	}
	for range concurrentWatchCount {
		select {
		case err := <-results:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return fmt.Errorf("concurrent clientv3 Watches did not receive Created and their events: %w", ctx.Err())
		}
	}

	cancelWatch()
	closeContext, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	for _, responseChannel := range responses {
		if err := waitClientV3WatchClosed(closeContext, responseChannel); err != nil {
			return err
		}
	}
	return nil
}

func verifyClientV3WatchResponse(ctx context.Context, responses clientv3.WatchChan, watchKey string) error {
	created := false
	gotEvent := false
	for !created || !gotEvent {
		select {
		case response, open := <-responses:
			if !open {
				return errors.New("clientv3 Watch channel closed before Created and event delivery")
			}
			if err := response.Err(); err != nil {
				return fmt.Errorf("clientv3 Watch returned an error: %w", err)
			}
			if response.Created {
				if created || len(response.Events) != 0 {
					return errors.New("clientv3 Watch Created response was duplicated or included events")
				}
				created = true
				continue
			}
			if !created {
				return errors.New("clientv3 Watch delivered an event before Created")
			}
			for _, event := range response.Events {
				if event == nil || event.Kv == nil || string(event.Kv.Key) != watchKey {
					return errors.New("clientv3 Watch returned an unexpected event")
				}
				if event.Type != mvccpb.PUT || string(event.Kv.Value) != "multi-a" ||
					event.Kv.ModRevision != 2 {
					return errors.New("clientv3 Watch event fields differ from the fixture")
				}
				gotEvent = true
			}
		case <-ctx.Done():
			return fmt.Errorf("clientv3 Watch did not receive Created and its event: %w", ctx.Err())
		}
	}
	return nil
}

func waitClientV3WatchClosed(ctx context.Context, responses clientv3.WatchChan) error {
	for {
		select {
		case _, open := <-responses:
			if !open {
				return nil
			}
		case <-ctx.Done():
			return errors.New("clientv3 Watch channel did not close after context cancellation")
		}
	}
}

func checkInteropControlBoundaries(ctx context.Context, client etcdserverpb.WatchClient,
	prefix string) error {
	reader, cancel, err := openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()

	key := []byte(prefix + "interop/control/no-response")
	const startRevision = int64(13)
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
		Key: key, WatchId: 0, StartRevision: startRevision,
	}); err != nil {
		return fmt.Errorf("local Watch automatic ID: %w", err)
	}
	if err := reader.stream.Send(&etcdserverpb.WatchRequest{}); err != nil {
		return fmt.Errorf("send local empty Watch request: %w", err)
	}
	if err := interopCancel(reader, 0); err != nil {
		return fmt.Errorf("empty Watch request emitted a response or lost its watcher: %w", err)
	}
	if len(reader.pending) != 0 {
		return errors.New("local empty Watch request emitted an unsolicited response")
	}

	const explicitWatchID = int64(601)
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
		Key: key, WatchId: explicitWatchID, StartRevision: startRevision,
	}); err != nil {
		return err
	}
	if err := reader.stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 602},
	}}); err != nil {
		return fmt.Errorf("send local unknown Watch cancellation: %w", err)
	}
	if err := interopCancel(reader, explicitWatchID); err != nil {
		return fmt.Errorf("unknown Watch cancellation emitted a response or removed another watcher: %w", err)
	}
	if len(reader.pending) != 0 {
		return errors.New("local unknown Watch cancellation emitted an unsolicited response")
	}

	invalidRange := &etcdserverpb.WatchCreateRequest{
		Key: key, RangeEnd: key, WatchId: 603,
	}
	if err := interopSendCreate(reader, invalidRange); err != nil {
		return err
	}
	rejected, err := reader.wait("invalid Watch range", func(response *etcdserverpb.WatchResponse) bool {
		return response.WatchId == -1 && response.Created && response.Canceled
	})
	if err != nil {
		return err
	}
	if rejected.Header == nil || rejected.CancelReason != "mvcc: watcher range is empty" ||
		len(rejected.Events) != 0 || len(reader.pending) != 0 {
		return errors.New("local invalid Watch range response differs from the pinned native contract")
	}
	return nil
}

func checkInteropMultipleWatches(ctx context.Context, client etcdserverpb.WatchClient,
	prefix string) error {
	reader, cancel, err := openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()

	keyA := []byte(prefix + "interop/multi/a")
	keyB := []byte(prefix + "interop/multi/b")
	for _, watch := range []struct {
		id  int64
		key []byte
	}{{101, keyA}, {102, keyB}} {
		if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
			Key: watch.key, WatchId: watch.id,
		}); err != nil {
			return err
		}
	}
	if err := interopSendCreate(reader, &etcdserverpb.WatchCreateRequest{
		Key: keyA, WatchId: 101,
	}); err != nil {
		return err
	}
	duplicate, err := reader.wait("duplicate Watch ID", func(response *etcdserverpb.WatchResponse) bool {
		return response.WatchId == -1 && response.Created && response.Canceled
	})
	if err != nil {
		return err
	}
	if duplicate.Header == nil || duplicate.CancelReason != m4WatchDuplicateIDReason ||
		len(duplicate.Events) != 0 {
		return errors.New("local Watch duplicate-ID response differs from the native contract")
	}
	if err := requireInteropEvent(reader, 101, interopExpectedEvent{
		typeID: mvccpb.PUT, key: keyA, value: []byte("multi-a"), revision: 2,
	}); err != nil {
		return err
	}
	if err := requireInteropEvent(reader, 102, interopExpectedEvent{
		typeID: mvccpb.PUT, key: keyB, value: []byte("multi-b"), revision: 3,
	}); err != nil {
		return err
	}
	if err := interopCancel(reader, 101); err != nil {
		return err
	}
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{Key: keyA, WatchId: 101}); err != nil {
		return err
	}
	if err := requireInteropEvent(reader, 101, interopExpectedEvent{
		typeID: mvccpb.PUT, key: keyA, value: []byte("multi-a"), revision: 2,
	}); err != nil {
		return err
	}
	if err := interopCancel(reader, 101); err != nil {
		return err
	}
	return interopCancel(reader, 102)
}

func checkInteropByteRanges(ctx context.Context, client etcdserverpb.WatchClient,
	prefix string) error {
	reader, cancel, err := openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{WatchId: 201}); err != nil {
		cancel()
		return err
	}
	if err := requireInteropEvent(reader, 201, interopExpectedEvent{
		typeID: mvccpb.PUT, key: []byte{0}, value: []byte("empty-key"), revision: 8,
	}); err != nil {
		cancel()
		return err
	}
	if err := interopCancel(reader, 201); err != nil {
		cancel()
		return err
	}
	cancel()

	reader, cancel, err = openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
		RangeEnd: []byte{0}, WatchId: 202, PrevKv: true,
	}); err != nil {
		cancel()
		return err
	}
	for _, expected := range interopFixtureEvents(prefix) {
		if err := requireInteropEvent(reader, 202, expected); err != nil {
			cancel()
			return err
		}
	}
	if err := interopCancel(reader, 202); err != nil {
		cancel()
		return err
	}
	cancel()

	reader, cancel, err = openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	key := []byte{0, 0xff, 0x0f}
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
		Key: key, RangeEnd: []byte{0, 0xff, 0x11}, WatchId: 203,
	}); err != nil {
		cancel()
		return err
	}
	for _, expected := range []interopExpectedEvent{
		{typeID: mvccpb.PUT, key: []byte{0, 0xff, 0x0f}, value: []byte("binary-before"), revision: 4},
		{typeID: mvccpb.PUT, key: []byte{0, 0xff, 0x10}, value: []byte("binary-key"), revision: 5},
	} {
		if err := requireInteropEvent(reader, 203, expected); err != nil {
			cancel()
			return err
		}
	}
	if err := interopCancel(reader, 203); err != nil {
		cancel()
		return err
	}
	cancel()

	reader, cancel, err = openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	key = []byte{0, 0xff, 0x10}
	if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 204}); err != nil {
		cancel()
		return err
	}
	if err := requireInteropEvent(reader, 204, interopExpectedEvent{
		typeID: mvccpb.PUT, key: key, value: []byte("binary-key"), revision: 5,
	}); err != nil {
		cancel()
		return err
	}
	if err := interopCancel(reader, 204); err != nil {
		cancel()
		return err
	}
	cancel()
	return nil
}

func checkInteropFiltersAndPreviousValues(ctx context.Context, client etcdserverpb.WatchClient,
	prefix string) error {
	reader, cancel, err := openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancel()

	base := prefix + "interop/filter/"
	rangeEnd := []byte(prefix + "interop/filter0")
	putKey, deleteKey := []byte(base+"put"), []byte(base+"delete")
	putMarker, deleteMarker := []byte(base+"put-marker"), []byte(base+"delete-marker")
	creates := []*etcdserverpb.WatchCreateRequest{
		{Key: []byte(base), RangeEnd: rangeEnd, WatchId: 301, PrevKv: true},
		{Key: []byte(base), RangeEnd: rangeEnd, WatchId: 302, PrevKv: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT}},
		{Key: []byte(base), RangeEnd: rangeEnd, WatchId: 303, PrevKv: true,
			Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NODELETE}},
	}
	for _, create := range creates {
		if err := interopCreate(reader, create); err != nil {
			return err
		}
	}
	watchEvents := map[int64][]interopExpectedEvent{
		301: {
			{typeID: mvccpb.PUT, key: putKey, value: []byte("put-after"), previous: []byte("put-before"), revision: 9},
			{typeID: mvccpb.DELETE, key: deleteKey, previous: []byte("delete-before"), revision: 10},
			{typeID: mvccpb.PUT, key: putMarker, value: []byte("put-marker"), revision: 11},
			{typeID: mvccpb.DELETE, key: deleteMarker, previous: []byte("delete-marker-before"), revision: 12},
		},
		302: {
			{typeID: mvccpb.DELETE, key: deleteKey, previous: []byte("delete-before"), revision: 10},
			{typeID: mvccpb.DELETE, key: deleteMarker, previous: []byte("delete-marker-before"), revision: 12},
		},
		303: {
			{typeID: mvccpb.PUT, key: putKey, value: []byte("put-after"), previous: []byte("put-before"), revision: 9},
			{typeID: mvccpb.PUT, key: putMarker, value: []byte("put-marker"), revision: 11},
		},
	}
	for watchID, expected := range watchEvents {
		for _, event := range expected {
			if err := requireInteropEvent(reader, watchID, event); err != nil {
				return err
			}
		}
	}
	for _, watchID := range []int64{301, 302, 303} {
		if err := interopCancel(reader, watchID); err != nil {
			return err
		}
	}
	return nil
}

func checkInteropStreamCloseAndRecreate(ctx context.Context, client etcdserverpb.WatchClient,
	prefix string) error {
	streamContext, cancel := context.WithCancel(ctx)
	stream, err := client.Watch(streamContext)
	if err != nil {
		cancel()
		return err
	}
	reader := &interopWatchReader{stream: stream}
	for _, watchID := range []int64{401, 402} {
		if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
			Key: interopMultiKey(prefix, watchID), WatchId: watchID,
		}); err != nil {
			cancel()
			return err
		}
	}
	cancel()
	for drained := 0; drained <= 2; drained++ {
		response, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
				break
			}
			return fmt.Errorf("receive local Watch stream closure: %w", err)
		}
		if drained == 2 {
			return errors.New("local Watch stream sent more than two buffered responses after cancellation")
		}
		if response == nil || (response.WatchId != 401 && response.WatchId != 402) {
			return errors.New("local Watch stream returned an unexpected buffered response after cancellation")
		}
	}

	reader, cancelNew, err := openInteropStream(ctx, client)
	if err != nil {
		return err
	}
	defer cancelNew()
	for _, watchID := range []int64{401, 402} {
		if err := interopCreate(reader, &etcdserverpb.WatchCreateRequest{
			Key: interopMultiKey(prefix, watchID), WatchId: watchID,
		}); err != nil {
			return err
		}
	}
	for _, expected := range []struct {
		id       int64
		key      []byte
		value    []byte
		revision int64
	}{
		{401, []byte(prefix + "interop/multi/a"), []byte("multi-a"), 2},
		{402, []byte(prefix + "interop/multi/b"), []byte("multi-b"), 3},
	} {
		if err := requireInteropEvent(reader, expected.id, interopExpectedEvent{
			typeID: mvccpb.PUT, key: expected.key, value: expected.value, revision: expected.revision,
		}); err != nil {
			return err
		}
	}
	for _, watchID := range []int64{401, 402} {
		if err := interopCancel(reader, watchID); err != nil {
			return err
		}
	}
	return nil
}

type interopWatchReader struct {
	stream  etcdserverpb.Watch_WatchClient
	pending []*etcdserverpb.WatchResponse
}

type interopExpectedEvent struct {
	typeID   mvccpb.Event_EventType
	key      []byte
	value    []byte
	previous []byte
	revision int64
}

func openInteropStream(ctx context.Context, client etcdserverpb.WatchClient) (
	*interopWatchReader, context.CancelFunc, error) {
	streamContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	stream, err := client.Watch(streamContext)
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	return &interopWatchReader{stream: stream}, cancel, nil
}

func interopCreate(reader *interopWatchReader, create *etcdserverpb.WatchCreateRequest) error {
	if err := interopSendCreate(reader, create); err != nil {
		return err
	}
	response, err := reader.wait("Watch Created", func(response *etcdserverpb.WatchResponse) bool {
		return response.WatchId == create.WatchId && response.Created && !response.Canceled
	})
	if err != nil {
		return err
	}
	if response.Header == nil || len(response.Events) != 0 {
		return errors.New("local Watch Created response has no header or includes events")
	}
	return nil
}

func interopSendCreate(reader *interopWatchReader, create *etcdserverpb.WatchCreateRequest) error {
	return reader.stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: create,
	}})
}

func interopCancel(reader *interopWatchReader, watchID int64) error {
	if err := reader.stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: watchID},
	}}); err != nil {
		return err
	}
	response, err := reader.wait("Watch Canceled", func(response *etcdserverpb.WatchResponse) bool {
		return response.WatchId == watchID && response.Canceled && !response.Created
	})
	if err != nil {
		return err
	}
	if len(response.Events) != 0 {
		return errors.New("local Watch sent an event in its cancellation acknowledgement")
	}
	for _, pending := range reader.pending {
		if pending.WatchId == watchID && len(pending.Events) != 0 {
			return errors.New("local Watch emitted an unexpected event before cancellation")
		}
	}
	return nil
}

func (reader *interopWatchReader) wait(operation string,
	match func(*etcdserverpb.WatchResponse) bool) (*etcdserverpb.WatchResponse, error) {
	for index, response := range reader.pending {
		if match(response) {
			reader.pending = append(reader.pending[:index], reader.pending[index+1:]...)
			return response, nil
		}
	}
	for {
		response, err := reader.stream.Recv()
		if err != nil {
			return nil, fmt.Errorf("receive local %s: %w", operation, err)
		}
		if response == nil {
			return nil, fmt.Errorf("receive local %s: WatchService returned nil", operation)
		}
		if match(response) {
			return response, nil
		}
		reader.pending = append(reader.pending, response)
	}
}

func requireInteropEvent(reader *interopWatchReader, watchID int64, expected interopExpectedEvent) error {
	response, err := reader.wait(fmt.Sprintf("Watch event id=%d revision=%d", watchID, expected.revision), func(response *etcdserverpb.WatchResponse) bool {
		return response.WatchId == watchID && len(response.Events) != 0
	})
	if err != nil {
		return err
	}
	if response.Header == nil || response.Header.Revision != expected.revision || len(response.Events) != 1 {
		return errors.New("local Watch event revision or response grouping differs from the expected event")
	}
	event := response.Events[0]
	if event == nil || event.Kv == nil || event.Type != expected.typeID ||
		!bytes.Equal(event.Kv.Key, expected.key) || !bytes.Equal(event.Kv.Value, expected.value) {
		return errors.New("local Watch event key, value, or type differs from its selected revision")
	}
	if expected.typeID == mvccpb.DELETE && (event.Kv.CreateRevision != 0 ||
		event.Kv.ModRevision != expected.revision || event.Kv.Version != 0 || event.Kv.Lease != 0) {
		return errors.New("local DELETE Watch event metadata differs from the pinned etcd behavior")
	}
	if expected.previous == nil {
		if event.PrevKv != nil {
			return errors.New("local Watch returned an unexpected PrevKV")
		}
		return nil
	}
	if event.PrevKv == nil || !bytes.Equal(event.PrevKv.Key, expected.key) ||
		!bytes.Equal(event.PrevKv.Value, expected.previous) {
		return errors.New("local Watch omitted or changed the expected PrevKV")
	}
	return nil
}

func interopFixtureEvents(prefix string) []interopExpectedEvent {
	base := prefix + "interop/"
	filterPrefix := base + "filter/"
	return []interopExpectedEvent{
		{typeID: mvccpb.PUT, key: []byte(base + "multi/a"), value: []byte("multi-a"), revision: 2},
		{typeID: mvccpb.PUT, key: []byte(base + "multi/b"), value: []byte("multi-b"), revision: 3},
		{typeID: mvccpb.PUT, key: []byte{0, 0xff, 0x0f}, value: []byte("binary-before"), revision: 4},
		{typeID: mvccpb.PUT, key: []byte{0, 0xff, 0x10}, value: []byte("binary-key"), revision: 5},
		{typeID: mvccpb.PUT, key: []byte{0, 0xff, 0x11}, value: []byte("binary-after"), revision: 6},
		{typeID: mvccpb.PUT, key: []byte{0xff, 0x00}, value: []byte("binary-high"), revision: 7},
		{typeID: mvccpb.PUT, key: []byte{0}, value: []byte("empty-key"), revision: 8},
		{typeID: mvccpb.PUT, key: []byte(filterPrefix + "put"), value: []byte("put-after"),
			previous: []byte("put-before"), revision: 9},
		{typeID: mvccpb.DELETE, key: []byte(filterPrefix + "delete"),
			previous: []byte("delete-before"), revision: 10},
		{typeID: mvccpb.PUT, key: []byte(filterPrefix + "put-marker"), value: []byte("put-marker"), revision: 11},
		{typeID: mvccpb.DELETE, key: []byte(filterPrefix + "delete-marker"),
			previous: []byte("delete-marker-before"), revision: 12},
	}
}

func interopMultiKey(prefix string, watchID int64) []byte {
	name := "a"
	if watchID == 102 || watchID == 402 {
		name = "b"
	}
	return []byte(prefix + "interop/multi/" + name)
}
