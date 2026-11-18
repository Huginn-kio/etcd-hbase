package scenario

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	m4WatchJavaEndpointEnv = "ETCD_HBASE_M4_WATCH_ENDPOINT"
	m4WatchJavaPrefixEnv   = "ETCD_HBASE_M4_WATCH_PREFIX"
	m4WatchJavaEventCount  = 32
	m4WatchJavaValueBytes  = 1536
	m4WatchJavaUpdateBytes = 32
	m4WatchJavaSeedBatch   = 8
	m4WatchJavaRPCTimeout  = 10 * time.Second
	m4WatchJavaTestTimeout = 90 * time.Second
	m4WatchJavaRawWatchID  = int64(4601)
)

type m4WatchJavaFixture struct {
	key             []byte
	oldValue        []byte
	updatedValue    []byte
	initialRevision int64
}

type m4WatchJavaFragmentInterrupt struct {
	enabled                 atomic.Bool
	injected                atomic.Bool
	watchStreams            atomic.Int32
	fragmentResponses       atomic.Int32
	interruptAfterFragment  int32
	externalCancelRequested atomic.Bool
	cancelMu                sync.Mutex
	cancelRPC               context.CancelFunc
}

type m4WatchJavaInterruptStream struct {
	grpc.ClientStream
	interrupt *m4WatchJavaFragmentInterrupt
	cancelRPC context.CancelFunc
}

func (interrupt *m4WatchJavaFragmentInterrupt) streamInterceptor(
	ctx context.Context, descriptor *grpc.StreamDesc, connection *grpc.ClientConn,
	method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
	if method != "/etcdserverpb.Watch/Watch" || !interrupt.enabled.Load() {
		return streamer(ctx, descriptor, connection, method, options...)
	}
	streamContext, cancelRPC := context.WithCancel(ctx)
	stream, err := streamer(streamContext, descriptor, connection, method, options...)
	if err != nil {
		cancelRPC()
		return nil, err
	}
	interrupt.watchStreams.Add(1)
	interrupt.cancelMu.Lock()
	interrupt.cancelRPC = cancelRPC
	interrupt.cancelMu.Unlock()
	return &m4WatchJavaInterruptStream{
		ClientStream: stream, interrupt: interrupt, cancelRPC: cancelRPC,
	}, nil
}

func (stream *m4WatchJavaInterruptStream) RecvMsg(message any) error {
	if err := stream.ClientStream.RecvMsg(message); err != nil {
		if stream.interrupt.externalCancelRequested.CompareAndSwap(true, false) {
			stream.interrupt.injected.Store(true)
			return status.Error(codes.Unavailable,
				"test injected a disconnect after a complete fragmented revision")
		}
		return err
	}
	response, ok := message.(*etcdserverpb.WatchResponse)
	if !ok || !response.Fragment || stream.interrupt.injected.Load() {
		return nil
	}
	fragmentNumber := stream.interrupt.fragmentResponses.Add(1)
	if fragmentNumber < stream.interrupt.interruptAfterFragment {
		return nil
	}
	if fragmentNumber != stream.interrupt.interruptAfterFragment ||
		!stream.interrupt.injected.CompareAndSwap(false, true) {
		return nil
	}
	// Cancel the RPC transport so the Java service releases this watch before the pinned client
	// reconnects and resumes from the last completely reassembled revision.
	stream.cancelRPC()
	return status.Error(codes.Unavailable,
		"test injected an interruption during a fragmented Watch revision")
}

func (interrupt *m4WatchJavaFragmentInterrupt) cancelCurrentRPC() bool {
	interrupt.cancelMu.Lock()
	cancelRPC := interrupt.cancelRPC
	interrupt.cancelMu.Unlock()
	if cancelRPC == nil || !interrupt.externalCancelRequested.CompareAndSwap(false, true) {
		return false
	}
	cancelRPC()
	return true
}

func TestM4WatchJavaServiceFragmentReplayAutoResumeAndLiveHandoff(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv(m4WatchJavaEndpointEnv))
	if endpoint == "" {
		t.Skipf("set %s to run the Java WatchService integration test", m4WatchJavaEndpointEnv)
	}
	prefixBase := strings.TrimSpace(os.Getenv(m4WatchJavaPrefixEnv))
	if prefixBase == "" {
		t.Fatalf("%s is required when %s is set", m4WatchJavaPrefixEnv, m4WatchJavaEndpointEnv)
	}

	testContext, cancelTest := context.WithTimeout(context.Background(), m4WatchJavaTestTimeout)
	defer cancelTest()
	client := newM4WatchJavaClient(t, endpoint)

	prefix := newM4WatchJavaTestPrefix(t, prefixBase)
	registerM4WatchJavaCleanup(t, client, prefix)

	fixtures, initialRevision, updateRevision := createM4WatchJavaRevisionFixtures(
		t, testContext, client, prefix)

	assertM4WatchJavaRawFragments(
		t, testContext, client, prefix, initialRevision, updateRevision, fixtures)
	assertM4WatchJavaFragmentRecovery(
		t, testContext, client, endpoint, prefix, initialRevision, updateRevision, fixtures)
	replayPrefix := prefix + "replay/"
	replayFixtures, _, replayUpdateRevision :=
		createM4WatchJavaRevisionFixtures(t, testContext, client, replayPrefix)
	assertM4WatchJavaClientReplayAndHandoff(
		t, testContext, client, replayPrefix,
		replayUpdateRevision, replayFixtures)
}

func newM4WatchJavaClient(t *testing.T, endpoint string) *clientv3.Client {
	return newM4WatchJavaClientWithDialOptions(t, endpoint)
}

func newM4WatchJavaClientWithDialOptions(t *testing.T, endpoint string,
	dialOptions ...grpc.DialOption) *clientv3.Client {
	return newM4WatchJavaClientConfigured(t, endpoint, true, dialOptions...)
}

func newM4WatchJavaClientForInterruptionTest(t *testing.T, endpoint string,
	dialOptions ...grpc.DialOption) *clientv3.Client {
	return newM4WatchJavaClientConfigured(t, endpoint, false, dialOptions...)
}

func newM4WatchJavaClientConfigured(t *testing.T, endpoint string,
	closeOnCleanup bool, dialOptions ...grpc.DialOption) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
		DialOptions: dialOptions,
	})
	if err != nil {
		t.Fatalf("connect to configured Java WatchService endpoint: %v", err)
	}
	if closeOnCleanup {
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Errorf("close Java WatchService client: %v", err)
			}
		})
	}
	return client
}

func registerM4WatchJavaCleanup(t *testing.T, client *clientv3.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupContext, cancelCleanup := context.WithTimeout(
			context.Background(), m4WatchJavaRPCTimeout)
		defer cancelCleanup()
		if _, err := client.Delete(cleanupContext, prefix, clientv3.WithPrefix()); err != nil {
			t.Errorf("delete isolated Java WatchService test keys: %v", err)
		}
	})
}

func createM4WatchJavaRevisionFixtures(t *testing.T, parent context.Context,
	client *clientv3.Client, prefix string) ([]m4WatchJavaFixture, int64, int64) {
	t.Helper()
	fixtures := newM4WatchJavaFixtures(prefix)
	for start := 0; start < len(fixtures); start += m4WatchJavaSeedBatch {
		end := min(start+m4WatchJavaSeedBatch, len(fixtures))
		seedOperations := make([]clientv3.Op, 0, end-start)
		for _, fixture := range fixtures[start:end] {
			seedOperations = append(seedOperations,
				clientv3.OpPut(string(fixture.key), string(fixture.oldValue)))
		}
		seedRevision := commitM4WatchJavaOperations(t, parent, client, seedOperations)
		for index := start; index < end; index++ {
			fixtures[index].initialRevision = seedRevision
		}
	}
	initialRevision := fixtures[len(fixtures)-1].initialRevision
	updateOperations := make([]clientv3.Op, 0, len(fixtures))
	for _, fixture := range fixtures {
		updateOperations = append(updateOperations,
			clientv3.OpPut(string(fixture.key), string(fixture.updatedValue)))
	}
	updateRevision := commitM4WatchJavaOperations(t, parent, client, updateOperations)
	if updateRevision != initialRevision+1 {
		t.Fatalf("update transaction revision = %d, want the next revision after %d",
			updateRevision, initialRevision)
	}
	assertM4WatchJavaInitialRange(
		t, parent, client, prefix, initialRevision, updateRevision, fixtures)
	return fixtures, initialRevision, updateRevision
}

func newM4WatchJavaTestPrefix(t *testing.T, prefixBase string) string {
	t.Helper()
	trimmed := strings.TrimRight(strings.TrimSpace(prefixBase), "/")
	if trimmed == "" {
		t.Fatalf("%s must contain an isolated test prefix", m4WatchJavaPrefixEnv)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatalf("generate isolated Watch test namespace: %v", err)
	}
	return trimmed + "/j46-watch-client-" + hex.EncodeToString(nonce[:]) + "/"
}

func newM4WatchJavaFixtures(prefix string) []m4WatchJavaFixture {
	fixtures := make([]m4WatchJavaFixture, 0, m4WatchJavaEventCount)
	for index := 0; index < m4WatchJavaEventCount; index++ {
		key := []byte(fmt.Sprintf("%s%02d", prefix, index))
		oldValue := bytes.Repeat([]byte{byte(index + 1)}, m4WatchJavaValueBytes)
		updatedValue := bytes.Repeat([]byte{byte(index + 65)}, m4WatchJavaUpdateBytes)
		fixtures = append(fixtures, m4WatchJavaFixture{
			key: key, oldValue: oldValue, updatedValue: updatedValue,
		})
	}
	return fixtures
}

func commitM4WatchJavaOperations(t *testing.T, parent context.Context,
	client *clientv3.Client, operations []clientv3.Op) int64 {
	t.Helper()
	rpcContext, cancelRPC := context.WithTimeout(parent, m4WatchJavaRPCTimeout)
	defer cancelRPC()
	response, err := client.Txn(rpcContext).Then(operations...).Commit()
	if err != nil {
		t.Fatalf("commit Java WatchService fixture transaction: %v", err)
	}
	if response == nil || response.Header == nil || !response.Succeeded || response.Header.Revision < 1 {
		t.Fatalf("Java WatchService fixture transaction returned an invalid response: %v", response)
	}
	return response.Header.Revision
}

func assertM4WatchJavaInitialRange(t *testing.T, parent context.Context,
	client *clientv3.Client, prefix string, requestedRevision, currentRevision int64,
	fixtures []m4WatchJavaFixture) {
	t.Helper()
	rpcContext, cancelRPC := context.WithTimeout(parent, m4WatchJavaRPCTimeout)
	defer cancelRPC()
	response, err := client.Get(
		rpcContext, prefix, clientv3.WithPrefix(), clientv3.WithRev(requestedRevision))
	if err != nil {
		t.Fatalf("read Java WatchService initial fixture revision: %v", err)
	}
	if response == nil {
		t.Fatal("initial Range returned a nil response")
	}
	if response.Header == nil || response.Header.Revision != currentRevision {
		t.Fatalf("historical Range header revision = %v, want current revision %d",
			response.Header, currentRevision)
	}
	if len(response.Kvs) != len(fixtures) {
		t.Fatalf("initial Range returned %d keys, want %d", len(response.Kvs), len(fixtures))
	}
	for index, fixture := range fixtures {
		keyValue := response.Kvs[index]
		if keyValue == nil || !bytes.Equal(keyValue.Key, fixture.key) ||
			!bytes.Equal(keyValue.Value, fixture.oldValue) ||
			keyValue.CreateRevision != fixture.initialRevision ||
			keyValue.ModRevision != fixture.initialRevision ||
			keyValue.Version != 1 {
			t.Fatalf("initial Range key %d does not belong to one fixture revision: %v",
				index, keyValue)
		}
	}
}

func assertM4WatchJavaRawFragments(t *testing.T, parent context.Context,
	client *clientv3.Client, prefix string, initialRevision, updateRevision int64,
	fixtures []m4WatchJavaFixture) {
	t.Helper()
	stream, cancelStream := openM4WatchJavaRawPrefixWatch(
		t, parent, client, prefix, updateRevision)
	defer cancelStream()

	responses, err := readM4WatchJavaFragmentedRevision(stream, m4WatchJavaRawWatchID, updateRevision)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) < 3 {
		t.Fatalf("raw Watch used only %d fragments; need at least three to test first, middle, and final boundaries",
			len(responses))
	}
	events := make([]*mvccpb.Event, 0, len(fixtures))
	for index, response := range responses {
		if response.Fragment != (index < len(responses)-1) {
			t.Fatalf("raw Watch fragment %d has fragment=%t; only non-final fragments may be true",
				index, response.Fragment)
		}
		events = append(events, response.Events...)
	}
	if err := verifyM4WatchJavaEvents(
		"raw update Watch", events, fixtures, initialRevision, updateRevision, true); err != nil {
		t.Fatal(err)
	}
	if err := cancelM4Watch(stream, m4WatchJavaRawWatchID); err != nil {
		t.Fatalf("cancel raw Java WatchService watch: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Errorf("close raw Java WatchService stream: %v", err)
	}
}

func assertM4WatchJavaFragmentRecovery(t *testing.T, parent context.Context,
	client *clientv3.Client, endpoint, prefix string, initialRevision, updateRevision int64,
	fixtures []m4WatchJavaFixture) {
	t.Helper()
	stream, cancelStream := openM4WatchJavaRawPrefixWatch(
		t, parent, client, prefix, updateRevision)
	firstFragment, err := receiveM4Watch(stream, "first fragment before Watch interruption")
	if err != nil {
		cancelStream()
		t.Fatal(err)
	}
	if firstFragment.Header == nil || firstFragment.Header.Revision != updateRevision ||
		firstFragment.WatchId != m4WatchJavaRawWatchID || !firstFragment.Fragment ||
		len(firstFragment.Events) == 0 || len(firstFragment.Events) >= len(fixtures) {
		cancelStream()
		t.Fatalf("raw Watch did not stop inside a fragmented revision: %v", firstFragment)
	}
	if err := verifyM4WatchJavaPartialUpdateFragment(
		firstFragment.Events, fixtures, initialRevision, updateRevision); err != nil {
		cancelStream()
		t.Fatal(err)
	}
	cancelStream()

	assertM4WatchJavaClientAutoResumeAtFragment(t, parent, endpoint,
		prefix, initialRevision, updateRevision, fixtures, 1)
	assertM4WatchJavaClientAutoResumeAtFragment(t, parent, endpoint,
		prefix, initialRevision, updateRevision, fixtures, 2)
	assertM4WatchJavaClientResumesAfterCompleteRevision(t, parent, endpoint,
		prefix, updateRevision, fixtures)
}

func assertM4WatchJavaClientAutoResumeAtFragment(t *testing.T, parent context.Context,
	endpoint, prefix string, initialRevision, updateRevision int64,
	fixtures []m4WatchJavaFixture, interruptAfterFragment int32) {
	t.Helper()
	interrupt := &m4WatchJavaFragmentInterrupt{interruptAfterFragment: interruptAfterFragment}
	client := newM4WatchJavaClientForInterruptionTest(t, endpoint,
		grpc.WithStreamInterceptor(interrupt.streamInterceptor))
	defer closeM4WatchJavaClient(t, client)
	interrupt.enabled.Store(true)

	watchContext, cancelWatch := context.WithTimeout(parent, 30*time.Second)
	defer cancelWatch()
	responses := client.Watcher.Watch(watchContext, prefix,
		clientv3.WithRev(updateRevision), clientv3.WithPrefix(),
		clientv3.WithPrevKV(), clientv3.WithFragment())
	assertM4WatchJavaClientBatch(t, watchContext, responses,
		fmt.Sprintf("clientv3 automatic reconnect after fragment %d", interruptAfterFragment), fixtures,
		initialRevision, updateRevision, true)
	waitForM4WatchJavaReconnect(t, watchContext, interrupt, interruptAfterFragment)
}

func assertM4WatchJavaClientResumesAfterCompleteRevision(t *testing.T, parent context.Context,
	endpoint, prefix string, updateRevision int64, fixtures []m4WatchJavaFixture) {
	t.Helper()
	interrupt := &m4WatchJavaFragmentInterrupt{}
	client := newM4WatchJavaClientForInterruptionTest(t, endpoint,
		grpc.WithStreamInterceptor(interrupt.streamInterceptor))
	defer closeM4WatchJavaClient(t, client)
	interrupt.enabled.Store(true)

	watchContext, cancelWatch := context.WithTimeout(parent, 30*time.Second)
	defer cancelWatch()
	responses := client.Watcher.Watch(watchContext, prefix,
		clientv3.WithRev(updateRevision), clientv3.WithPrefix(),
		clientv3.WithPrevKV(), clientv3.WithFragment())
	assertM4WatchJavaClientBatch(t, watchContext, responses,
		"clientv3 receives the complete revision before the stream breaks", fixtures,
		updateRevision-1, updateRevision, true)
	if !interrupt.cancelCurrentRPC() {
		t.Fatal("could not inject a disconnect after the complete revision was delivered")
	}
	waitForM4WatchJavaReconnect(t, watchContext, interrupt, 0)

	nextKey := []byte(prefix + "after-reconnect")
	nextValue := bytes.Repeat([]byte{0xF1}, m4WatchJavaValueBytes)
	putContext, cancelPut := context.WithTimeout(parent, m4WatchJavaRPCTimeout)
	put, err := client.Put(putContext, string(nextKey), string(nextValue))
	cancelPut()
	if err != nil {
		t.Fatalf("put key after the Go Watch client resumed at the next revision: %v", err)
	}
	if put == nil || put.Header == nil || put.Header.Revision != updateRevision+1 {
		t.Fatalf("post-resume Put revision = %v, want %d", put, updateRevision+1)
	}
	assertM4WatchJavaClientBatch(t, watchContext, responses,
		"clientv3 resumes after a completed fragmented revision", []m4WatchJavaFixture{{
			key: nextKey, oldValue: nextValue,
		}}, 0, put.Header.Revision, false)
}

func waitForM4WatchJavaReconnect(t *testing.T, ctx context.Context,
	interrupt *m4WatchJavaFragmentInterrupt, fragment int32) {
	t.Helper()
	deadline := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	for !interrupt.injected.Load() || interrupt.watchStreams.Load() < 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("clientv3 did not reconnect after fragment %d: injected=%t streams=%d: %v",
				fragment, interrupt.injected.Load(), interrupt.watchStreams.Load(), ctx.Err())
		case <-deadline.C:
		}
	}
}

func closeM4WatchJavaClient(t *testing.T, client *clientv3.Client) {
	t.Helper()
	if err := client.Close(); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("close clientv3 Watch client after interruption test: %v", err)
	}
}

func verifyM4WatchJavaPartialUpdateFragment(events []*mvccpb.Event,
	fixtures []m4WatchJavaFixture, initialRevision, updateRevision int64) error {
	fixtureIndexes := make(map[string]int, len(fixtures))
	fixtureByKey := make(map[string]m4WatchJavaFixture, len(fixtures))
	for index, fixture := range fixtures {
		key := string(fixture.key)
		fixtureIndexes[key] = index
		fixtureByKey[key] = fixture
	}
	previousIndex := -1
	for eventIndex, event := range events {
		if event == nil || event.Kv == nil {
			return fmt.Errorf("partial raw Watch fragment event %d is missing its key-value", eventIndex)
		}
		key := string(event.Kv.Key)
		index, found := fixtureIndexes[key]
		if !found || index <= previousIndex {
			return fmt.Errorf("partial raw Watch fragment event %d is outside the ordered update revision", eventIndex)
		}
		fixture := fixtureByKey[key]
		if err := verifyM4WatchJavaEvents("partial raw Watch update", []*mvccpb.Event{event},
			[]m4WatchJavaFixture{fixture}, initialRevision, updateRevision, true); err != nil {
			return err
		}
		previousIndex = index
	}
	return nil
}

func openM4WatchJavaRawPrefixWatch(t *testing.T, parent context.Context,
	client *clientv3.Client, prefix string, startRevision int64) (
	etcdserverpb.Watch_WatchClient, context.CancelFunc) {
	t.Helper()
	watchClient := etcdserverpb.NewWatchClient(client.ActiveConnection())
	stream, cancelStream, err := openM4WatchStreamWithTimeout(
		parent, watchClient, m4WatchJavaTestTimeout)
	if err != nil {
		t.Fatalf("open raw Java WatchService stream: %v", err)
		return nil, cancelStream
	}

	create := &etcdserverpb.WatchCreateRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		StartRevision: startRevision, WatchId: m4WatchJavaRawWatchID,
		PrevKv: true, Fragment: true,
	}
	if err := sendM4WatchCreate(stream, create); err != nil {
		cancelStream()
		t.Fatalf("create raw Java WatchService watch: %v", err)
		return nil, cancelStream
	}
	created, err := receiveM4Watch(stream, "raw Watch created acknowledgement")
	if err != nil {
		cancelStream()
		t.Fatal(err)
		return nil, cancelStream
	}
	if created.Header == nil || created.Header.Revision < startRevision ||
		created.WatchId != m4WatchJavaRawWatchID || !created.Created ||
		created.Canceled || len(created.Events) != 0 {
		cancelStream()
		t.Fatalf("raw Java WatchService created response is invalid: %v", created)
		return nil, cancelStream
	}
	return stream, cancelStream
}

func readM4WatchJavaFragmentedRevision(stream etcdserverpb.Watch_WatchClient,
	watchID, revision int64) ([]*etcdserverpb.WatchResponse, error) {
	responses := make([]*etcdserverpb.WatchResponse, 0, 2)
	for {
		response, err := receiveM4Watch(stream, "fragmented Watch revision")
		if err != nil {
			return nil, err
		}
		if response.Header == nil || response.Header.Revision != revision ||
			response.WatchId != watchID || response.Created || response.Canceled ||
			response.CompactRevision != 0 || len(response.Events) == 0 {
			return nil, fmt.Errorf("raw Watch fragment differs from revision %d: %v", revision, response)
		}
		responses = append(responses, response)
		if !response.Fragment {
			return responses, nil
		}
	}
}

func assertM4WatchJavaClientReplayAndHandoff(t *testing.T, parent context.Context,
	client *clientv3.Client, prefix string, updateRevision int64,
	fixtures []m4WatchJavaFixture) {
	t.Helper()
	watchContext, cancelWatch := context.WithTimeout(parent, 45*time.Second)
	defer cancelWatch()
	responses := client.Watcher.Watch(watchContext, prefix,
		clientv3.WithRev(updateRevision), clientv3.WithPrefix(),
		clientv3.WithPrevKV(), clientv3.WithFragment())
	assertM4WatchJavaClientBatch(t, watchContext, responses,
		"clientv3 fragmented historical replay", fixtures, updateRevision, updateRevision, true)

	liveKey := []byte(prefix + "live")
	liveValue := bytes.Repeat([]byte{0xE7}, m4WatchJavaValueBytes)
	liveContext, cancelLive := context.WithTimeout(parent, m4WatchJavaRPCTimeout)
	livePut, err := client.Put(liveContext, string(liveKey), string(liveValue))
	cancelLive()
	if err != nil {
		t.Fatalf("put live Java WatchService handoff event: %v", err)
	}
	if livePut == nil || livePut.Header == nil || livePut.Header.Revision <= updateRevision {
		t.Fatalf("live Put returned an invalid revision after replay %d: %v", updateRevision, livePut)
	}
	liveFixture := []m4WatchJavaFixture{{key: liveKey, oldValue: liveValue}}
	assertM4WatchJavaClientBatch(t, watchContext, responses,
		"clientv3 live handoff", liveFixture, 0, livePut.Header.Revision, false)
}

func assertM4WatchJavaClientBatch(t *testing.T, ctx context.Context,
	responses clientv3.WatchChan, label string, fixtures []m4WatchJavaFixture,
	initialRevision, revision int64, isUpdate bool) {
	t.Helper()
	response, err := receiveM4WatchJavaClientBatch(ctx, responses)
	if err != nil {
		t.Fatalf("receive %s revision: %v", label, err)
	}
	if response.Header.Revision != revision {
		t.Fatalf("%s revision = %d, want %d", label, response.Header.Revision, revision)
	}
	if err := verifyM4WatchJavaClientEvents(
		label, response.Events, fixtures, initialRevision, revision, isUpdate); err != nil {
		t.Fatal(err)
	}
}

func receiveM4WatchJavaClientBatch(ctx context.Context,
	responses clientv3.WatchChan) (clientv3.WatchResponse, error) {
	for {
		select {
		case response, open := <-responses:
			if !open {
				return clientv3.WatchResponse{}, fmt.Errorf("clientv3 Watch channel closed before a complete revision")
			}
			if err := response.Err(); err != nil {
				return clientv3.WatchResponse{}, fmt.Errorf("clientv3 Watch failed: %w", err)
			}
			if response.Created {
				if len(response.Events) != 0 {
					return clientv3.WatchResponse{}, fmt.Errorf("clientv3 Created response included events")
				}
				continue
			}
			if len(response.Events) == 0 {
				return clientv3.WatchResponse{}, fmt.Errorf("clientv3 Watch returned an empty non-created response")
			}
			return response, nil
		case <-ctx.Done():
			return clientv3.WatchResponse{}, fmt.Errorf("wait for clientv3 Watch response: %w", ctx.Err())
		}
	}
}

func verifyM4WatchJavaClientEvents(label string, events []*clientv3.Event,
	fixtures []m4WatchJavaFixture, initialRevision, revision int64, isUpdate bool) error {
	protobufEvents := make([]*mvccpb.Event, len(events))
	for index, event := range events {
		if event != nil {
			protobufEvents[index] = (*mvccpb.Event)(event)
		}
	}
	return verifyM4WatchJavaEvents(label, protobufEvents, fixtures,
		initialRevision, revision, isUpdate)
}

func verifyM4WatchJavaEvents(label string, events []*mvccpb.Event,
	fixtures []m4WatchJavaFixture, initialRevision, revision int64, isUpdate bool) error {
	if len(events) != len(fixtures) {
		return fmt.Errorf("%s returned %d events, want %d", label, len(events), len(fixtures))
	}
	for index, fixture := range fixtures {
		event := events[index]
		if event == nil || event.Type != mvccpb.PUT || event.Kv == nil {
			return fmt.Errorf("%s event %d is missing its PUT key-value", label, index)
		}
		if !bytes.Equal(event.Kv.Key, fixture.key) {
			return fmt.Errorf("%s event %d key = %q, want %q",
				label, index, event.Kv.Key, fixture.key)
		}

		wantValue := fixture.oldValue
		wantCreateRevision := fixture.initialRevision
		if wantCreateRevision == 0 {
			wantCreateRevision = revision
		}
		wantVersion := int64(1)
		if isUpdate {
			wantValue = fixture.updatedValue
			wantCreateRevision = fixture.initialRevision
			wantVersion = 2
		}
		if !bytes.Equal(event.Kv.Value, wantValue) || event.Kv.CreateRevision != wantCreateRevision ||
			event.Kv.ModRevision != revision || event.Kv.Version != wantVersion {
			return fmt.Errorf("%s event %d value or KV revisions do not match revision %d",
				label, index, revision)
		}
		if err := verifyM4WatchJavaPreviousValue(label, index, event, fixture,
			initialRevision, isUpdate); err != nil {
			return err
		}
	}
	return nil
}

func verifyM4WatchJavaPreviousValue(label string, index int, event *mvccpb.Event,
	fixture m4WatchJavaFixture, initialRevision int64, isUpdate bool) error {
	if !isUpdate {
		if event.PrevKv != nil {
			return fmt.Errorf("%s event %d unexpectedly has a previous value", label, index)
		}
		return nil
	}
	previous := event.PrevKv
	if previous == nil || !bytes.Equal(previous.Key, fixture.key) ||
		!bytes.Equal(previous.Value, fixture.oldValue) ||
		previous.CreateRevision != fixture.initialRevision ||
		previous.ModRevision != fixture.initialRevision || previous.Version != 1 {
		return fmt.Errorf("%s event %d has an incorrect previous value", label, index)
	}
	return nil
}
