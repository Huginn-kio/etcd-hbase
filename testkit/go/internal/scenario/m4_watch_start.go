package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type watchStartCase struct {
	startRevision int64
	name          string
	watchID       int64
}

var m4WatchStartCases = []watchStartCase{
	{startRevision: -2, name: "minus-two", watchID: 41},
	{startRevision: -1, name: "minus-one", watchID: 42},
	{startRevision: 0, name: "zero", watchID: 43},
	{startRevision: 0, name: "watch-id-minus-one", watchID: -1},
}

const m4WatchDuplicateIDReason = "mvcc: duplicate watch ID provided on the WatchStream"

// RunM4NativeWatchStart records the J45 raw-proto start-revision matrix against pinned etcd.
func RunM4NativeWatchStart(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || output == nil || dataDir == "" {
		return errors.New("native M4 Watch start run requires context, data directory, and trace output")
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return fmt.Errorf("create isolated native data directory: %w", err)
	}
	server, err := startEmbeddedWithElectionTimeout(ctx, dataDir, m4NativeElectionTimeoutMs)
	if err != nil {
		return err
	}
	defer server.close()
	return runM4NativeWatchStart(ctx, server.client, prefix, output)
}

// RunM4WatchStartEndpoint checks one expected Watch checkpoint against a local service endpoint.
func RunM4WatchStartEndpoint(ctx context.Context, client *clientv3.Client, prefix, phase string,
	revision, compactRevision int64, output io.Writer) error {
	if err := ValidateM4Lease("local-service", prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil || revision < 1 || compactRevision < -1 ||
		(phase != "initial" && phase != "compacted") ||
		(phase == "initial" && compactRevision != -1) ||
		(phase == "compacted" && (compactRevision < 1 || compactRevision > revision)) {
		return errors.New("local Watch start run requires a valid phase and revision checkpoint")
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: "local-service"}
	watchClient := etcdserverpb.NewWatchClient(client.ActiveConnection())
	return runWatchStartPhase(ctx, runner, watchClient, prefix, phase, revision, compactRevision)
}

func runM4NativeWatchStart(ctx context.Context, client *clientv3.Client, prefix string, output io.Writer) error {
	if err := ValidateM4Lease("native-etcd", prefix); err != nil {
		return err
	}
	if ctx == nil || client == nil || output == nil {
		return errors.New("M4 Watch start scenario requires context, client, and trace output")
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64),
		evidenceScope: "native-etcd"}
	watchClient := etcdserverpb.NewWatchClient(client.ActiveConnection())
	initialRevision, err := seedM4WatchStartHistory(ctx, runner, prefix)
	if err != nil {
		return err
	}

	if err := runWatchStartPhase(ctx, runner, watchClient, prefix, "initial", initialRevision, -1); err != nil {
		return err
	}
	if err := runNegativeWatchLive(ctx, runner, watchClient, prefix, initialRevision); err != nil {
		return err
	}
	compactRevision, err := compactWatchFixture(ctx, runner, client, prefix)
	if err != nil {
		return err
	}
	return runWatchStartPhase(ctx, runner, watchClient, prefix, "compacted", compactRevision, compactRevision)
}

func seedM4WatchStartHistory(ctx context.Context, runner *runner, prefix string) (int64, error) {
	key := []byte(prefix + "initial/minus-one")
	request := &etcdserverpb.PutRequest{Key: key, Value: []byte("j45-watch-history")}
	result, err := runner.call(ctx, "watch-start-history-put", "etcdserverpb.KV/Put", request,
		func(requestContext context.Context) (protoMessage, error) {
			return etcdserverpb.NewKVClient(runner.client.ActiveConnection()).Put(requestContext, request)
		})
	if err != nil {
		return 0, err
	}
	response, ok := result.(*etcdserverpb.PutResponse)
	if !ok || response.Header == nil || response.Header.Revision <= 1 {
		return 0, errors.New("Watch history fixture Put did not advance the native revision")
	}
	return response.Header.Revision, nil
}

func runNegativeWatchLive(ctx context.Context, runner *runner,
	watchClient etcdserverpb.WatchClient, prefix string, initialRevision int64) error {
	const watchID int64 = 45
	key := []byte(prefix + "initial/minus-one")
	request := watchCreateRequest(watchID, key, -1)
	streamContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := watchClient.Watch(streamContext)
	if err != nil {
		return fmt.Errorf("open negative-start live Watch stream: %w", err)
	}
	started := time.Now()
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send negative-start live Watch create: %w", err)
	}
	created, err := receiveWatchResponse(stream, "negative-start live Watch create")
	if err != nil {
		return err
	}
	if err := runner.record("watch-negative-live-created", "etcdserverpb.Watch/Watch",
		request, created, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if err := requireWatchCreated(created, watchID, initialRevision); err != nil {
		return fmt.Errorf("negative-start live Watch: %w", err)
	}
	if err := waitForNegativeWatchSync(ctx, runner, stream, initialRevision, started); err != nil {
		return err
	}

	const value = "j45-watch-live"
	putRequest := &etcdserverpb.PutRequest{Key: key, Value: []byte(value)}
	putResult, err := runner.call(ctx, "watch-negative-live-put", "etcdserverpb.KV/Put", putRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return etcdserverpb.NewKVClient(runner.client.ActiveConnection()).Put(requestContext, putRequest)
		})
	if err != nil {
		return err
	}
	putResponse, ok := putResult.(*etcdserverpb.PutResponse)
	if !ok || putResponse.Header == nil || putResponse.Header.Revision != initialRevision+1 {
		return errors.New("negative-start live fixture Put did not advance exactly one revision")
	}

	response, err := receiveWatchResponse(stream, "negative-start live Watch event")
	if err != nil {
		return err
	}
	if err := runner.record("watch-negative-live-event", "etcdserverpb.Watch/Watch",
		request, response, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if response == nil || response.Header == nil || response.Header.Revision != putResponse.Header.Revision ||
		response.WatchId != watchID || response.Created || response.Canceled || len(response.Events) != 1 {
		return errors.New("negative-start Watch did not deliver exactly the post-create event")
	}
	event := response.Events[0]
	if event == nil || event.Type != mvccpb.PUT || event.Kv == nil ||
		!bytes.Equal(event.Kv.Key, key) || !bytes.Equal(event.Kv.Value, []byte(value)) ||
		event.Kv.ModRevision != putResponse.Header.Revision {
		return errors.New("negative-start Watch delivered an old or mismatched event")
	}
	return cancelWatchID(runner, stream, "watch-negative-live-cancelled", watchID,
		putResponse.Header.Revision, started)
}

func waitForNegativeWatchSync(ctx context.Context, runner *runner,
	stream etcdserverpb.Watch_WatchClient, revision int64, started time.Time) error {
	const syncSettleDelay = 500 * time.Millisecond
	// etcd v3.5.23 returns Created before its background watcher sync. Wait several sync periods,
	// then use one ProgressRequest response as the barrier so the trace keeps a 1:1 request/reply.
	timer := time.NewTimer(syncSettleDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	request := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
		ProgressRequest: &etcdserverpb.WatchProgressRequest{},
	}}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send native Watch progress barrier: %w", err)
	}
	response, err := receiveWatchResponse(stream, "negative-start sync progress barrier")
	if err != nil {
		return err
	}
	if response.Header == nil || response.Header.Revision != revision || response.WatchId != -1 ||
		response.Created || response.Canceled || len(response.Events) != 0 {
		return errors.New("native Watch did not report the synced negative-start progress barrier")
	}
	return runner.record("watch-negative-live-progress", "etcdserverpb.Watch/Watch",
		request, response, nil, nil, time.Since(started))
}

func compactWatchFixture(ctx context.Context, runner *runner, client *clientv3.Client, prefix string) (int64, error) {
	kv := etcdserverpb.NewKVClient(client.ActiveConnection())
	key := []byte(prefix + "compact-fixture")
	putRequest := &etcdserverpb.PutRequest{Key: key, Value: []byte("j45-watch-compact")}
	putResult, err := runner.call(ctx, "watch-compact-fixture-put", "etcdserverpb.KV/Put", putRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Put(requestContext, putRequest)
		})
	if err != nil {
		return 0, err
	}
	putResponse, ok := putResult.(*etcdserverpb.PutResponse)
	if !ok || putResponse.Header == nil || putResponse.Header.Revision <= 1 {
		return 0, errors.New("Watch compaction fixture Put did not advance the native revision")
	}

	rangeRequest := &etcdserverpb.RangeRequest{Key: key}
	rangeResult, err := runner.call(ctx, "watch-compact-fixture-range", "etcdserverpb.KV/Range", rangeRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Range(requestContext, rangeRequest)
		})
	if err != nil {
		return 0, err
	}
	rangeResponse, ok := rangeResult.(*etcdserverpb.RangeResponse)
	if !ok || rangeResponse.Header == nil || rangeResponse.Header.Revision != putResponse.Header.Revision ||
		len(rangeResponse.Kvs) != 1 || string(rangeResponse.Kvs[0].Value) != "j45-watch-compact" {
		return 0, errors.New("Watch compaction fixture Range did not confirm the selected revision")
	}

	compactRequest := &etcdserverpb.CompactionRequest{Revision: rangeResponse.Header.Revision}
	compactResult, err := runner.call(ctx, "watch-compact-fixture-compact", "etcdserverpb.KV/Compact", compactRequest,
		func(requestContext context.Context) (protoMessage, error) {
			return kv.Compact(requestContext, compactRequest)
		})
	if err != nil {
		return 0, err
	}
	compactResponse, ok := compactResult.(*etcdserverpb.CompactionResponse)
	if !ok || compactResponse.Header == nil || compactResponse.Header.Revision != rangeResponse.Header.Revision {
		return 0, errors.New("Watch compaction fixture response changed the selected revision")
	}
	return compactRequest.Revision, nil
}

func runWatchStartPhase(ctx context.Context, runner *runner, watchClient etcdserverpb.WatchClient,
	prefix, phase string, revision, compactRevision int64) error {
	streamContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := watchClient.Watch(streamContext)
	if err != nil {
		return fmt.Errorf("open raw-proto %s Watch stream: %w", phase, err)
	}
	for _, startCase := range m4WatchStartCases {
		key := []byte(prefix + phase + "/" + startCase.name)
		request := watchCreateRequest(startCase.watchID, key, startCase.startRevision)
		baseID := phase + "-" + startCase.name
		started := time.Now()
		if err := stream.Send(request); err != nil {
			return fmt.Errorf("send %s Watch create: %w", baseID, err)
		}
		created, err := receiveWatchResponse(stream, baseID+" create")
		if err != nil {
			return err
		}
		if err := runner.record(baseID+"-created", "etcdserverpb.Watch/Watch", request,
			created, nil, nil, time.Since(started)); err != nil {
			return err
		}
		if err := requireWatchCreated(created, startCase.watchID, revision); err != nil {
			return fmt.Errorf("%s: %w", baseID, err)
		}

		isCompacted := startCase.startRevision != 0 && startCase.startRevision < compactRevision
		if isCompacted {
			compacted, err := receiveWatchResponse(stream, baseID+" compaction")
			if err != nil {
				return err
			}
			if err := runner.record(baseID+"-compacted", "etcdserverpb.Watch/Watch", request,
				compacted, nil, nil, time.Since(started)); err != nil {
				return err
			}
			if err := requireWatchCompacted(compacted, startCase.watchID, compactRevision); err != nil {
				return fmt.Errorf("%s: %w", baseID, err)
			}
			if err := verifyCompactedWatchIDLifecycle(runner, stream, baseID, key,
				startCase.watchID, revision, started); err != nil {
				return err
			}
			continue
		}

		if err := cancelWatchID(runner, stream, baseID+"-cancelled", startCase.watchID,
			revision, started); err != nil {
			return err
		}
	}
	if phase == "initial" {
		if err := runFutureWatchStart(runner, stream, prefix, revision); err != nil {
			return err
		}
	}
	// The native Watch server keeps the RPC open after a client half-close; the
	// scoped context cancellation below is the stream-level cleanup signal.
	return nil
}

func runFutureWatchStart(runner *runner, stream etcdserverpb.Watch_WatchClient,
	prefix string, currentRevision int64) error {
	const maximumInt64 = 1<<63 - 1
	if currentRevision == maximumInt64 {
		return errors.New("cannot construct a future Watch revision at MaxInt64")
	}
	const watchID int64 = 44
	startRevision := currentRevision + 1
	key := []byte(prefix + "initial/future")
	request := watchCreateRequest(watchID, key, startRevision)
	caseID := "initial-future"
	started := time.Now()
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send %s Watch create: %w", caseID, err)
	}
	created, err := receiveWatchResponse(stream, caseID+" create")
	if err != nil {
		return err
	}
	if err := runner.record(caseID+"-created", "etcdserverpb.Watch/Watch",
		request, created, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if err := requireWatchCreated(created, watchID, currentRevision); err != nil {
		return fmt.Errorf("%s: %w", caseID, err)
	}
	return cancelWatchID(runner, stream, caseID+"-cancelled", watchID,
		currentRevision, started)
}

func verifyCompactedWatchIDLifecycle(runner *runner, stream etcdserverpb.Watch_WatchClient,
	baseID string, key []byte, watchID, revision int64, started time.Time) error {
	duplicateRequest := watchCreateRequest(watchID, key, 0)
	if err := stream.Send(duplicateRequest); err != nil {
		return fmt.Errorf("send duplicate %s Watch create: %w", baseID, err)
	}
	duplicate, err := receiveWatchResponse(stream, baseID+" duplicate ID")
	if err != nil {
		return err
	}
	if err := runner.record(baseID+"-duplicate-id", "etcdserverpb.Watch/Watch", duplicateRequest,
		duplicate, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if duplicate == nil || duplicate.Header == nil || duplicate.Header.Revision != revision ||
		duplicate.WatchId != -1 || !duplicate.Created || !duplicate.Canceled ||
		duplicate.CancelReason != m4WatchDuplicateIDReason || len(duplicate.Events) != 0 {
		return fmt.Errorf("%s duplicate Watch ID response differs from native behavior", baseID)
	}

	if err := cancelWatchID(runner, stream, baseID+"-cancel-id", watchID, revision, started); err != nil {
		return err
	}

	reuseRequest := watchCreateRequest(watchID, key, 0)
	if err := stream.Send(reuseRequest); err != nil {
		return fmt.Errorf("send reused %s Watch create: %w", baseID, err)
	}
	reused, err := receiveWatchResponse(stream, baseID+" reused ID")
	if err != nil {
		return err
	}
	if err := runner.record(baseID+"-reused-created", "etcdserverpb.Watch/Watch", reuseRequest,
		reused, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if err := requireWatchCreated(reused, watchID, revision); err != nil {
		return fmt.Errorf("%s reused ID: %w", baseID, err)
	}
	return cancelWatchID(runner, stream, baseID+"-reused-cancelled", watchID, revision, started)
}

func cancelWatchID(runner *runner, stream etcdserverpb.Watch_WatchClient,
	caseID string, watchID, revision int64, started time.Time) error {
	request := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: watchID},
	}}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send Watch cancel for ID %d: %w", watchID, err)
	}
	response, err := receiveWatchResponse(stream, caseID)
	if err != nil {
		return err
	}
	if err := runner.record(caseID, "etcdserverpb.Watch/Watch", request,
		response, nil, nil, time.Since(started)); err != nil {
		return err
	}
	if response == nil || response.Header == nil || response.Header.Revision != revision ||
		response.WatchId != watchID || !response.Canceled || response.Created ||
		response.CompactRevision != 0 || response.CancelReason != "" || len(response.Events) != 0 {
		return fmt.Errorf("Watch cancel for ID %d did not return the native cancellation acknowledgement", watchID)
	}
	return nil
}

func watchCreateRequest(watchID int64, key []byte, startRevision int64) *etcdserverpb.WatchRequest {
	return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: watchID, StartRevision: startRevision,
		},
	}}
}

func receiveWatchResponse(stream etcdserverpb.Watch_WatchClient, operation string) (*etcdserverpb.WatchResponse, error) {
	response, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receive %s response: %w", operation, err)
	}
	if response == nil {
		return nil, fmt.Errorf("receive %s response: native etcd returned nil", operation)
	}
	return response, nil
}

func requireWatchCreated(response *etcdserverpb.WatchResponse, watchID, revision int64) error {
	if response == nil || response.Header == nil || response.Header.Revision != revision ||
		response.WatchId != watchID || !response.Created || response.Canceled ||
		response.CompactRevision != 0 || response.CancelReason != "" || len(response.Events) != 0 {
		return errors.New("create response did not match the native Created contract")
	}
	return nil
}

func requireWatchCompacted(response *etcdserverpb.WatchResponse,
	watchID, compactRevision int64) error {
	if response == nil || response.Header == nil || response.Header.Revision != 0 ||
		response.WatchId != watchID || response.Created || !response.Canceled ||
		response.CompactRevision != compactRevision || response.CancelReason != "" ||
		len(response.Events) != 0 {
		return fmt.Errorf("compaction response differs from native Canceled contract: response=%v want watch_id=%d header_revision=0 compact_revision=%d empty_cancel_reason",
			response, watchID, compactRevision)
	}
	return nil
}
