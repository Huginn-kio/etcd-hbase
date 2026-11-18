//go:build m4watchintegration

package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

const (
	m4CombinedLoadWrites              = 32
	m4CombinedLoadValueBytes          = 4 << 10
	m4CombinedLoadReplayWatchers      = 4
	m4CombinedLoadWatchCreateAttempts = 12
	// The runtime exposes nine regular request slots at the combined-load profile; two are held by
	// the live consumers, so the bounded probe must try beyond that remainder to observe rejection.
	m4CombinedLoadSaturationWatchers  = 16
	m4CombinedLoadSlowWatchDrainLimit = m4CombinedLoadWrites + 8
	m4CombinedLoadWatchRetryBase      = 50 * time.Millisecond
	m4CombinedLoadWatchRetryMaximum   = time.Second
	m4CombinedLoadWatchResourceLimit  = "etcd-hbase: Watch resource limit exceeded"
	m4CombinedWatchDiagnosticLimit    = 256
	m4CombinedWatchDiagnosticBytes    = 4 << 10
)

var errM4CombinedWatchResourceLimit = errors.New(m4CombinedLoadWatchResourceLimit)

type m4CombinedWatchDiagnosticContextKey struct{}

// m4CombinedWatchDiagnostics records bounded, value-free Watch RPC metadata so a closed
// clientv3 channel can be distinguished from an application cancellation or a gRPC failure.
type m4CombinedWatchDiagnostics struct {
	mu            sync.Mutex
	records       []string
	recordedBytes int
}

func (diagnostics *m4CombinedWatchDiagnostics) TagRPC(
	ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if info.FullMethodName != "/etcdserverpb.Watch/Watch" {
		return ctx
	}
	return context.WithValue(ctx, m4CombinedWatchDiagnosticContextKey{}, true)
}

func (diagnostics *m4CombinedWatchDiagnostics) HandleRPC(
	ctx context.Context, rpcStats stats.RPCStats) {
	if ctx.Value(m4CombinedWatchDiagnosticContextKey{}) != true {
		return
	}
	switch event := rpcStats.(type) {
	case *stats.InPayload:
		response, ok := event.Payload.(*etcdserverpb.WatchResponse)
		if !ok {
			return
		}
		diagnostics.record(fmt.Sprintf(
			"response watch_id=%d created=%t canceled=%t compact_revision=%d revision=%d events=%d cancel_reason_bytes=%d",
			response.GetWatchId(), response.GetCreated(), response.GetCanceled(),
			response.GetCompactRevision(), response.GetHeader().GetRevision(),
			len(response.GetEvents()), len(response.GetCancelReason())))
	case *stats.OutPayload:
		request, ok := event.Payload.(*etcdserverpb.WatchRequest)
		if !ok {
			return
		}
		if create := request.GetCreateRequest(); create != nil {
			diagnostics.record(fmt.Sprintf(
				"request create start_revision=%d key_bytes=%d range_end_bytes=%d",
				create.GetStartRevision(), len(create.GetKey()), len(create.GetRangeEnd())))
		} else if cancel := request.GetCancelRequest(); cancel != nil {
			diagnostics.record(fmt.Sprintf("request cancel watch_id=%d", cancel.GetWatchId()))
		}
	case *stats.End:
		if event.Error == nil {
			diagnostics.record("gRPC Watch ended with status=OK")
			return
		}
		diagnostics.record(fmt.Sprintf("gRPC Watch ended with status=%s", status.Code(event.Error)))
	}
}

func (diagnostics *m4CombinedWatchDiagnostics) TagConn(
	ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*m4CombinedWatchDiagnostics) HandleConn(context.Context, stats.ConnStats) {}

func (diagnostics *m4CombinedWatchDiagnostics) record(record string) {
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if len(record) > m4CombinedWatchDiagnosticBytes-2 {
		record = record[:m4CombinedWatchDiagnosticBytes-2]
	}
	for len(diagnostics.records) > 0 &&
		(len(diagnostics.records) >= m4CombinedWatchDiagnosticLimit ||
			diagnostics.recordedBytes+len(record)+2 > m4CombinedWatchDiagnosticBytes) {
		removed := diagnostics.records[0]
		copy(diagnostics.records, diagnostics.records[1:])
		diagnostics.records = diagnostics.records[:len(diagnostics.records)-1]
		diagnostics.recordedBytes -= len(removed) + 2
	}
	diagnostics.records = append(diagnostics.records, record)
	diagnostics.recordedBytes += len(record) + 2
}

func (diagnostics *m4CombinedWatchDiagnostics) snapshot() string {
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if len(diagnostics.records) == 0 {
		return "no Watch RPC payloads or terminal status observed"
	}
	return strings.Join(append([]string(nil), diagnostics.records...), "; ")
}

func TestM4CombinedWatchDiagnosticsRedactsAndBoundsOutput(t *testing.T) {
	diagnostics := &m4CombinedWatchDiagnostics{}
	ctx := diagnostics.TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName: "/etcdserverpb.Watch/Watch",
	})
	privateMessage := strings.Repeat("private", 1024)
	diagnostics.HandleRPC(ctx, &stats.InPayload{Payload: &etcdserverpb.WatchResponse{
		Header:       &etcdserverpb.ResponseHeader{},
		CancelReason: privateMessage,
	}})
	diagnostics.HandleRPC(ctx, &stats.End{Error: status.Error(codes.Internal, privateMessage)})
	if output := diagnostics.snapshot(); strings.Contains(output, "private") {
		t.Fatalf("Watch diagnostics included raw termination text: %s", output)
	}

	for record := 0; record < m4CombinedWatchDiagnosticLimit*2; record++ {
		diagnostics.record(strings.Repeat("x", 128))
	}
	diagnostics.mu.Lock()
	count := len(diagnostics.records)
	recordedBytes := diagnostics.recordedBytes
	diagnostics.mu.Unlock()
	if count > m4CombinedWatchDiagnosticLimit {
		t.Fatalf("diagnostic record count = %d, limit = %d", count, m4CombinedWatchDiagnosticLimit)
	}
	if recordedBytes > m4CombinedWatchDiagnosticBytes {
		t.Fatalf("diagnostic bytes = %d, limit = %d", recordedBytes, m4CombinedWatchDiagnosticBytes)
	}
	if output := diagnostics.snapshot(); len(output) > m4CombinedWatchDiagnosticBytes {
		t.Fatalf("diagnostic output bytes = %d, limit = %d", len(output), m4CombinedWatchDiagnosticBytes)
	}
}

func TestM4CombinedWatchErrorDiagnosticsOmitRawStatusMessages(t *testing.T) {
	privateMessage := strings.Repeat("private-status-detail-", 256)
	response := clientv3.WatchResponse{Canceled: true, CompactRevision: 17}
	diagnostic := describeM4CombinedWatchError("Watch creation response",
		response, status.Error(codes.Internal, privateMessage))
	if strings.Contains(diagnostic.Error(), "private-status-detail") {
		t.Fatalf("Watch diagnostic exposed the raw gRPC status message: %s", diagnostic)
	}
	if !strings.Contains(diagnostic.Error(), "status=Internal") ||
		!strings.Contains(diagnostic.Error(), "CompactRevision=17") {
		t.Fatalf("Watch diagnostic omitted safe status metadata: %s", diagnostic)
	}
	if len(diagnostic.Error()) > m4CombinedWatchDiagnosticBytes {
		t.Fatalf("Watch diagnostic exceeded its byte bound: %d", len(diagnostic.Error()))
	}

	slowWatchDiagnostic := describeM4SlowWatchTerminalError(status.Error(codes.Internal, privateMessage))
	if strings.Contains(slowWatchDiagnostic.Error(), "private-status-detail") ||
		!strings.Contains(slowWatchDiagnostic.Error(), "status=Internal") {
		t.Fatalf("slow Watch diagnostic must preserve only the status code: %s", slowWatchDiagnostic)
	}
}

func TestM4WatchJavaServiceCombinedLeaseAndReplayLoad(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv(m4WatchJavaEndpointEnv))
	if endpoint == "" {
		t.Skipf("set %s to run the Java combined Lease and Watch integration test", m4WatchJavaEndpointEnv)
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

	lease := grantM4RegistrationLease(t, testContext, client, 15)
	defer revokeM4RegistrationLease(t, etcdserverpb.NewLeaseClient(client.ActiveConnection()), lease.ID)
	watchPrefix := prefix + "combined/"
	registrationKey := watchPrefix + "session"
	registrationValue := marshalM4MilvusSession(t, "querycoord", 9001, lease.ID)
	registration, err := client.Put(testContext, registrationKey, string(registrationValue),
		clientv3.WithLease(lease.ID))
	if err != nil || registration == nil || registration.Header == nil {
		t.Fatalf("register the combined-load Lease key: response=%v error=%v", registration, err)
	}

	listed, err := client.Get(testContext, watchPrefix, clientv3.WithPrefix())
	if err != nil || listed == nil || listed.Header == nil || len(listed.Kvs) != 1 {
		t.Fatalf("list the combined-load registration before Watch: response=%v error=%v", listed, err)
	}
	startRevision := listed.Header.Revision + 1

	fastContext, cancelFast := context.WithCancel(testContext)
	defer cancelFast()
	fastWatch := client.Watcher.Watch(fastContext, watchPrefix, clientv3.WithPrefix(),
		clientv3.WithRev(startRevision), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	awaitM4MilvusWatchCreated(t, testContext, fastWatch, "combined-load fast consumer")

	slowContext, cancelSlow := context.WithCancel(testContext)
	defer cancelSlow()
	slowStream, err := etcdserverpb.NewWatchClient(client.ActiveConnection()).Watch(slowContext)
	if err != nil {
		t.Fatalf("open the intentionally slow raw Watch stream: %v", err)
	}
	slowStreamClosed := false
	defer func() {
		if !slowStreamClosed {
			if closeErr := slowStream.CloseSend(); closeErr != nil && status.Code(closeErr) != codes.Canceled {
				t.Errorf("close the slow raw Watch stream: %v", closeErr)
			}
		}
	}()
	if err := slowStream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(watchPrefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(watchPrefix)),
			StartRevision: startRevision, PrevKv: true,
		},
	}}); err != nil {
		t.Fatalf("create the intentionally slow raw Watch: %v", err)
	}
	created, err := slowStream.Recv()
	if err != nil || created == nil || !created.Created {
		t.Fatalf("slow raw Watch did not reach Created: response=%v error=%v", created, err)
	}

	totalWrites := m4CombinedLoadWrites
	liveEvents := make(chan []*clientv3.Event, 1)
	liveFailures := make(chan error, 1)
	go func() {
		events, collectFailure := collectM4CombinedWatchEvents(testContext, fastWatch, totalWrites, nil)
		if collectFailure != nil {
			liveFailures <- collectFailure
			return
		}
		liveEvents <- events
	}()

	key := watchPrefix + "business/session"
	for completed := 1; completed <= totalWrites; completed++ {
		value := bytes.Repeat([]byte{byte(completed), byte(completed >> 8)},
			m4CombinedLoadValueBytes/2)
		requestContext, cancelRequest := context.WithTimeout(testContext, m4WatchJavaRPCTimeout)
		_, putFailure := client.Put(requestContext, key, string(value))
		cancelRequest()
		if putFailure != nil {
			t.Fatalf("write business key %q iteration %d: %v", key, completed, putFailure)
		}
		if completed%8 == 0 {
			keepAliveContext, cancelKeepAlive := context.WithTimeout(testContext, m4WatchJavaRPCTimeout)
			response, keepAliveFailure := client.KeepAliveOnce(keepAliveContext, lease.ID)
			cancelKeepAlive()
			if keepAliveFailure != nil || response == nil || response.TTL <= 0 || response.ID != lease.ID {
				t.Fatalf("renew Lease during combined business and Watch load: response=%v error=%v",
					response, keepAliveFailure)
			}
		}
		select {
		case failure := <-liveFailures:
			t.Fatalf("fast Watch consumer failed while the raw peer was backpressured: %v", failure)
		default:
		}
	}

	var observedLiveEvents []*clientv3.Event
	select {
	case failure := <-liveFailures:
		t.Fatalf("fast Watch consumer failed while the raw peer was backpressured: %v", failure)
	case observedLiveEvents = <-liveEvents:
	case <-testContext.Done():
		t.Fatalf("fast Watch did not receive all %d live events: %v", totalWrites, testContext.Err())
	}
	if err := validateM4CombinedLoadEvents(observedLiveEvents, watchPrefix, totalWrites); err != nil {
		t.Fatalf("fast Watch lost or reordered events while the peer was slow: %v", err)
	}
	resourceRejections, err := exerciseM4CombinedLoadWatchLimit(testContext, endpoint,
		watchPrefix, startRevision, m4CombinedLoadSaturationWatchers)
	if err != nil {
		t.Fatal(err)
	}

	if err := slowStream.CloseSend(); err != nil {
		t.Fatalf("half-close the slow raw Watch stream before cancel: %v", err)
	}
	slowStreamClosed = true
	cancelSlow()
	if err := awaitM4SlowWatchCancellation(slowStream); err != nil {
		t.Fatalf("wait for the slow raw Watch stream to observe cancellation: %v", err)
	}
	cancelFast()
	// Do not treat transient admission pressure from the live streams as a replay failure. The
	// client waits for both subscriptions and their server-side cleanup to drain before recovery.
	if err := client.Watcher.Close(); err != nil {
		t.Fatalf("drain the slow and fast Watch subscriptions before replay: %v", err)
	}
	watchDiagnostics := &m4CombinedWatchDiagnostics{}
	recoveryClient := newM4WatchJavaClientWithDialOptions(t, endpoint,
		grpc.WithStatsHandler(watchDiagnostics))
	rejectedCreates, err := verifyM4CombinedReplayRecovery(testContext, recoveryClient,
		watchDiagnostics, watchPrefix, startRevision, totalWrites,
		m4CombinedLoadReplayWatchers, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("observed %d explicit Watch limit rejection(s) and completed %d bounded history replays "+
		"after %d recovery retry/retries", resourceRejections, m4CombinedLoadReplayWatchers,
		rejectedCreates)
	if err := verifyM4CombinedLoadFinalState(testContext, recoveryClient, watchPrefix, registrationKey,
		registrationValue, lease.ID, totalWrites, observedLiveEvents); err != nil {
		t.Fatal(err)
	}
	if err := recoveryClient.Watcher.Close(); err != nil {
		t.Fatalf("wait for the client Watch streams to close: %v", err)
	}
	cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCleanup()
	cleanupAttempts, err := deleteM4CombinedLoadPrefixAfterDrain(cleanupContext, recoveryClient, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("deleted the combined-load prefix after Watch stream drain in %d bounded attempts",
		cleanupAttempts)
}

func awaitM4SlowWatchCancellation(stream etcdserverpb.Watch_WatchClient) error {
	result := make(chan error, 1)
	go func() {
		for drained := 0; ; drained++ {
			_, err := stream.Recv()
			if err != nil {
				result <- err
				return
			}
			if drained >= m4CombinedLoadSlowWatchDrainLimit {
				result <- fmt.Errorf("slow Watch produced more than %d queued responses before termination",
					m4CombinedLoadSlowWatchDrainLimit)
				return
			}
		}
	}()
	timer := time.NewTimer(m4WatchJavaRPCTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		if err == nil {
			return fmt.Errorf("slow Watch produced a response without a terminal status")
		}
		if status.Code(err) == codes.Canceled || errors.Is(err, io.EOF) {
			return nil
		}
		return describeM4SlowWatchTerminalError(err)
	case <-timer.C:
		return fmt.Errorf("slow Watch did not terminate within %s after cancellation",
			m4WatchJavaRPCTimeout)
	}
}

func describeM4SlowWatchTerminalError(err error) error {
	return fmt.Errorf("slow Watch returned unexpected terminal error (status=%s)", status.Code(err))
}

func exerciseM4CombinedLoadWatchLimit(ctx context.Context, endpoint string, watchPrefix string,
	startRevision int64, maximumWatchers int) (int, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return 0, fmt.Errorf("connect the bounded Watch overload probe: %w", err)
	}
	clientClosed := false
	defer func() {
		if !clientClosed {
			_ = client.Close()
		}
	}()
	watchContexts := make([]context.CancelFunc, 0, maximumWatchers)
	watchChannels := make([]clientv3.WatchChan, 0, maximumWatchers)
	defer func() {
		for _, cancelWatch := range watchContexts {
			cancelWatch()
		}
	}()
	resourceRejections := 0
	for watcherIndex := 0; watcherIndex < maximumWatchers; watcherIndex++ {
		watchContext, cancelWatch := context.WithCancel(ctx)
		watchContexts = append(watchContexts, cancelWatch)
		watch := client.Watcher.Watch(watchContext, watchPrefix, clientv3.WithPrefix(),
			clientv3.WithRev(startRevision), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
		if err := awaitM4CombinedWatchCreated(ctx, watch); err != nil {
			cancelWatch()
			if !isM4CombinedWatchResourceLimitError(err) {
				return resourceRejections, fmt.Errorf(
					"bounded overload probe watcher %d failed before Created: %w", watcherIndex, err)
			}
			resourceRejections++
			break
		}
		watchChannels = append(watchChannels, watch)
	}
	if resourceRejections == 0 {
		for watcherIndex, watch := range watchChannels {
			if err := awaitM4CombinedWatchResourceLimit(ctx, watch); err != nil {
				return resourceRejections, fmt.Errorf(
					"bounded overload probe watcher %d did not expose the configured resource limit: %w",
					watcherIndex, err)
			}
			resourceRejections++
			break
		}
	}
	if resourceRejections == 0 {
		return 0, fmt.Errorf("bounded overload probe created %d Watches without observing a resource limit",
			maximumWatchers)
	}
	for _, cancelWatch := range watchContexts {
		cancelWatch()
	}
	if err := client.Watcher.Close(); err != nil {
		return resourceRejections, fmt.Errorf("close bounded overload probe Watch streams: %w", err)
	}
	if err := client.Close(); err != nil {
		return resourceRejections, fmt.Errorf("close bounded overload probe client: %w", err)
	}
	clientClosed = true
	return resourceRejections, nil
}

func awaitM4CombinedWatchResourceLimit(ctx context.Context, watch clientv3.WatchChan) error {
	for {
		select {
		case response, open := <-watch:
			if !open {
				return fmt.Errorf("Watch closed before its resource-limit response")
			}
			if isM4CombinedWatchResourceLimitResponse(response) {
				return nil
			}
			if err := response.Err(); err != nil {
				return describeM4CombinedWatchError("Watch returned an unexpected response",
					response, err)
			}
			if response.Canceled {
				return fmt.Errorf("Watch was canceled without the expected resource-limit identity")
			}
		case <-ctx.Done():
			return fmt.Errorf("waiting for bounded Watch resource rejection: %w", ctx.Err())
		}
	}
}

func verifyM4CombinedReplayRecovery(ctx context.Context, client *clientv3.Client,
	diagnostics *m4CombinedWatchDiagnostics, watchPrefix string,
	startRevision int64, expectedEvents int, replayCount int,
	leaseID clientv3.LeaseID) (int, error) {
	totalRejected := 0
	for replayIndex := 0; replayIndex < replayCount; replayIndex++ {
		events, rejected, err := replayM4CombinedHistory(ctx, client, watchPrefix,
			diagnostics, startRevision, expectedEvents, replayIndex)
		totalRejected += rejected
		if err != nil {
			return totalRejected, err
		}
		if err := validateM4CombinedLoadEvents(events, watchPrefix, expectedEvents); err != nil {
			return totalRejected, fmt.Errorf("replay %d lost or reordered committed history: %w",
				replayIndex, err)
		}
		if err := renewM4CombinedLoadLease(ctx, client, leaseID); err != nil {
			return totalRejected, err
		}
	}
	return totalRejected, nil
}

func replayM4CombinedHistory(ctx context.Context, client *clientv3.Client, watchPrefix string,
	diagnostics *m4CombinedWatchDiagnostics, startRevision int64, expectedEvents int,
	replayIndex int) ([]*clientv3.Event, int, error) {
	rejectedAttempts := 0
	for attempt := 0; attempt < m4CombinedLoadWatchCreateAttempts; attempt++ {
		watchContext, cancelWatch := context.WithCancel(ctx)
		watch := client.Watcher.Watch(watchContext, watchPrefix, clientv3.WithPrefix(),
			clientv3.WithRev(startRevision), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
		if err := awaitM4CombinedWatchCreated(ctx, watch); err != nil {
			cancelWatch()
			if !isM4CombinedWatchResourceLimitError(err) {
				return nil, rejectedAttempts, fmt.Errorf(
					"reconnect %d failed before Created: %w", replayIndex, err)
			}
			rejectedAttempts++
		} else {
			events, err := collectM4CombinedWatchEvents(ctx, watch, expectedEvents, diagnostics)
			cancelWatch()
			if err == nil {
				return events, rejectedAttempts, nil
			}
			if !isM4CombinedWatchResourceLimitError(err) {
				return nil, rejectedAttempts, fmt.Errorf(
					"reconnect %d failed after starting at revision %d: %w",
					replayIndex, startRevision, err)
			}
			// Discard partial delivery. The next watcher restarts at the unchanged inclusive
			// revision so the validated result contains every event exactly once.
			rejectedAttempts++
		}
		if attempt+1 == m4CombinedLoadWatchCreateAttempts {
			return nil, rejectedAttempts, fmt.Errorf(
				"reconnect %d exceeded %d bounded replay attempts from revision %d",
				replayIndex, m4CombinedLoadWatchCreateAttempts, startRevision)
		}
		if err := waitM4CombinedWatchRetry(ctx, combinedWatchRetryDelay(rejectedAttempts)); err != nil {
			return nil, rejectedAttempts, fmt.Errorf(
				"reconnect %d canceled while backing off from revision %d: %w",
				replayIndex, startRevision, err)
		}
	}
	return nil, rejectedAttempts, fmt.Errorf("reconnect %d exhausted bounded replay attempts", replayIndex)
}

func awaitM4CombinedWatchCreated(ctx context.Context, watch clientv3.WatchChan) error {
	for {
		select {
		case response, open := <-watch:
			if !open {
				return fmt.Errorf("Watch closed before Created")
			}
			if isM4CombinedWatchResourceLimitResponse(response) {
				return errM4CombinedWatchResourceLimit
			}
			if err := response.Err(); err != nil {
				return describeM4CombinedWatchError("Watch creation response", response, err)
			}
			if response.Created {
				return nil
			}
			if response.Canceled {
				return fmt.Errorf("Watch was canceled before Created")
			}
		case <-ctx.Done():
			return fmt.Errorf("waiting for Watch Created: %w", ctx.Err())
		}
	}
}

func isM4CombinedWatchResourceLimitError(err error) bool {
	return errors.Is(err, errM4CombinedWatchResourceLimit)
}

func describeM4CombinedWatchError(operation string, response clientv3.WatchResponse,
	responseError error) error {
	return fmt.Errorf("%s (Created=%t, Canceled=%t, CompactRevision=%d, status=%s)",
		operation, response.Created, response.Canceled, response.CompactRevision,
		status.Code(responseError))
}

func isM4CombinedWatchResourceLimitResponse(response clientv3.WatchResponse) bool {
	// clientv3 v3.5.23 consumes the server's Created+Canceled rejection while
	// creating its substream, then reports it as a Canceled response with closeErr.
	if !response.Canceled || response.CompactRevision != 0 {
		return false
	}
	err := response.Err()
	return err != nil && status.Code(err) == codes.Unknown &&
		err.Error() == m4CombinedLoadWatchResourceLimit
}

func combinedWatchRetryDelay(rejectedCreates int) time.Duration {
	delay := m4CombinedLoadWatchRetryBase
	for retry := 1; retry < rejectedCreates && delay < m4CombinedLoadWatchRetryMaximum; retry++ {
		delay *= 2
	}
	if delay > m4CombinedLoadWatchRetryMaximum {
		return m4CombinedLoadWatchRetryMaximum
	}
	return delay
}

func waitM4CombinedWatchRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func collectM4CombinedWatchEvents(ctx context.Context, watch clientv3.WatchChan,
	expectedEvents int, diagnostics *m4CombinedWatchDiagnostics) ([]*clientv3.Event, error) {
	events := make([]*clientv3.Event, 0, expectedEvents)
	for len(events) < expectedEvents {
		select {
		case response, open := <-watch:
			if !open {
				diagnosticsSnapshot := "not configured"
				if diagnostics != nil {
					diagnosticsSnapshot = diagnostics.snapshot()
				}
				return nil, fmt.Errorf("Watch closed after %d/%d events (context error=%v; gRPC diagnostics: %s)",
					len(events), expectedEvents, ctx.Err(), diagnosticsSnapshot)
			}
			if isM4CombinedWatchResourceLimitResponse(response) {
				return nil, fmt.Errorf("Watch resource limit after %d/%d events: %w",
					len(events), expectedEvents, errM4CombinedWatchResourceLimit)
			}
			if responseError := response.Err(); responseError != nil {
				return nil, describeM4CombinedWatchError(
					fmt.Sprintf("Watch failed after %d/%d events", len(events), expectedEvents),
					response, responseError)
			}
			if response.Created {
				continue
			}
			for _, event := range response.Events {
				events = append(events, event)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("Watch event collection timed out: %w", ctx.Err())
		}
	}
	if len(events) != expectedEvents {
		return nil, fmt.Errorf("Watch returned %d events, expected %d", len(events), expectedEvents)
	}
	return events, nil
}

func renewM4CombinedLoadLease(ctx context.Context, client *clientv3.Client,
	leaseID clientv3.LeaseID) error {
	keepAliveContext, cancelKeepAlive := context.WithTimeout(ctx, m4WatchJavaRPCTimeout)
	defer cancelKeepAlive()
	response, err := client.KeepAliveOnce(keepAliveContext, leaseID)
	if err != nil {
		return fmt.Errorf("renew registration Lease between historical replays: response=%v error=%w",
			response, err)
	}
	if response == nil || response.TTL <= 0 || response.ID != leaseID {
		return fmt.Errorf("renew registration Lease between historical replays: response=%v", response)
	}
	return nil
}

func verifyM4CombinedLoadFinalState(ctx context.Context, client *clientv3.Client,
	watchPrefix string, registrationKey string, registrationValue []byte,
	leaseID clientv3.LeaseID, expectedWrites int, liveEvents []*clientv3.Event) error {
	requestContext, cancelRequest := context.WithTimeout(ctx, m4WatchJavaRPCTimeout)
	defer cancelRequest()
	response, err := client.Get(requestContext, watchPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("read final service list after Watch replays: %w", err)
	}
	if response == nil || response.Header == nil || len(response.Kvs) != 2 ||
		len(liveEvents) != expectedWrites {
		return fmt.Errorf("final service list or live event stream is incomplete: response=%v events=%d",
			response, len(liveEvents))
	}
	finalBusinessValue := bytes.Repeat([]byte{byte(expectedWrites), byte(expectedWrites >> 8)},
		m4CombinedLoadValueBytes/2)
	registrationFound := false
	businessValueFound := false
	for _, item := range response.Kvs {
		switch string(item.Key) {
		case registrationKey:
			registrationFound = bytes.Equal(item.Value, registrationValue) &&
				item.Lease == int64(leaseID)
		case watchPrefix + "business/session":
			businessValueFound = bytes.Equal(item.Value, finalBusinessValue)
		}
	}
	if !registrationFound || !businessValueFound {
		return fmt.Errorf("final service list lost the leased registration or last business value")
	}
	lastLiveRevision := liveEvents[len(liveEvents)-1].Kv.ModRevision
	if response.Header.Revision < lastLiveRevision {
		return fmt.Errorf("final Range revision %d precedes last Watch event revision %d",
			response.Header.Revision, lastLiveRevision)
	}
	return nil
}

func deleteM4CombinedLoadPrefixAfterDrain(ctx context.Context, client *clientv3.Client,
	prefix string) (int, error) {
	for attempt := 0; attempt < m4CombinedLoadWatchCreateAttempts; attempt++ {
		requestContext, cancelRequest := context.WithTimeout(ctx, m4WatchJavaRPCTimeout)
		response, err := client.Delete(requestContext, prefix, clientv3.WithPrefix())
		cancelRequest()
		if err == nil {
			if response == nil || response.Deleted != 2 {
				deleted := int64(0)
				if response != nil {
					deleted = response.Deleted
				}
				return attempt + 1, fmt.Errorf(
					"combined-load cleanup deleted %d keys, expected the registration and business key",
					deleted)
			}
			return attempt + 1, nil
		}
		if !isM4CombinedLoadAdmissionLimit(err) {
			return attempt + 1, fmt.Errorf("delete the combined-load prefix after Watch drain: %w", err)
		}
		if attempt+1 == m4CombinedLoadWatchCreateAttempts {
			return attempt + 1, fmt.Errorf(
				"combined-load resources did not return to the admission baseline after %d cleanup attempts: %w",
				m4CombinedLoadWatchCreateAttempts, err)
		}
		if waitErr := waitM4CombinedWatchRetry(ctx, combinedWatchRetryDelay(attempt+1)); waitErr != nil {
			return attempt + 1, fmt.Errorf("wait for Watch producer drain before cleanup: %w", waitErr)
		}
	}
	return m4CombinedLoadWatchCreateAttempts, fmt.Errorf("combined-load cleanup attempts exhausted")
}

func isM4CombinedLoadAdmissionLimit(err error) bool {
	if status.Code(err) != codes.ResourceExhausted {
		return false
	}
	message := status.Convert(err).Message()
	return message == "backend resource limit was reached" || message == "request capacity exhausted"
}

func validateM4CombinedLoadEvents(events []*clientv3.Event, watchPrefix string,
	expectedEvents int) error {
	if len(events) != expectedEvents {
		return fmt.Errorf("received %d events, expected %d", len(events), expectedEvents)
	}
	previousByKey := make(map[string][]byte, 1)
	previousRevision := int64(0)
	for _, event := range events {
		if event == nil || event.Kv == nil || event.Type != mvccpb.PUT {
			return fmt.Errorf("expected a PUT event with a key-value")
		}
		key := string(event.Kv.Key)
		if !strings.HasPrefix(key, watchPrefix+"business/") {
			return fmt.Errorf("unexpected watched key %q", key)
		}
		if len(event.Kv.Value) != m4CombinedLoadValueBytes ||
			event.Kv.ModRevision <= previousRevision {
			return fmt.Errorf("event value or revision is invalid for %q at revision %d",
				key, event.Kv.ModRevision)
		}
		if previousValue, exists := previousByKey[key]; exists {
			if event.PrevKv == nil || !bytes.Equal(event.PrevKv.Value, previousValue) {
				return fmt.Errorf("event for %q did not carry the immediately previous value", key)
			}
		} else if event.PrevKv != nil {
			return fmt.Errorf("first event for %q unexpectedly has PrevKV", key)
		}
		previousByKey[key] = event.Kv.Value
		previousRevision = event.Kv.ModRevision
	}
	return nil
}
