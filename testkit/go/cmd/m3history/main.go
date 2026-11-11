package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/history"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	check := flag.String("check-evidence", "", "recheck a saved local history")
	endpoint := flag.String("endpoint", "", "isolated etcd-compatible endpoint")
	prefix := flag.String("prefix", "", "fresh isolated M3 test prefix")
	output := flag.String("output", "", "new local history evidence file")
	restartDir := flag.String("restart-dir", "", "optional local failover coordination directory")
	restartMode := flag.String("restart-mode", "after-commit", "failover barrier mode")
	flag.Parse()
	if *check != "" {
		if flag.NArg() != 0 || *endpoint != "" || *prefix != "" || *output != "" || *restartDir != "" {
			return errors.New("evidence recheck cannot start an RPC fixture")
		}
		return checkEvidence(*check)
	}
	if flag.NArg() != 0 || *output == "" || scenario.ValidateM3Put("target-hbase", *prefix) != nil {
		return errors.New("invalid isolated history arguments")
	}
	if *restartDir != "" && !validRestartMode(*restartMode) {
		return errors.New("invalid restart mode")
	}
	return runHistory(historyConfig{endpoint: *endpoint, prefix: *prefix, output: *output, restartDir: *restartDir, restartMode: *restartMode})
}

type historyConfig struct{ endpoint, prefix, output, restartDir, restartMode string }

func runHistory(config historyConfig) (runErr error) {
	options := client.Options{Endpoint: config.endpoint, Timeout: 30 * time.Second}
	if options.Validate() != nil {
		return errors.New("invalid history endpoint")
	}
	connection, err := newHistoryClient(options)
	if err != nil {
		return errors.New("history client initialization failed")
	}
	connections := &historyConnections{active: connection, options: options}
	defer func() { runErr = errors.Join(runErr, connections.active.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return executeHistory(ctx, connections, config)
}

func executeHistory(ctx context.Context, connections *historyConnections, config historyConfig) error {
	kv := client.RawKV(connections.active)
	key := []byte(config.prefix + "point")
	otherKey := []byte(config.prefix + "nested")
	lostKey := []byte(config.prefix + "lost-response")
	lostKey2 := []byte(config.prefix + "lost-response-2")
	end := append([]byte(config.prefix), 0xff)
	initial, baseRevision, err := readInitialState(ctx, kv, key, otherKey, []byte(config.prefix), end)
	if err != nil {
		return err
	}
	maxCalls := 10
	if config.restartDir != "" {
		maxCalls += 2
		if config.restartMode == "old-owner-resume" {
			maxCalls += 2
		}
	}
	recorder, err := history.NewRecorder(maxCalls, 1<<20)
	if err != nil {
		return err
	}
	workload := historyWorkload{ctx: ctx, kv: kv, recorder: recorder, key: key, otherKey: otherKey, prefix: config.prefix, end: end}
	successfulWrites, err := workload.concurrentWrites()
	if err != nil {
		return err
	}
	txnRevision, err := workload.nestedTransaction()
	if err != nil {
		return err
	}
	if err := workload.concurrentReads(); err != nil {
		return err
	}
	if config.restartDir != "" {
		phase := failoverPhase{ctx: ctx, kv: kv, recorder: recorder, connections: connections,
			directory: config.restartDir, mode: config.restartMode, lostKey: lostKey, lostKey2: lostKey2}
		kv, err = phase.run()
		if err != nil {
			return err
		}
	}
	return finishHistory(ctx, kv, recorder, initial, baseRevision, successfulWrites, txnRevision, end, config)
}

func finishHistory(ctx context.Context, kv etcdserverpb.KVClient, recorder *history.Recorder, initial *history.KVState,
	baseRevision int64, successfulWrites int, txnRevision int64, end []byte, config historyConfig) error {
	if _, err := recorder.Invoke(ctx, kv, &etcdserverpb.DeleteRangeRequest{Key: []byte(config.prefix), RangeEnd: end, PrevKv: true}); err != nil {
		return errors.New("history prefix DeleteRange failed")
	}
	if _, err := recorder.Invoke(ctx, kv, &etcdserverpb.RangeRequest{Key: []byte(config.prefix), RangeEnd: end, Revision: txnRevision}); err != nil {
		return errors.New("history historical prefix Range failed")
	}
	observations, err := recorder.Snapshot()
	if err != nil {
		return err
	}
	result, err := history.Check(ctx, observations, history.KVModel{}, initial, 10000)
	if err != nil {
		return errors.New("history validation failed")
	}
	evidenceRestartMode := "none"
	if config.restartDir != "" {
		evidenceRestartMode = config.restartMode
	}
	evidence := historyEvidence{
		Schema: evidenceSchema, Schedule: evidenceSchedule, Seed: evidenceSeed, ModelScope: evidenceScope,
		RestartMode: evidenceRestartMode, BaseRevision: baseRevision,
		SuccessfulWrites: successfulWrites, Observations: observations, Result: result,
	}
	return persistEvidence(config.output, evidence)
}

func persistEvidence(path string, evidence historyEvidence) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("history evidence could not be created")
	}
	encodeErr := json.NewEncoder(file).Encode(evidence)
	closeErr := file.Close()
	if encodeErr != nil || closeErr != nil {
		return errors.New("history evidence could not be written")
	}
	if evidence.Result.Verdict != history.Linearizable {
		return errors.New("point and nested transaction history did not prove linearizable")
	}
	return nil
}

func readInitialState(ctx context.Context, kv etcdserverpb.KVClient, key, otherKey, prefix, end []byte) (*history.KVState, int64, error) {
	baseline, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil || baseline == nil || baseline.Header == nil || len(baseline.Kvs) != 0 {
		return nil, 0, errors.New("history fixture is not empty or unavailable")
	}
	otherBaseline, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: otherKey})
	if err != nil || otherBaseline == nil || otherBaseline.Header == nil || otherBaseline.Header.Revision != baseline.Header.Revision || len(otherBaseline.Kvs) != 0 {
		return nil, 0, errors.New("nested history fixture is not empty or stable")
	}
	prefixBaseline, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end})
	if err != nil || prefixBaseline == nil || prefixBaseline.Header == nil || prefixBaseline.Header.Revision != baseline.Header.Revision || len(prefixBaseline.Kvs) != 0 {
		return nil, 0, errors.New("history prefix is not empty or stable")
	}
	initial, err := history.NewKVState(baseline.Header.Revision, nil)
	if err != nil {
		return nil, 0, errors.New("invalid history initial revision")
	}
	return initial, baseline.Header.Revision, nil
}

type historyConnections struct {
	active  *clientv3.Client
	options client.Options
}

// failoverPhase owns fault markers and the cancellation watcher until recovery completes.
type failoverPhase struct {
	ctx               context.Context
	kv                etcdserverpb.KVClient
	recorder          *history.Recorder
	connections       *historyConnections
	directory, mode   string
	lostKey, lostKey2 []byte
}

func (phase failoverPhase) run() (etcdserverpb.KVClient, error) {
	if err := phase.invokeFault(); err != nil {
		return nil, err
	}
	kv, err := phase.reconnect()
	if err != nil {
		return nil, err
	}
	if err := phase.verifyRecovery(kv); err != nil {
		return nil, err
	}
	return kv, nil
}

func (phase failoverPhase) invokeFault() error {
	kv, recorder := phase.kv, phase.recorder
	lostKey, lostKey2 := phase.lostKey, phase.lostKey2

	if err := phase.prepareCancellationBarrier(); err != nil {
		return err
	}
	lostTxn := lostTransaction(lostKey, lostKey2)
	callContext, cleanup := phase.faultContext()
	_, lostErr := recorder.Invoke(callContext, kv, lostTxn)
	cleanup()
	expectedCode := codes.Unavailable
	if phase.mode == "old-owner-resume" {
		expectedCode = codes.DeadlineExceeded
	}
	if phase.mode == "cancel-after-cas" {
		expectedCode = codes.Canceled
	}
	if status.Code(lostErr) != expectedCode {
		return fmt.Errorf("expected an unknown result at the fault barrier, got %s", status.Code(lostErr))
	}
	return nil
}

// faultContext owns the watcher until cleanup has joined it.
func (phase failoverPhase) faultContext() (context.Context, func()) {
	ctx := phase.ctx
	callContext := ctx
	callCancel := func() {}
	watchCancel := func() {}
	var watcherDone chan struct{}
	if phase.mode == "old-owner-resume" {
		callContext, callCancel = context.WithTimeout(ctx, 10*time.Second)
	}
	if phase.mode == "cancel-after-cas" {
		callContext, callCancel = context.WithCancel(ctx)
		var watchContext context.Context
		watchContext, watchCancel = context.WithCancel(ctx)
		watcherDone = make(chan struct{})
		go func() {
			defer close(watcherDone)
			if waitMarker(watchContext, phase.directory, "cancel-client") == nil {
				callCancel()
			}
		}()
	}
	return callContext, func() {
		callCancel()
		watchCancel()
		if watcherDone != nil {
			<-watcherDone
		}
	}
}

func lostTransaction(lostKey, lostKey2 []byte) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: lostKey, Value: []byte("first")}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: lostKey2, Value: []byte("second")}}},
	}}
}

func (phase failoverPhase) prepareCancellationBarrier() error {
	ctx := phase.ctx
	if phase.mode == "cancel-after-cas" {
		if err := createMarker(phase.directory, "before-fault"); err != nil {
			return err
		}
		if err := waitMarker(ctx, phase.directory, "fault-ready"); err != nil {
			return err
		}
	}
	return nil
}

func (phase failoverPhase) reconnect() (etcdserverpb.KVClient, error) {
	ctx := phase.ctx

	endpoint, err := awaitRestart(ctx, phase.directory)
	if err != nil {
		return nil, err
	}
	if err := phase.connections.active.Close(); err != nil {
		return nil, errors.New("history original connection close failed")
	}
	phase.connections.options.Endpoint = endpoint
	if phase.connections.options.Validate() != nil {
		return nil, errors.New("invalid restart endpoint")
	}
	next, err := newHistoryClient(phase.connections.options)
	if err != nil {
		return nil, errors.New("history restart connection failed")
	}
	phase.connections.active = next
	kv := client.RawKV(phase.connections.active)
	return kv, nil
}

func (phase failoverPhase) verifyRecovery(kv etcdserverpb.KVClient) error {
	ctx, recorder := phase.ctx, phase.recorder
	lostKey, lostKey2 := phase.lostKey, phase.lostKey2

	recovered, err := recorder.Invoke(ctx, kv, &etcdserverpb.RangeRequest{Key: lostKey, RangeEnd: append(append([]byte(nil), lostKey...), 0xff)})
	if err != nil {
		return fmt.Errorf("history recovered Range failed: %s", status.Code(err))
	}
	read, ok := recovered.(*etcdserverpb.RangeResponse)
	if !ok || !unpublishedMode(phase.mode) && (len(read.Kvs) != 2 || string(read.Kvs[0].Key) != string(lostKey) || string(read.Kvs[0].Value) != "first" || string(read.Kvs[1].Key) != string(lostKey2) || string(read.Kvs[1].Value) != "second") ||
		unpublishedMode(phase.mode) && len(read.Kvs) != 0 {
		return errors.New("lost response recovery did not match the observed barrier")
	}
	if phase.mode == "old-owner-resume" {
		if _, err := recorder.Invoke(ctx, kv, &etcdserverpb.PutRequest{Key: lostKey, Value: []byte("new-owner"), PrevKv: true}); err != nil {
			return fmt.Errorf("new owner write failed: %s", status.Code(err))
		}
		if err := createMarker(phase.directory, "new-owner-written"); err != nil {
			return err
		}
		if err := waitMarker(ctx, phase.directory, "old-drained"); err != nil {
			return err
		}
		observed, err := recorder.Invoke(ctx, kv, &etcdserverpb.RangeRequest{Key: lostKey})
		if err != nil {
			return fmt.Errorf("post-resume Range failed: %s", status.Code(err))
		}
		last, ok := observed.(*etcdserverpb.RangeResponse)
		if !ok || len(last.Kvs) != 1 || string(last.Kvs[0].Value) != "new-owner" {
			return errors.New("old owner changed a later committed value")
		}
	}
	return nil
}

// historyWorkload owns the shared recorder and schedules each bounded RPC phase.
type historyWorkload struct {
	ctx                context.Context
	kv                 etcdserverpb.KVClient
	recorder           *history.Recorder
	key, otherKey, end []byte
	prefix             string
}

func (workload historyWorkload) concurrentWrites() (int, error) {
	ctx, kv, recorder, key := workload.ctx, workload.kv, workload.recorder, workload.key
	var workers sync.WaitGroup
	failures := make(chan error, 2)
	comparedPut := func(value string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte(value), PrevKv: true}}}
	}
	requests := []proto.Message{
		&etcdserverpb.PutRequest{Key: key, Value: []byte("one"), PrevKv: true},
		&etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{Key: key, Target: etcdserverpb.Compare_VERSION,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}},
			Success: []*etcdserverpb.RequestOp{comparedPut("two")}, Failure: []*etcdserverpb.RequestOp{comparedPut("two")}},
	}
	for index, request := range requests {
		workers.Add(1)
		go func(worker uint32, request proto.Message) {
			defer workers.Done()
			_, callErr := recorder.InvokeWorker(ctx, kv, worker, request)
			failures <- callErr
		}(uint32(index+1), request)
	}
	workers.Wait()
	close(failures)
	successfulWrites := 0
	for failure := range failures {
		if failure == nil {
			successfulWrites++
		} else if status.Code(failure) != codes.ResourceExhausted {
			return 0, fmt.Errorf("concurrent history write failed: %s", status.Code(failure))
		}
	}
	if successfulWrites == 0 {
		return 0, errors.New("no concurrent history write succeeded")
	}
	return successfulWrites, nil
}

func (workload historyWorkload) nestedTransaction() (int64, error) {
	ctx, kv, recorder, key, otherKey, end := workload.ctx, workload.kv, workload.recorder, workload.key, workload.otherKey, workload.end
	txn := nestedWorkloadRequest(key, otherKey)
	type callResult struct {
		response proto.Message
		err      error
	}
	transactionDone := make(chan callResult, 1)
	rangeDone := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		result, callErr := recorder.InvokeWorker(ctx, kv, 3, txn)
		transactionDone <- callResult{result, callErr}
	}()
	go func() {
		<-start
		_, callErr := recorder.InvokeWorker(ctx, kv, 4, &etcdserverpb.RangeRequest{Key: []byte(workload.prefix), RangeEnd: end})
		rangeDone <- callErr
	}()
	close(start)
	transactionResult, concurrentRangeErr := <-transactionDone, <-rangeDone
	if transactionResult.err != nil {
		return 0, fmt.Errorf("history nested Txn failed: %s", status.Code(transactionResult.err))
	}
	if concurrentRangeErr != nil && status.Code(concurrentRangeErr) != codes.ResourceExhausted {
		return 0, fmt.Errorf("history concurrent Range failed: %s", status.Code(concurrentRangeErr))
	}
	txnResponse, ok := transactionResult.response.(*etcdserverpb.TxnResponse)
	if !ok || txnResponse.Header == nil {
		return 0, errors.New("history nested Txn has no revision")
	}
	return txnResponse.Header.Revision, nil
}

func nestedWorkloadRequest(key, otherKey []byte) *etcdserverpb.TxnRequest {
	compare := func(key []byte, version int64) *etcdserverpb.Compare {
		return &etcdserverpb.Compare{Key: key, Target: etcdserverpb.Compare_VERSION,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: version}}
	}
	return &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{compare(key, 0)},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{compare(key, 0)},
				Failure: []*etcdserverpb.RequestOp{
					{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
					{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: otherKey, Value: []byte("nested")}}},
				}},
		}}}}
}

func (workload historyWorkload) concurrentReads() error {
	ctx, kv, recorder, key, otherKey, end := workload.ctx, workload.kv, workload.recorder, workload.key, workload.otherKey, workload.end
	reads := []proto.Message{
		&etcdserverpb.RangeRequest{Key: key},
		&etcdserverpb.RangeRequest{Key: otherKey},
		&etcdserverpb.DeleteRangeRequest{Key: []byte(workload.prefix + "never-created"), PrevKv: true},
	}
	readFailures := make(chan error, len(reads))
	start := make(chan struct{})
	for index, request := range reads {
		go func(worker uint32, request proto.Message) {
			<-start
			_, callErr := recorder.InvokeWorker(ctx, kv, worker, request)
			readFailures <- callErr
		}(uint32(index+5), request)
	}
	close(start)
	var readFailure error
	for range reads {
		if callErr := <-readFailures; callErr != nil && status.Code(callErr) != codes.ResourceExhausted {
			readFailure = callErr
		}
	}
	if readFailure != nil {
		return fmt.Errorf("history concurrent read/delete failed: %s", status.Code(readFailure))
	}
	if _, err := recorder.Invoke(ctx, kv, &etcdserverpb.RangeRequest{Key: []byte(workload.prefix), RangeEnd: end}); err != nil {
		return errors.New("history prefix Range failed")
	}
	return nil
}

func validRestartMode(mode string) bool {
	return unpublishedMode(mode) || mode == "cas-ack-lost" || mode == "partial-materialization" ||
		mode == "old-owner-resume" || mode == "cancel-after-cas" || mode == "after-commit"
}

func unpublishedMode(mode string) bool {
	return mode == "before-dispatch" || mode == "candidate-preparation" || mode == "result-preparation" || mode == "sealed-before-publish"
}

// This marker follows the failed RPC return; the fixture controls and records
// the earlier fault barrier. Files stay in a private local test directory.
func awaitRestart(ctx context.Context, directory string) (string, error) {
	if err := createMarker(directory, "before-restart"); err != nil {
		return "", err
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(directory, "after-restart"))
		if err == nil {
			if len(data) == 0 || len(data) > 256 {
				return "", errors.New("invalid restart marker")
			}
			return strings.TrimSpace(string(data)), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("restart marker read failed")
		}
		select {
		case <-ctx.Done():
			return "", errors.New("restart wait canceled")
		case <-ticker.C:
		}
	}
}

func createMarker(directory, name string) error {
	marker, err := os.OpenFile(filepath.Join(directory, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("history marker creation failed")
	}
	if err := marker.Close(); err != nil {
		return errors.New("history marker close failed")
	}
	return nil
}

func waitMarker(ctx context.Context, directory, name string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(directory, name)); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("history marker read failed")
		}
		select {
		case <-ctx.Done():
			return errors.New("history marker wait canceled")
		case <-ticker.C:
		}
	}
}

func newHistoryClient(options client.Options) (*clientv3.Client, error) {
	// v3.5.23 interprets MaxUnaryRetries=1 as one invocation, including the
	// initial attempt. Disable configured gRPC retries as a separate layer.
	return clientv3.New(clientv3.Config{Endpoints: []string{options.Endpoint},
		DialTimeout: options.Timeout, MaxUnaryRetries: 1,
		DialOptions: []grpc.DialOption{grpc.WithDisableRetry()}, Logger: zap.NewNop()})
}
