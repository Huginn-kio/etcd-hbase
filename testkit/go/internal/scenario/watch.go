package scenario

import (
	"context"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func (runner *runner) watch(ctx context.Context, kv etcdserverpb.KVClient) error {
	watch := etcdserverpb.NewWatchClient(runner.client.ActiveConnection())
	watchKey := []byte("m0/watch/r-plus-one")
	startRevision, err := runner.watchStartRevision(ctx, kv, watchKey)
	if err != nil {
		return err
	}
	watchContext, cancelWatch := context.WithTimeout(ctx, requestTimeout)
	defer cancelWatch()
	stream, err := watch.Watch(watchContext)
	if err != nil {
		return fmt.Errorf("open native Watch stream: %w", err)
	}
	request := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: watchKey, StartRevision: startRevision,
	}}}
	if err := runner.createWatch(stream, request); err != nil {
		return err
	}
	if err := runner.triggerWatchPut(ctx, kv, watchKey); err != nil {
		return err
	}
	if err := runner.receiveWatchPut(stream, request, watchKey, startRevision); err != nil {
		return err
	}
	_ = stream.CloseSend()
	cancelWatch()
	return runner.recordWatchCancellation(stream, request)
}

func (runner *runner) watchStartRevision(ctx context.Context, kv etcdserverpb.KVClient, key []byte) (int64, error) {
	request := &etcdserverpb.RangeRequest{Key: key}
	result, err := runner.call(ctx, "C19", "etcdserverpb.KV/Range", request, func(requestContext context.Context) (protoMessage, error) {
		return kv.Range(requestContext, request)
	})
	if err != nil {
		return 0, err
	}
	response, ok := result.(*etcdserverpb.RangeResponse)
	if !ok || response.Header == nil {
		return 0, errors.New("native watch baseline has no revision header")
	}
	return response.Header.Revision + 1, nil
}

func (runner *runner) createWatch(stream etcdserverpb.Watch_WatchClient, request *etcdserverpb.WatchRequest) error {
	started := time.Now()
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("send native Watch create: %w", err)
	}
	created, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive native Watch create response: %w", err)
	}
	if !created.Created || created.Canceled || created.WatchId < 0 || created.Header == nil {
		return errors.New("native Watch did not acknowledge creation with a watch ID and header")
	}
	return runner.record("C19", "etcdserverpb.Watch/Watch", request, created, nil, nil, time.Since(started))
}

func (runner *runner) triggerWatchPut(ctx context.Context, kv etcdserverpb.KVClient, key []byte) error {
	request := &etcdserverpb.PutRequest{Key: key, Value: []byte("after-watch-revision")}
	_, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", request, func(requestContext context.Context) (protoMessage, error) {
		return kv.Put(requestContext, request)
	})
	return err
}

func (runner *runner) receiveWatchPut(stream etcdserverpb.Watch_WatchClient, request *etcdserverpb.WatchRequest, key []byte, startRevision int64) error {
	for {
		started := time.Now()
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive native Watch event: %w", err)
		}
		if err := runner.record("C19", "etcdserverpb.Watch/Watch", request, response, nil, nil, time.Since(started)); err != nil {
			return err
		}
		if len(response.Events) > 0 {
			return validateWatchPut(response, key, startRevision)
		}
	}
}

func validateWatchPut(response *etcdserverpb.WatchResponse, key []byte, startRevision int64) error {
	matchesPut := len(response.Events) == 1 && response.Events[0].Type == mvccpb.PUT &&
		string(response.Events[0].Kv.Key) == string(key) &&
		string(response.Events[0].Kv.Value) == "after-watch-revision" &&
		response.Events[0].Kv.ModRevision == startRevision &&
		response.Header != nil && response.Header.Revision == response.Events[0].Kv.ModRevision
	if !matchesPut {
		return errors.New("native Watch did not return the expected R+1 Put event and revision header")
	}
	return nil
}

func (runner *runner) recordWatchCancellation(stream etcdserverpb.Watch_WatchClient, request *etcdserverpb.WatchRequest) error {
	started := time.Now()
	_, terminalErr := stream.Recv()
	terminal := &trace.StreamTerminal{Kind: "context-canceled", GRPCCode: codes.Canceled.String()}
	if terminalErr == nil || (status.Code(terminalErr) != codes.Canceled && !errors.Is(terminalErr, context.Canceled)) {
		return fmt.Errorf("native Watch did not terminate after cancellation: %s", status.Code(terminalErr))
	}
	return runner.record("C19", "etcdserverpb.Watch/Watch", request, nil, terminalErr, terminal, time.Since(started))
}
