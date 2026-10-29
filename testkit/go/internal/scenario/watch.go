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
	watchBaseReq := &etcdserverpb.RangeRequest{Key: watchKey}
	watchBaseResp, err := runner.call(ctx, "C19", "etcdserverpb.KV/Range", watchBaseReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, watchBaseReq)
	})
	if err != nil {
		return err
	}
	watchBase, ok := watchBaseResp.(*etcdserverpb.RangeResponse)
	if !ok || watchBase.Header == nil {
		return errors.New("native watch baseline has no revision header")
	}
	watchCtx, cancelWatch := context.WithTimeout(ctx, requestTimeout)
	watchStream, err := watch.Watch(watchCtx)
	if err != nil {
		cancelWatch()
		return fmt.Errorf("open native Watch stream: %w", err)
	}
	watchReq := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: watchKey, StartRevision: watchBase.Header.Revision + 1,
	}}}
	watchCreateStarted := time.Now()
	if err := watchStream.Send(watchReq); err != nil {
		cancelWatch()
		return fmt.Errorf("send native Watch create: %w", err)
	}
	created, err := watchStream.Recv()
	if err != nil {
		cancelWatch()
		return fmt.Errorf("receive native Watch create response: %w", err)
	}
	if !created.Created || created.Canceled || created.WatchId < 0 || created.Header == nil {
		cancelWatch()
		return errors.New("native Watch did not acknowledge creation with a watch ID and header")
	}
	if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, created, nil, nil, time.Since(watchCreateStarted)); err != nil {
		cancelWatch()
		return err
	}
	watchPut := &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("after-watch-revision")}
	if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", watchPut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, watchPut)
	}); err != nil {
		cancelWatch()
		return err
	}
	var eventResponse *etcdserverpb.WatchResponse
	for eventResponse == nil {
		receiveStarted := time.Now()
		response, recvErr := watchStream.Recv()
		if recvErr != nil {
			cancelWatch()
			return fmt.Errorf("receive native Watch event: %w", recvErr)
		}
		if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, response, nil, nil, time.Since(receiveStarted)); err != nil {
			cancelWatch()
			return err
		}
		if len(response.Events) > 0 {
			eventResponse = response
		}
	}
	if len(eventResponse.Events) != 1 || eventResponse.Events[0].Type != mvccpb.PUT ||
		string(eventResponse.Events[0].Kv.Key) != string(watchKey) ||
		string(eventResponse.Events[0].Kv.Value) != "after-watch-revision" ||
		eventResponse.Events[0].Kv.ModRevision != watchBase.Header.Revision+1 ||
		eventResponse.Header == nil || eventResponse.Header.Revision != eventResponse.Events[0].Kv.ModRevision {
		cancelWatch()
		return errors.New("native Watch did not return the expected R+1 Put event and revision header")
	}
	_ = watchStream.CloseSend()
	cancelWatch()
	terminalStarted := time.Now()
	_, terminalErr := watchStream.Recv()
	terminal := &trace.StreamTerminal{Kind: "context-canceled", GRPCCode: codes.Canceled.String()}
	if terminalErr == nil || (status.Code(terminalErr) != codes.Canceled && !errors.Is(terminalErr, context.Canceled)) {
		return fmt.Errorf("native Watch did not terminate after cancellation: %s", status.Code(terminalErr))
	}
	if err := runner.record("C19", "etcdserverpb.Watch/Watch", watchReq, nil, terminalErr, terminal, time.Since(terminalStarted)); err != nil {
		return err
	}

	return nil
}
