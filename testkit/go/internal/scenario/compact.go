package scenario

import (
	"context"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
)

func (runner *runner) compact(ctx context.Context, kv etcdserverpb.KVClient, binaryKey []byte, baselineRevision int64) error {
	latestReq := &etcdserverpb.RangeRequest{Key: binaryKey}
	latestResult, err := runner.call(ctx, "C06", "etcdserverpb.KV/Range", latestReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, latestReq)
	})
	if err != nil {
		return err
	}
	latest, ok := latestResult.(*etcdserverpb.RangeResponse)
	if !ok || latest.Header == nil {
		return errors.New("native pre-compaction Range has no revision header")
	}
	compactReq := &etcdserverpb.CompactionRequest{Revision: latest.Header.Revision}
	if _, err := runner.call(ctx, "C14", "etcdserverpb.KV/Compact", compactReq, func(requestCtx context.Context) (protoMessage, error) {
		return etcdserverpb.NewKVClient(runner.client.ActiveConnection()).Compact(requestCtx, compactReq)
	}); err != nil {
		return err
	}
	oldRevisionReq := &etcdserverpb.RangeRequest{Key: binaryKey, Revision: baselineRevision + 1}
	oldRevisionResp, oldRevisionErr := runner.callWithError(ctx, "C06", "etcdserverpb.KV/Range", oldRevisionReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, oldRevisionReq)
	})
	classified := trace.ClassifyError(oldRevisionErr)
	if oldRevisionResp != nil || classified == nil || classified.GRPCCode != codes.OutOfRange.String() || classified.SemanticKind != "etcd/Compacted" {
		code := "none"
		if classified != nil {
			code = classified.GRPCCode
		}
		return fmt.Errorf("native compacted historical Range returned grpc_code=%s, expected OUT_OF_RANGE", code)
	}
	return nil
}
