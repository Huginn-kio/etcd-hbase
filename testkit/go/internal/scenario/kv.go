package scenario

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (runner *runner) run(ctx context.Context) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())

	baselineReq := &etcdserverpb.RangeRequest{Key: []byte("m0/scenario-baseline")}
	baselineResult, err := runner.call(ctx, "native-baseline", "etcdserverpb.KV/Range", baselineReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, baselineReq)
	})
	if err != nil {
		return err
	}
	baseline, ok := baselineResult.(*etcdserverpb.RangeResponse)
	if !ok || baseline.Header == nil || baseline.Header.Revision < 1 {
		return errors.New("native baseline has no valid revision header")
	}

	binaryKey := []byte{0x00, 0xff, 'm', '0'}
	for _, step := range []func() error{
		func() error { return runner.binaryAndRanges(ctx, kv, binaryKey) },
		func() error { return runner.transactions(ctx, kv, binaryKey) },
		func() error { return runner.deletion(ctx, kv) },
		func() error { return runner.leases(ctx, kv) },
		func() error { return runner.watch(ctx, kv) },
		func() error { return runner.compact(ctx, kv, binaryKey, baseline.Header.Revision) },
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (runner *runner) binaryAndRanges(ctx context.Context, kv etcdserverpb.KVClient, binaryKey []byte) error {
	binaryValue := []byte{0x00, 0x80, 0xff, 'v'}
	putReq := &etcdserverpb.PutRequest{Key: binaryKey, Value: binaryValue}
	putResp, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", putReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, putReq)
	})
	if err != nil {
		return err
	}
	if putResp == nil {
		return errors.New("native binary Put returned no response")
	}
	rangeReq := &etcdserverpb.RangeRequest{Key: binaryKey}
	rangeResp, err := runner.call(ctx, "C01", "etcdserverpb.KV/Range", rangeReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, rangeReq)
	})
	if err != nil {
		return err
	}
	binaryRange, ok := rangeResp.(*etcdserverpb.RangeResponse)
	if !ok || len(binaryRange.Kvs) != 1 || string(binaryRange.Kvs[0].Key) != string(binaryKey) || string(binaryRange.Kvs[0].Value) != string(binaryValue) {
		return errors.New("native binary Range did not return the original key and value")
	}

	for index, value := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		key := []byte(fmt.Sprintf("m0/prefix/%d", index+1))
		request := &etcdserverpb.PutRequest{Key: key, Value: value}
		if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", request, func(requestCtx context.Context) (protoMessage, error) {
			return kv.Put(requestCtx, request)
		}); err != nil {
			return err
		}
	}
	prefix := []byte("m0/prefix/")
	prefixEnd := prefixRangeEnd(prefix)
	limited := &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: prefixEnd, Limit: 2}
	limitedResp, err := runner.call(ctx, "C03", "etcdserverpb.KV/Range", limited, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, limited)
	})
	if err != nil {
		return err
	}
	limitedRange, ok := limitedResp.(*etcdserverpb.RangeResponse)
	if !ok || len(limitedRange.Kvs) != 2 || !limitedRange.More {
		return errors.New("native prefix Range did not preserve Limit and More")
	}
	countOnly := &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: prefixEnd, Limit: 1, CountOnly: true}
	countResp, err := runner.call(ctx, "C05", "etcdserverpb.KV/Range", countOnly, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Range(requestCtx, countOnly)
	})
	if err != nil {
		return err
	}
	countRange, ok := countResp.(*etcdserverpb.RangeResponse)
	if !ok || len(countRange.Kvs) != 0 || countRange.Count != 3 || countRange.More {
		return errors.New("native CountOnly Range did not return the expected count, empty KVs, and More=false")
	}

	return nil
}

func (runner *runner) transactions(ctx context.Context, kv etcdserverpb.KVClient, binaryKey []byte) error {
	compareVersion := &etcdserverpb.Compare{
		Result: etcdserverpb.Compare_EQUAL, Target: etcdserverpb.Compare_VERSION,
		Key: binaryKey, TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
	}
	txnSuccess := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{compareVersion},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/success"), Value: []byte("then")}}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/wrong-success-failure"), Value: []byte("else")}}}},
	}
	result, err := runner.call(ctx, "C11", "etcdserverpb.KV/Txn", txnSuccess, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Txn(requestCtx, txnSuccess)
	})
	if err != nil {
		return err
	}
	if response, ok := result.(*etcdserverpb.TxnResponse); !ok || !response.Succeeded || len(response.Responses) != 1 {
		return errors.New("native Txn did not execute its matching success branch")
	}
	txnFailure := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Result: etcdserverpb.Compare_EQUAL, Target: etcdserverpb.Compare_VALUE,
			Key: binaryKey, TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("not-the-value")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/wrong-failure-success"), Value: []byte("then")}}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("m0/txn/failure"), Value: []byte("else")}}}},
	}
	result, err = runner.call(ctx, "C12", "etcdserverpb.KV/Txn", txnFailure, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Txn(requestCtx, txnFailure)
	})
	if err != nil {
		return err
	}
	if response, ok := result.(*etcdserverpb.TxnResponse); !ok || response.Succeeded || len(response.Responses) != 1 {
		return errors.New("native Txn did not execute its matching failure branch")
	}

	return nil
}

func (runner *runner) deletion(ctx context.Context, kv etcdserverpb.KVClient) error {
	deleteKey := []byte("m0/delete/prev-kv")
	deletePut := &etcdserverpb.PutRequest{Key: deleteKey, Value: []byte("before-delete")}
	if _, err := runner.call(ctx, "C01", "etcdserverpb.KV/Put", deletePut, func(requestCtx context.Context) (protoMessage, error) {
		return kv.Put(requestCtx, deletePut)
	}); err != nil {
		return err
	}
	deleteReq := &etcdserverpb.DeleteRangeRequest{Key: deleteKey, PrevKv: true}
	deleteResp, err := runner.call(ctx, "C09", "etcdserverpb.KV/DeleteRange", deleteReq, func(requestCtx context.Context) (protoMessage, error) {
		return kv.DeleteRange(requestCtx, deleteReq)
	})
	if err != nil {
		return err
	}
	deleted, ok := deleteResp.(*etcdserverpb.DeleteRangeResponse)
	if !ok || deleted.Deleted != 1 || len(deleted.PrevKvs) != 1 || string(deleted.PrevKvs[0].Value) != "before-delete" {
		return errors.New("native DeleteRange did not return the previous key-value")
	}

	return nil
}
