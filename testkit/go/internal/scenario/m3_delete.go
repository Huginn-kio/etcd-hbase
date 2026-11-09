package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func RunM3NativeDelete(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native delete requires a context, new data directory, and output")
	}
	if err := ValidateM3Put("native-etcd", prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		return errors.New("M3 native data directory could not be created")
	}
	server, err := startEmbedded(ctx, dataDir)
	if err != nil {
		return errors.New("M3 native server could not be started")
	}
	defer server.close()
	return RunM3Delete(ctx, server.client, "native-etcd", prefix, output)
}

func RunM3Delete(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 delete requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3DeleteScenario(ctx, prefix)
}

func (runner *runner) m3DeleteScenario(ctx context.Context, prefix string) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	point := []byte(prefix + "point")
	baseline, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{Key: point, Value: []byte("old")})
	if err != nil || baseline.Header == nil {
		return errors.New("delete baseline did not create a key")
	}
	base := baseline.Header.Revision
	deleted, err := runner.m3Delete(ctx, kv, "J33-point", &etcdserverpb.DeleteRangeRequest{Key: point, PrevKv: true})
	if err != nil || deleted.Header == nil || deleted.Header.Revision != base+1 || deleted.Deleted != 1 ||
		len(deleted.PrevKvs) != 1 || !bytes.Equal(deleted.PrevKvs[0].Value, []byte("old")) {
		return errors.New("point delete returned the wrong revision or previous value")
	}
	empty, err := runner.m3Delete(ctx, kv, "J33-empty", &etcdserverpb.DeleteRangeRequest{Key: point, PrevKv: true})
	if err != nil || empty.Header == nil || empty.Header.Revision != base+1 || empty.Deleted != 0 || len(empty.PrevKvs) != 0 {
		return errors.New("empty delete changed revision or returned a previous value")
	}
	if _, err = runner.m3Put(ctx, kv, "J33-recreate", &etcdserverpb.PutRequest{Key: point, Value: []byte("new"), PrevKv: true}); err != nil {
		return err
	}
	recreated, err := runner.m3Txn(ctx, kv, "J33-recreate-read", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{m3Range(point)}})
	if err != nil || recreated.Header == nil || recreated.Header.Revision != base+2 || len(recreated.Responses) != 1 {
		return errors.New("recreated key could not be read")
	}
	read := recreated.Responses[0].GetResponseRange()
	if read == nil || len(read.Kvs) != 1 || read.Kvs[0].CreateRevision != base+2 || read.Kvs[0].Version != 1 ||
		!bytes.Equal(read.Kvs[0].Value, []byte("new")) {
		return errors.New("delete then recreate did not reset key metadata")
	}
	group := []byte(prefix + "group/")
	keys := [][]byte{append(bytes.Clone(group), 'a'), append(bytes.Clone(group), 'a', 0), append(bytes.Clone(group), 0xff)}
	other := []byte(prefix + "other")
	ops := make([]*etcdserverpb.RequestOp, 0, len(keys)+1)
	for _, key := range keys {
		ops = append(ops, m3PutOp(key, []byte("range"), false))
	}
	ops = append(ops, m3PutOp(other, []byte("other"), false))
	if _, err = runner.m3Txn(ctx, kv, "J33-seed-range", &etcdserverpb.TxnRequest{Success: ops}); err != nil {
		return err
	}
	ranged, err := runner.m3Delete(ctx, kv, "J33-prefix", &etcdserverpb.DeleteRangeRequest{
		Key: group, RangeEnd: []byte(clientv3.GetPrefixRangeEnd(string(group))), PrevKv: true})
	if err != nil || ranged.Deleted != int64(len(keys)) || len(ranged.PrevKvs) != len(keys) {
		return errors.New("prefix delete returned the wrong number of keys")
	}
	for index, key := range keys {
		if !bytes.Equal(ranged.PrevKvs[index].Key, key) {
			return errors.New("prefix delete previous values are not in key order")
		}
	}
	if err := runner.m3DeleteOverlap(ctx, kv, prefix); err != nil {
		return err
	}
	if err := runner.m3DeleteZeroEnd(ctx, kv, prefix); err != nil {
		return err
	}
	if err := runner.m3ClientDelete(ctx, kv, prefix); err != nil {
		return err
	}
	withoutPrevious, err := runner.m3Delete(ctx, kv, "J33-no-prev", &etcdserverpb.DeleteRangeRequest{Key: other})
	if err != nil || withoutPrevious.Deleted != 1 || len(withoutPrevious.PrevKvs) != 0 {
		return errors.New("delete without PrevKV returned an incorrect result")
	}
	invalid := &etcdserverpb.DeleteRangeRequest{}
	_, callErr := runner.callWithError(ctx, "J33-empty-key", "etcdserverpb.KV/DeleteRange", invalid,
		func(callContext context.Context) (protoMessage, error) { return kv.DeleteRange(callContext, invalid) })
	if classified := trace.ClassifyError(callErr); classified == nil || classified.SemanticKind != "etcd/EmptyKey" {
		return errors.New("delete empty key did not return the native error")
	}
	return nil
}

func (runner *runner) m3DeleteOverlap(ctx context.Context, kv etcdserverpb.KVClient, prefix string) error {
	a, b, c := []byte(prefix+"overlap/a"), []byte(prefix+"overlap/b"), []byte(prefix+"overlap/c")
	if _, err := runner.m3Txn(ctx, kv, "J33-seed-overlap", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(a, []byte("a"), false), m3PutOp(b, []byte("b"), false), m3PutOp(c, []byte("c"), false),
	}}); err != nil {
		return err
	}
	deleteOp := func(start, end []byte) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: start, RangeEnd: end, PrevKv: true}}}
	}
	response, err := runner.m3Txn(ctx, kv, "J33-overlap", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3Range(a), deleteOp(a, c), m3Range(a), m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			deleteOp(a, []byte(prefix+"overlap/z")), deleteOp(a, []byte(prefix+"overlap/z")),
		}}),
	}})
	if err != nil || len(response.Responses) != 4 {
		return errors.New("overlapping delete transaction failed")
	}
	first := response.Responses[1].GetResponseDeleteRange()
	after := response.Responses[2].GetResponseRange()
	nested := response.Responses[3].GetResponseTxn()
	if first == nil || first.Deleted != 2 || after == nil || len(after.Kvs) != 0 || nested == nil || len(nested.Responses) != 2 {
		return errors.New("overlapping deletes did not use the completed working view")
	}
	second, empty := nested.Responses[0].GetResponseDeleteRange(), nested.Responses[1].GetResponseDeleteRange()
	if second == nil || second.Deleted != 1 || empty == nil || empty.Deleted != 0 {
		return errors.New("nested overlapping deletes did not use the completed working view")
	}
	return nil
}

func (runner *runner) m3DeleteZeroEnd(ctx context.Context, kv etcdserverpb.KVClient, prefix string) error {
	key, start := []byte(prefix+"zz/key"), []byte(prefix+"zz/")
	if _, err := runner.m3Put(ctx, kv, "J33-seed-zero", &etcdserverpb.PutRequest{Key: key, Value: []byte("old")}); err != nil {
		return err
	}
	del := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: start, RangeEnd: []byte{0}, PrevKv: true}}}
	first, err := runner.m3Txn(ctx, kv, "J33-zero-delete-put", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		del, m3PutOp(key, []byte("new"), true), m3Range(key),
	}})
	if err != nil || len(first.Responses) != 3 {
		return errors.New("zero-end delete then Put transaction failed")
	}
	firstDelete, firstPut, firstRead := first.Responses[0].GetResponseDeleteRange(),
		first.Responses[1].GetResponsePut(), first.Responses[2].GetResponseRange()
	if firstDelete == nil || firstDelete.Deleted != 1 || firstPut == nil || firstPut.PrevKv != nil ||
		firstRead == nil || len(firstRead.Kvs) != 1 || firstRead.Kvs[0].Version != 1 {
		return errors.New("zero-end delete then Put did not recreate the key")
	}
	second, err := runner.m3Txn(ctx, kv, "J33-zero-put-delete", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(key, []byte("next"), true), del, m3Range(key),
	}})
	if err != nil || len(second.Responses) != 3 {
		return errors.New("zero-end Put then delete transaction failed")
	}
	secondDelete, secondRead := second.Responses[1].GetResponseDeleteRange(), second.Responses[2].GetResponseRange()
	if secondDelete == nil || secondDelete.Deleted != 1 || len(secondDelete.PrevKvs) != 1 ||
		!bytes.Equal(secondDelete.PrevKvs[0].Value, []byte("next")) || secondRead == nil || len(secondRead.Kvs) != 0 {
		return errors.New("zero-end Put then delete did not use the prior working value")
	}
	return nil
}

type capturingDeleteClient struct {
	etcdserverpb.KVClient
	request *etcdserverpb.DeleteRangeRequest
}

func (client *capturingDeleteClient) DeleteRange(ctx context.Context, request *etcdserverpb.DeleteRangeRequest,
	opts ...grpc.CallOption) (*etcdserverpb.DeleteRangeResponse, error) {
	client.request = proto.Clone(request).(*etcdserverpb.DeleteRangeRequest)
	return client.KVClient.DeleteRange(ctx, request, opts...)
}

func (runner *runner) m3ClientDelete(ctx context.Context, kv etcdserverpb.KVClient, prefix string) error {
	group := prefix + "client/"
	if _, err := runner.m3Txn(ctx, kv, "J33-seed-client", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp([]byte(group+"a"), []byte("a"), false), m3PutOp([]byte(group+"b"), []byte("b"), false),
	}}); err != nil {
		return err
	}
	capture := &capturingDeleteClient{KVClient: kv}
	client := clientv3.NewKVFromKVClient(capture, nil)
	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	started := time.Now()
	response, callErr := client.Delete(requestContext, group, clientv3.WithPrefix(), clientv3.WithPrevKV())
	elapsed := time.Since(started)
	cancel()
	if capture.request == nil {
		return errors.New("clientv3 did not dispatch a DeleteRange request")
	}
	var wire *etcdserverpb.DeleteRangeResponse
	if response != nil {
		wire = (*etcdserverpb.DeleteRangeResponse)(response)
	}
	if err := runner.record("J33-client-prefix", "etcdserverpb.KV/DeleteRange", capture.request, wire, callErr, nil, elapsed); err != nil {
		return err
	}
	if callErr != nil || wire == nil || wire.Deleted != 2 || len(wire.PrevKvs) != 2 {
		return errors.New("clientv3 prefix delete did not return both previous values")
	}
	return nil
}

func (runner *runner) m3Delete(ctx context.Context, kv etcdserverpb.KVClient,
	caseID string, request *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/DeleteRange", request,
		func(callContext context.Context) (protoMessage, error) { return kv.DeleteRange(callContext, request) })
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.DeleteRangeResponse)
	if !ok || response == nil {
		return nil, fmt.Errorf("%s: DeleteRange returned no response", caseID)
	}
	return response, nil
}
