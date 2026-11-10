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

// RunM3NativeTxn starts a pinned native etcd with a new, caller-owned data directory.
func RunM3NativeTxn(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native Txn requires a context, new data directory, and output")
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
	return RunM3Txn(ctx, server.client, "native-etcd", prefix, output)
}

// RunM3Txn uses the same bounded, isolated prefix on the native and target endpoints.
func RunM3Txn(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 Txn scenario requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3TxnScenario(ctx, prefix)
}

func (runner *runner) m3TxnScenario(ctx context.Context, prefix string) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	key := append([]byte(prefix+"a"), 0, 0xff)
	value := []byte{0, 0xff, 'v'}
	baseline, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if baseline.Header == nil || baseline.Header.Revision < 1 || baseline.PrevKv != nil {
		return errors.New("M3 Txn baseline did not create a fresh key")
	}
	base := baseline.Header.Revision
	if err := runner.m3TxnCompareMatrix(ctx, kv, key, value, base); err != nil {
		return err
	}
	if err := runner.m3TxnCompareEdges(ctx, kv, prefix, key, base); err != nil {
		return err
	}
	return runner.m3TxnWrites(ctx, kv, prefix, key, value, base)
}

func (runner *runner) m3TxnCompareMatrix(ctx context.Context, kv etcdserverpb.KVClient, key, value []byte, base int64) error {
	for _, target := range []string{"version", "create", "mod", "value", "lease"} {
		for _, relation := range []string{"equal", "not-equal", "greater", "less"} {
			comparison := m3Comparison(key, value, base, target, relation)
			caseID := fmt.Sprintf("J31-compare-%s-%s", target, relation)
			response, callErr := runner.m3Txn(ctx, kv, caseID, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{comparison}, Success: []*etcdserverpb.RequestOp{m3Range(key)},
			})
			if callErr != nil {
				return callErr
			}
			if response.Header == nil || response.Header.Revision != base || response.Succeeded != (relation != "not-equal") {
				return fmt.Errorf("%s: incorrect comparison or revision", caseID)
			}
		}
	}
	return nil
}

func (runner *runner) m3TxnCompareEdges(ctx context.Context, kv etcdserverpb.KVClient, prefix string, key []byte, base int64) error {
	missing := []byte(prefix + "missing")
	for _, test := range []struct {
		id       string
		compare  *etcdserverpb.Compare
		expected bool
	}{
		{"J31-compare-missing-version", &etcdserverpb.Compare{Key: missing, Target: etcdserverpb.Compare_VERSION,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}, true},
		{"J31-compare-missing-value", &etcdserverpb.Compare{Key: missing, Target: etcdserverpb.Compare_VALUE,
			Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Value{Value: []byte{}}}, false},
		{"J31-compare-range", &etcdserverpb.Compare{Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "z"),
			Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}, true},
	} {
		response, callErr := runner.m3Txn(ctx, kv, test.id, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{test.compare}, Success: []*etcdserverpb.RequestOp{m3Range(key)},
		})
		if callErr != nil {
			return callErr
		}
		if response.Header == nil || response.Header.Revision != base || response.Succeeded != test.expected {
			return fmt.Errorf("%s: incorrect missing-key or range comparison", test.id)
		}
	}
	return nil
}

func m3Comparison(key, value []byte, base int64, target, relation string) *etcdserverpb.Compare {
	compare := &etcdserverpb.Compare{Key: key}
	var adjustment int64
	switch relation {
	case "equal":
		compare.Result = etcdserverpb.Compare_EQUAL
	case "not-equal":
		compare.Result = etcdserverpb.Compare_NOT_EQUAL
	case "greater":
		compare.Result, adjustment = etcdserverpb.Compare_GREATER, -1
	case "less":
		compare.Result, adjustment = etcdserverpb.Compare_LESS, 1
	}
	switch target {
	case "version":
		compare.Target, compare.TargetUnion = etcdserverpb.Compare_VERSION, &etcdserverpb.Compare_Version{Version: 1 + adjustment}
	case "create":
		compare.Target, compare.TargetUnion = etcdserverpb.Compare_CREATE, &etcdserverpb.Compare_CreateRevision{CreateRevision: base + adjustment}
	case "mod":
		compare.Target, compare.TargetUnion = etcdserverpb.Compare_MOD, &etcdserverpb.Compare_ModRevision{ModRevision: base + adjustment}
	case "value":
		compared := value
		if adjustment < 0 {
			compared = []byte{0, 0xfe}
		}
		if adjustment > 0 {
			compared = []byte{0xff}
		}
		compare.Target, compare.TargetUnion = etcdserverpb.Compare_VALUE, &etcdserverpb.Compare_Value{Value: compared}
	case "lease":
		compare.Target, compare.TargetUnion = etcdserverpb.Compare_LEASE, &etcdserverpb.Compare_Lease{Lease: adjustment}
	}
	return compare
}

func m3TxnVersion(key []byte, expected int64) *etcdserverpb.Compare {
	return &etcdserverpb.Compare{Key: key, Target: etcdserverpb.Compare_VERSION,
		Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: expected}}
}

func (runner *runner) m3TxnWrites(ctx context.Context, kv etcdserverpb.KVClient,
	prefix string, key, value []byte, base int64) error {
	if err := runner.m3TxnThenWrite(ctx, kv, key, value, base); err != nil {
		return err
	}
	if err := runner.m3TxnElseAndNoChange(ctx, kv, prefix, key, value, base); err != nil {
		return err
	}
	if err := runner.m3ClientTxn(ctx, kv, key, base+2); err != nil {
		return err
	}
	return runner.m3TxnErrors(ctx, kv, key, value)
}

func (runner *runner) m3TxnThenWrite(ctx context.Context, kv etcdserverpb.KVClient, key, value []byte, base int64) error {
	replacement := []byte{'n', 0, 0x80}
	first, err := runner.m3Txn(ctx, kv, "J31-then-write", &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{m3TxnVersion(key, 1)},
		Success: []*etcdserverpb.RequestOp{m3Range(key), m3PutOp(key, replacement, true), m3Range(key)},
	})
	if err != nil {
		return err
	}
	if !validOrderedTxnResponse(first, base+1, value, replacement) {
		return errors.New("M3 Txn ordered read/write result is incorrect")
	}
	return nil
}

func validOrderedTxnResponse(response *etcdserverpb.TxnResponse, revision int64, before, after []byte) bool {
	if response.Header == nil || response.Header.Revision != revision || !response.Succeeded || len(response.Responses) != 3 {
		return false
	}
	first := response.Responses[0].GetResponseRange()
	if len(first.Kvs) != 1 {
		return false
	}
	last := response.Responses[2].GetResponseRange()
	return len(last.Kvs) == 1 && bytes.Equal(first.Kvs[0].Value, before) &&
		bytes.Equal(last.Kvs[0].Value, after)
}

func (runner *runner) m3TxnElseAndNoChange(ctx context.Context, kv etcdserverpb.KVClient, prefix string, key, value []byte, base int64) error {
	other := []byte(prefix + "other")
	second, err := runner.m3Txn(ctx, kv, "J31-else-write", &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{m3TxnVersion(key, 0)}, Success: []*etcdserverpb.RequestOp{m3PutOp(key, value, false)},
		Failure: []*etcdserverpb.RequestOp{m3PutOp(other, value, false)},
	})
	if err != nil {
		return err
	}
	if second.Header == nil || second.Header.Revision != base+2 || second.Succeeded || len(second.Responses) != 1 {
		return errors.New("M3 Txn Else write did not commit exactly once")
	}
	unchanged, err := runner.m3Txn(ctx, kv, "J31-no-change", &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{m3TxnVersion(other, 0)}, Success: []*etcdserverpb.RequestOp{m3PutOp(other, value, false)},
		Failure: []*etcdserverpb.RequestOp{m3Range(other)},
	})
	if err != nil {
		return err
	}
	if unchanged.Header == nil || unchanged.Header.Revision != base+2 || unchanged.Succeeded || len(unchanged.Responses) != 1 {
		return errors.New("M3 Txn no-change branch altered the revision")
	}
	return nil
}

func (runner *runner) m3TxnErrors(ctx context.Context, kv etcdserverpb.KVClient, key, value []byte) error {
	for _, test := range []struct {
		id, semantic string
		request      *etcdserverpb.TxnRequest
	}{
		{"J31-empty-compare", "etcd/EmptyKey", &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{}}}},
		{"J31-duplicate-put", "etcd/DuplicateKey", &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{m3PutOp(key, value, false), m3PutOp(key, value, false)}}},
	} {
		_, callErr := runner.callWithError(ctx, test.id, "etcdserverpb.KV/Txn", test.request,
			func(callContext context.Context) (protoMessage, error) { return kv.Txn(callContext, test.request) })
		classification := trace.ClassifyError(callErr)
		if classification == nil || classification.SemanticKind != test.semantic {
			return fmt.Errorf("%s: expected canonical error %s", test.id, test.semantic)
		}
	}
	return nil
}

func (runner *runner) m3Txn(ctx context.Context, kv etcdserverpb.KVClient,
	caseID string, request *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	response, err := runner.call(ctx, caseID, "etcdserverpb.KV/Txn", request,
		func(callContext context.Context) (protoMessage, error) { return kv.Txn(callContext, request) })
	if err != nil {
		return nil, err
	}
	result, ok := response.(*etcdserverpb.TxnResponse)
	if !ok || result == nil {
		return nil, errors.New("M3 Txn returned no response")
	}
	return result, nil
}

func m3Range(key []byte) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: key}}}
}

func m3PutOp(key, value []byte, previous bool) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: value, PrevKv: previous}}}
}

type capturingTxnClient struct {
	etcdserverpb.KVClient
	request *etcdserverpb.TxnRequest
}

func (client *capturingTxnClient) Txn(ctx context.Context, request *etcdserverpb.TxnRequest,
	opts ...grpc.CallOption) (*etcdserverpb.TxnResponse, error) {
	client.request = proto.Clone(request).(*etcdserverpb.TxnRequest)
	return client.KVClient.Txn(ctx, request, opts...)
}

func (runner *runner) m3ClientTxn(ctx context.Context, remote etcdserverpb.KVClient, key []byte, revision int64) error {
	capture := &capturingTxnClient{KVClient: remote}
	client := clientv3.NewKVFromKVClient(capture, nil)
	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	started := time.Now()
	response, callErr := client.Txn(requestContext).If(clientv3.Compare(clientv3.Version(string(key)), ">", 0)).
		Then(clientv3.OpGet(string(key))).Commit()
	elapsed := time.Since(started)
	cancel()
	if capture.request == nil {
		return errors.New("clientv3 did not dispatch a Txn request")
	}
	var wire *etcdserverpb.TxnResponse
	if response != nil {
		wire = (*etcdserverpb.TxnResponse)(response)
	}
	if err := runner.record("J31-client-read", "etcdserverpb.KV/Txn", capture.request, wire, callErr, nil, elapsed); err != nil {
		return err
	}
	if callErr != nil || wire == nil || wire.Header == nil || wire.Header.Revision != revision || !wire.Succeeded {
		return errors.New("clientv3 Txn returned an incorrect response")
	}
	return nil
}
