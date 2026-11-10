package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// RunM3NativeNestedTxn runs the pinned oracle in a fresh, caller-owned directory.
func RunM3NativeNestedTxn(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native nested Txn requires a context, new data directory, and output")
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
	return RunM3NestedTxn(ctx, server.client, "native-etcd", prefix, output)
}

// RunM3NestedTxn sends the same bounded requests to the oracle and target service.
func RunM3NestedTxn(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 nested Txn requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3NestedTxnScenario(ctx, prefix)
}

func (runner *runner) m3NestedTxnScenario(ctx context.Context, prefix string) error {
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	key := append([]byte(prefix+"history"), 0, 0xff)
	newKey := []byte(prefix + "new")
	baseline, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	if err != nil || baseline.Header == nil || baseline.Header.Revision < 1 {
		return errors.New("nested Txn baseline did not create a key")
	}
	base := baseline.Header.Revision
	if err := runner.m3NestedBranch(ctx, kv, key, newKey, base); err != nil {
		return err
	}
	if err := runner.m3NestedHistory(ctx, kv, key, base); err != nil {
		return err
	}
	return runner.m3NestedOtherCases(ctx, kv, key, newKey, prefix, base)
}

func m3VersionZero(key []byte) *etcdserverpb.Compare {
	return &etcdserverpb.Compare{Key: key, Target: etcdserverpb.Compare_VERSION,
		Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_Version{Version: 0}}
}

func (runner *runner) m3NestedBranch(ctx context.Context, kv etcdserverpb.KVClient, key, newKey []byte, base int64) error {
	branch, err := runner.m3Txn(ctx, kv, "J32-branch-base", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(newKey, []byte("new"), false), m3PutOp(key, []byte("base"), false),
		m3Nested(&etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{m3VersionZero(newKey)},
			Success: []*etcdserverpb.RequestOp{m3Range(newKey)}}),
	}})
	if err != nil {
		return err
	}
	if branch.Header == nil || branch.Header.Revision != base+1 || len(branch.Responses) != 3 {
		return errors.New("nested branch did not commit exactly once")
	}
	child := branch.Responses[2].GetResponseTxn()
	if !m3NativeChildSucceeded(child) {
		return errors.New("nested comparison or child header differs from native semantics")
	}
	read := child.Responses[0].GetResponseRange()
	if read == nil || len(read.Kvs) != 1 || !bytes.Equal(read.Kvs[0].Value, []byte("new")) {
		return errors.New("nested current read did not observe the completed prior Put")
	}
	return nil
}

func (runner *runner) m3NestedHistory(ctx context.Context, kv etcdserverpb.KVClient, key []byte, base int64) error {
	old, explicitBase, negative := m3Range(key), m3Range(key), m3Range(key)
	old.GetRequestRange().Revision = base
	explicitBase.GetRequestRange().Revision = base + 1
	negative.GetRequestRange().Revision = -1
	history, err := runner.m3Txn(ctx, kv, "J32-historical-reads", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		m3PutOp(key, []byte("latest"), false), m3Nested(&etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{m3Range(key), old, explicitBase, negative}}),
	}})
	if err != nil {
		return err
	}
	if history.Header == nil || history.Header.Revision != base+2 || len(history.Responses) != 2 {
		return errors.New("nested historical transaction has the wrong revision")
	}
	historical := history.Responses[1].GetResponseTxn()
	if historical == nil || len(historical.Responses) != 4 {
		return errors.New("nested historical response tree is incomplete")
	}
	for index, expected := range []string{"latest", "old", "base", "latest"} {
		rangeResult := historical.Responses[index].GetResponseRange()
		if !m3RangeHasValue(rangeResult, base+2, expected) {
			return fmt.Errorf("nested historical response %d differs", index)
		}
	}
	return nil
}

func (runner *runner) m3NestedOtherCases(ctx context.Context, kv etcdserverpb.KVClient, key, newKey []byte, prefix string, base int64) error {
	elseTxn := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{m3VersionZero(newKey)},
		Success: []*etcdserverpb.RequestOp{m3Range(newKey)}, Failure: []*etcdserverpb.RequestOp{
			m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{m3Range(key)}})}}
	otherwise, err := runner.m3Txn(ctx, kv, "J32-nested-else", &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{m3Nested(elseTxn)}})
	if err != nil {
		return err
	}
	if !m3NestedElseSelected(otherwise, base+2) {
		return errors.New("nested Else branch or read-only revision differs")
	}
	future := m3Range(key)
	future.GetRequestRange().Revision = math.MaxInt64
	ignored := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "missing"), IgnoreValue: true}}}
	if _, err := runner.m3Txn(ctx, kv, "J32-unselected-dynamic", &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{m3Range(key)}, Failure: []*etcdserverpb.RequestOp{
			m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{future, ignored}})}}); err != nil {
		return err
	}
	exclusive := m3PutOp([]byte(prefix+"exclusive"), nil, false)
	if _, err := runner.m3Txn(ctx, kv, "J32-mutually-exclusive", &etcdserverpb.TxnRequest{
		Failure: []*etcdserverpb.RequestOp{m3Nested(&etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{exclusive}, Failure: []*etcdserverpb.RequestOp{exclusive}})}}); err != nil {
		return err
	}
	return runner.m3NestedErrors(ctx, kv, key, prefix, future, ignored, exclusive)
}

func (runner *runner) m3NestedErrors(ctx context.Context, kv etcdserverpb.KVClient, key []byte, prefix string, future, ignored, exclusive *etcdserverpb.RequestOp) error {
	negativeTooOld := m3Range(key)
	negativeTooOld.GetRequestRange().Revision = -2
	deleteRange := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "z")}}}
	for _, test := range []struct {
		id, semantic string
		request      *etcdserverpb.TxnRequest
	}{
		{"J32-sibling-duplicate", "etcd/DuplicateKey", &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{
			m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{exclusive}}),
			m3Nested(&etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{exclusive}})}}},
		{"J32-parent-overlap", "etcd/DuplicateKey", &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{
			deleteRange, m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{exclusive}})}}},
		{"J32-dynamic-priority", "etcd/KeyNotFound", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			future, m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{ignored}})}}},
		{"J32-future-revision", "etcd/FutureRev", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{future}}},
		{"J32-negative-revision", "etcd/Compacted", &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{negativeTooOld}}},
		{"J32-unselected-empty-key", "etcd/EmptyKey", &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{
			m3Nested(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{m3Range(nil)}})}}},
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

func m3NativeChildSucceeded(child *etcdserverpb.TxnResponse) bool {
	return child != nil && child.Succeeded && child.Header != nil && child.Header.Revision == 0 &&
		child.Header.ClusterId == 0 && child.Header.MemberId == 0 && child.Header.RaftTerm == 0 && len(child.Responses) == 1
}

func m3RangeHasValue(result *etcdserverpb.RangeResponse, revision int64, expected string) bool {
	return result != nil && result.Header != nil && result.Header.Revision == revision &&
		len(result.Kvs) == 1 && string(result.Kvs[0].Value) == expected
}

func m3NestedElseSelected(response *etcdserverpb.TxnResponse, revision int64) bool {
	if response.Header == nil || response.Header.Revision != revision || len(response.Responses) != 1 {
		return false
	}
	child := response.Responses[0].GetResponseTxn()
	return child != nil && !child.Succeeded
}

func m3Nested(request *etcdserverpb.TxnRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: request}}
}
