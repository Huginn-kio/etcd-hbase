package scenario

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var m3PutPrefix = regexp.MustCompile(`^etcd-hbase-m3-test/[A-Za-z0-9][A-Za-z0-9._-]{0,63}/$`)

// RunM3NativePut starts a disposable pinned native server for one isolated run.
// dataDir must not already exist; the caller owns its removal after the process exits.
func RunM3NativePut(ctx context.Context, dataDir, prefix string, output io.Writer) error {
	if ctx == nil || dataDir == "" || output == nil {
		return errors.New("M3 native Put requires a context, new data directory, and output")
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
	return RunM3Put(ctx, server.client, "native-etcd", prefix, output)
}

// RunM3Put executes the same mutating Put scenario against either oracle endpoint.
// The caller owns the client and must provide a fresh, isolated test prefix.
func RunM3Put(ctx context.Context, client *clientv3.Client, scope, prefix string, output io.Writer) error {
	if ctx == nil || client == nil || output == nil {
		return errors.New("M3 Put scenario requires a context, client, and output")
	}
	if err := ValidateM3Put(scope, prefix); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runner := &runner{client: client, writer: output, ordinals: make(map[string]uint64), evidenceScope: scope}
	return runner.m3PutScenario(ctx, prefix)
}

// ValidateM3Put prevents the driver from using ambiguous evidence labels or arbitrary keys.
func ValidateM3Put(scope, prefix string) error {
	if scope != "native-etcd" && scope != "target-hbase" {
		return errors.New("M3 Put scenario has an invalid evidence scope")
	}
	if !m3PutPrefix.MatchString(prefix) {
		return errors.New("M3 Put scenario requires a bounded test prefix")
	}
	return nil
}

func (runner *runner) m3PutScenario(ctx context.Context, prefix string) error {
	key := append([]byte(prefix), 0, 0xff)
	initial := []byte{0, 0xff, 'v'}
	replacement := []byte{'n', 0, 0x80}
	kv := etcdserverpb.NewKVClient(runner.client.ActiveConnection())
	first, err := runner.m3Put(ctx, kv, "native-baseline", &etcdserverpb.PutRequest{
		Key: key, Value: initial, PrevKv: true,
	})
	if err != nil {
		return err
	}
	if first.Header == nil || first.Header.Revision <= 0 || first.PrevKv != nil {
		return errors.New("M3 Put baseline did not create a fresh key")
	}
	base := first.Header.Revision
	if base > math.MaxInt64-8 {
		return errors.New("M3 Put baseline leaves no revision capacity")
	}
	steps := []struct {
		caseID  string
		request *etcdserverpb.PutRequest
		value   []byte
	}{
		{"J30-update", &etcdserverpb.PutRequest{Key: key, Value: replacement, PrevKv: true}, initial},
		{"J30-same-value", &etcdserverpb.PutRequest{Key: key, Value: replacement, PrevKv: true}, replacement},
		{"J30-ignore-value", &etcdserverpb.PutRequest{Key: key, PrevKv: true, IgnoreValue: true}, replacement},
		{"J30-ignore-lease", &etcdserverpb.PutRequest{Key: key, PrevKv: true, IgnoreLease: true}, replacement},
		{"J30-ignore-both", &etcdserverpb.PutRequest{Key: key, PrevKv: true, IgnoreValue: true, IgnoreLease: true}, []byte{}},
	}
	for index, step := range steps {
		response, err := runner.m3Put(ctx, kv, step.caseID, step.request)
		if err != nil {
			return err
		}
		if err := checkM3PutPrevious(response, key, step.value, base, int64(index)+1); err != nil {
			return fmt.Errorf("%s: %w", step.caseID, err)
		}
	}
	if err := runner.m3ClientPut(ctx, kv, key, base); err != nil {
		return err
	}
	longKey := append([]byte(prefix), bytes.Repeat([]byte{'x'}, 8192)...)
	longResponse, err := runner.m3Put(ctx, kv, "J30-long-key", &etcdserverpb.PutRequest{
		Key: longKey, Value: []byte{0xff}, PrevKv: true,
	})
	if err != nil {
		return err
	}
	if longResponse.Header == nil || longResponse.Header.Revision != base+8 || longResponse.PrevKv != nil {
		return errors.New("M3 Put long-key response differs from a fresh write")
	}
	return runner.m3PutFailures(ctx, kv, prefix, key)
}

// capturingPutClient observes the actual protobuf built by clientv3, immediately
// before it is delegated to the endpoint under test. It does not synthesize a
// request or replace the endpoint's response.
type capturingPutClient struct {
	etcdserverpb.KVClient
	request *etcdserverpb.PutRequest
}

func (client *capturingPutClient) Put(ctx context.Context, request *etcdserverpb.PutRequest,
	opts ...grpc.CallOption) (*etcdserverpb.PutResponse, error) {
	client.request = proto.Clone(request).(*etcdserverpb.PutRequest)
	return client.KVClient.Put(ctx, request, opts...)
}

func (runner *runner) m3ClientPut(ctx context.Context, remote etcdserverpb.KVClient,
	key []byte, base int64) error {
	capture := &capturingPutClient{KVClient: remote}
	client := clientv3.NewKVFromKVClient(capture, nil)
	steps := []struct {
		caseID string
		value  string
		opts   []clientv3.OpOption
	}{
		{"J30-client-ignore-value", "", []clientv3.OpOption{clientv3.WithPrevKV(), clientv3.WithIgnoreValue()}},
		{"J30-client-ignore-lease", string([]byte{'n', 0, 0x80}), []clientv3.OpOption{clientv3.WithPrevKV(), clientv3.WithIgnoreLease()}},
	}
	for index, step := range steps {
		capture.request = nil
		requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
		started := time.Now()
		response, callErr := client.Put(requestContext, string(key), step.value, step.opts...)
		elapsed := time.Since(started)
		cancel()
		if capture.request == nil {
			return fmt.Errorf("%s: clientv3 did not dispatch a Put request", step.caseID)
		}
		var wireResponse *etcdserverpb.PutResponse
		if response != nil {
			wireResponse = (*etcdserverpb.PutResponse)(response)
		}
		if err := runner.record(step.caseID, "etcdserverpb.KV/Put", capture.request,
			wireResponse, callErr, nil, elapsed); err != nil {
			return err
		}
		if callErr != nil || wireResponse == nil {
			return fmt.Errorf("%s: clientv3 Put failed", step.caseID)
		}
		if err := checkM3PutPrevious(wireResponse, key, []byte{}, base, int64(index)+6); err != nil {
			return fmt.Errorf("%s: %w", step.caseID, err)
		}
	}
	return nil
}

func (runner *runner) m3Put(ctx context.Context, kv etcdserverpb.KVClient,
	caseID string, request *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	result, err := runner.call(ctx, caseID, "etcdserverpb.KV/Put", request,
		func(callContext context.Context) (protoMessage, error) { return kv.Put(callContext, request) })
	if err != nil {
		return nil, err
	}
	response, ok := result.(*etcdserverpb.PutResponse)
	if !ok || response == nil {
		return nil, errors.New("M3 Put returned no response")
	}
	return response, nil
}

func checkM3PutPrevious(response *etcdserverpb.PutResponse, key, value []byte, base, version int64) error {
	previous := response.PrevKv
	if response.Header == nil || response.Header.Revision != base+version ||
		previous == nil || !matchesM3PutPrevious(previous, key, value, base, version) {
		return errors.New("M3 Put returned incorrect previous KV or revision")
	}
	return nil
}

func matchesM3PutPrevious(previous *mvccpb.KeyValue, key, value []byte, base, version int64) bool {
	return bytes.Equal(previous.Key, key) && bytes.Equal(previous.Value, value) &&
		previous.CreateRevision == base && previous.ModRevision == base+version-1 &&
		previous.Version == version && previous.Lease == 0
}

func (runner *runner) m3PutFailures(ctx context.Context, kv etcdserverpb.KVClient,
	prefix string, key []byte) error {
	cases := []struct {
		caseID   string
		request  *etcdserverpb.PutRequest
		semantic string
	}{
		{"J30-empty-key", &etcdserverpb.PutRequest{}, "etcd/EmptyKey"},
		{"J30-value-provided", &etcdserverpb.PutRequest{Key: key, Value: []byte{1}, IgnoreValue: true}, "etcd/ValueProvided"},
		{"J30-lease-provided", &etcdserverpb.PutRequest{Key: key, Lease: 1, IgnoreLease: true}, "etcd/LeaseProvided"},
		{"J30-missing-ignore", &etcdserverpb.PutRequest{Key: []byte(prefix + "missing"), IgnoreValue: true}, "etcd/KeyNotFound"},
		{"J30-request-too-large", &etcdserverpb.PutRequest{Key: key,
			Value: bytes.Repeat([]byte{'x'}, 3*1024*1024/2+4096)}, "etcd/RequestTooLarge"},
	}
	for _, test := range cases {
		_, err := runner.callWithError(ctx, test.caseID, "etcdserverpb.KV/Put", test.request,
			func(callContext context.Context) (protoMessage, error) { return kv.Put(callContext, test.request) })
		classification := trace.ClassifyError(err)
		if classification == nil || classification.SemanticKind != test.semantic {
			return fmt.Errorf("%s: expected canonical Put error %s", test.caseID, test.semantic)
		}
	}
	return nil
}
