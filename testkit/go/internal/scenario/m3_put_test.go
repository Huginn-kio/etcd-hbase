package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

func TestM3PutScenarioRunsAgainstIndependentNativeServers(t *testing.T) {
	ctx := testContext(t)
	first := runM3PutOnNative(t, ctx)
	second := runM3PutOnNative(t, ctx)
	if err := compare.Compare(first, second); err != nil {
		t.Fatalf("independent native Put traces differ: %v", err)
	}
	if len(first) != 14 || first[0].CaseID != "native-baseline" {
		t.Fatalf("unexpected Put evidence count or baseline: %d", len(first))
	}
	suite, _, err := LoadM3PutSuite("../../../scenarios/m3-put.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for index, step := range suite.Steps {
		if first[index].CaseID != step.ID {
			t.Fatalf("scenario record %d differs from its registered case", index+1)
		}
	}
	for _, record := range first {
		if record.EvidenceScope != "native-etcd" || record.Method != "etcdserverpb.KV/Put" {
			t.Fatalf("unexpected Put evidence metadata: %#v", record)
		}
	}
	if first[len(first)-2].Error == nil || first[len(first)-2].Error.SemanticKind != "etcd/KeyNotFound" {
		t.Fatal("missing-key ignore-value failure was not observed")
	}
	if first[len(first)-1].Error == nil || first[len(first)-1].Error.SemanticKind != "etcd/RequestTooLarge" {
		t.Fatal("native request-too-large failure was not observed")
	}
	if first[6].CaseID != "J30-client-ignore-value" || first[7].CaseID != "J30-client-ignore-lease" {
		t.Fatal("clientv3 option scenarios were not recorded")
	}
	assertClientOptions(t, first[6], true, false)
	assertClientOptions(t, first[7], false, true)
}

func TestM3SuiteRejectsEmptyMissingDuplicateAndChangedVersion(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m3-put.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		"",
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "  - case_id: C07\n    id: J30-update\n", "", 1),
		string(original) + "schema_version: 1\n",
	} {
		file := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(file, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM3PutSuite(file); err == nil {
			t.Fatal("invalid M3 manifest was accepted")
		}
	}
}

func assertClientOptions(t *testing.T, record trace.Record, ignoreValue, ignoreLease bool) {
	t.Helper()
	var request struct {
		Fields struct {
			PrevKV      bool `json:"prevKv"`
			IgnoreValue bool `json:"ignoreValue"`
			IgnoreLease bool `json:"ignoreLease"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(record.Request, &request); err != nil {
		t.Fatal(err)
	}
	if !request.Fields.PrevKV || request.Fields.IgnoreValue != ignoreValue ||
		request.Fields.IgnoreLease != ignoreLease {
		t.Fatal("clientv3 options did not reach their captured requests")
	}
}

func runM3PutOnNative(t *testing.T, ctx context.Context) []trace.Record {
	t.Helper()
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	var output bytes.Buffer
	if err := RunM3Put(ctx, server.client, "native-etcd", "etcd-hbase-m3-test/isolated/", &output); err != nil {
		t.Fatalf("native Put scenario failed: %v", err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("read Put trace: %v", err)
	}
	return records
}

func TestM3PutScenarioRejectsFixedSuccessfulResponses(t *testing.T) {
	server, endpoint, calls := fixedPutEndpoint(t)
	defer server.Stop()
	connection, err := client.New(client.Options{Endpoint: endpoint, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var output bytes.Buffer
	err = RunM3Put(context.Background(), connection, "target-hbase", "etcd-hbase-m3-test/fixed/", &output)
	if err == nil || !strings.Contains(err.Error(), "previous KV") {
		t.Fatalf("fixed responses were not rejected for their missing previous KV: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("scenario continued after invalid Put response: %d calls", calls.Load())
	}
}

func TestM3PutScenarioRejectsInvalidScopeAndPrefixBeforeMutation(t *testing.T) {
	server, endpoint, calls := fixedPutEndpoint(t)
	defer server.Stop()
	connection, err := client.New(client.Options{Endpoint: endpoint, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	for _, input := range []struct{ scope, prefix string }{
		{"transport", "etcd-hbase-m3-test/good/"},
		{"target-hbase", "production-key"},
	} {
		if err := RunM3Put(context.Background(), connection, input.scope, input.prefix, &bytes.Buffer{}); err == nil {
			t.Fatal("unsafe scenario parameters were accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid scenario parameters dispatched a Put")
	}
}

func fixedPutEndpoint(t *testing.T) (*grpc.Server, string, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	calls := &atomic.Int32{}
	etcdserverpb.RegisterKVServer(server, &fixedPutServer{calls: calls})
	go func() { _ = server.Serve(listener) }()
	return server, listener.Addr().String(), calls
}

type fixedPutServer struct {
	etcdserverpb.UnimplementedKVServer
	calls *atomic.Int32
}

func (server *fixedPutServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	server.calls.Add(1)
	return &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}}, nil
}
