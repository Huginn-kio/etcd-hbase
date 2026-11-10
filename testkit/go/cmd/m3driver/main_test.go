package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

func TestOptionsRequireExplicitScopeAndIsolatedPrefix(t *testing.T) {
	base := []string{"--scenario", "suite.yaml", "--metadata", "metadata.json", "--endpoint", "127.0.0.1:2379", "--scope", "native-etcd",
		"--prefix", "etcd-hbase-m3-test/run-1/", "--trace", "-"}
	if _, err := parseOptions(base, &bytes.Buffer{}); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	for _, args := range [][]string{
		{"--endpoint", "127.0.0.1:2379", "--prefix", "etcd-hbase-m3-test/run-1/", "--trace", "-"},
		{"--endpoint", "127.0.0.1:2379", "--scope", "target-hbase", "--prefix", "etcd-hbase-m3-test/run-1/", "--trace", "-"},
		{"--endpoint", "127.0.0.1:2379", "--scope", "target-hbase", "--layout", "HISTORY", "--prefix", "production/", "--trace", "-"},
		{"--endpoint", "http://user:private-sentinel@127.0.0.1:2379", "--scope", "native-etcd", "--prefix", "etcd-hbase-m3-test/run-1/", "--trace", "-"},
	} {
		args = append(args, "--scenario", "suite.yaml", "--metadata", "metadata.json")
		if _, err := parseOptions(args, &bytes.Buffer{}); err == nil || strings.Contains(err.Error(), "private-sentinel") {
			t.Fatal("unsafe options were accepted or exposed")
		}
	}
}

func TestDriverKeepsFailureTraceAndRejectsFixedSuccessfulResponses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	calls := &atomic.Int32{}
	etcdserverpb.RegisterKVServer(server, &fixedPutServer{calls: calls})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	args := []string{"--scenario", "../../../scenarios/m3-put.yaml", "--metadata", path + ".metadata", "--endpoint", listener.Addr().String(), "--scope", "target-hbase", "--layout", "HISTORY",
		"--prefix", "etcd-hbase-m3-test/fixed/", "--trace", path}
	if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("fixed successful responses passed the scenario")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, err := trace.ReadJSONL(file)
	if err != nil || len(records) != 2 || calls.Load() != 2 {
		t.Fatalf("failed run did not preserve its real observations: records=%d calls=%d err=%v", len(records), calls.Load(), err)
	}
	metadataBytes, err := os.ReadFile(path + ".metadata")
	if err != nil {
		t.Fatal(err)
	}
	var metadata runMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil || metadata.Status != "failed" || metadata.EmbeddedNative {
		t.Fatal("failure was not recorded in evidence metadata")
	}
	if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || calls.Load() != 2 {
		t.Fatal("driver overwrote an existing trace or sent a duplicate operation")
	}
}

func TestDriverCanRunAnIsolatedPinnedNativeServer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trace.jsonl")
	args := []string{"--scenario", "../../../scenarios/m3-put.yaml", "--metadata", path + ".metadata", "--native-data-dir", filepath.Join(dir, "data"), "--scope", "native-etcd",
		"--prefix", "etcd-hbase-m3-test/native/", "--trace", path}
	if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, err := trace.ReadJSONL(file)
	if err != nil || len(records) != 14 {
		t.Fatalf("native trace incomplete: %d records, err=%v", len(records), err)
	}
	metadataBytes, err := os.ReadFile(path + ".metadata")
	if err != nil {
		t.Fatal(err)
	}
	var metadata runMetadata
	traceBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil || metadata.Status != "passed" ||
		metadata.TraceSHA256 != hex.EncodeToString(digest[:]) || metadata.Build.Protocol != "etcd-v3.5.23" ||
		!metadata.EmbeddedNative {
		t.Fatal("native run metadata does not bind the actual trace and pinned protocol")
	}
	if _, err := parseOptions(append(args, "--endpoint", "127.0.0.1:2379"), &bytes.Buffer{}); err == nil {
		t.Fatal("native data directory and endpoint were accepted together")
	}
}

func TestEvidenceRecordsTraceCloseFailure(t *testing.T) {
	metadataFile, err := os.Create(filepath.Join(t.TempDir(), "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	closeFailure := errors.New("trace close failed")
	var stdout bytes.Buffer
	output := evidenceOutput{options: options{trace: "trace.jsonl"}, stdout: &stdout,
		traceHash: sha256.New(), closeTrace: func() error { return closeFailure }, metadata: metadataFile}
	if err := output.finish(nil); !errors.Is(err, closeFailure) {
		t.Fatalf("trace close failure was lost: %v", err)
	}
	contents, err := os.ReadFile(metadataFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	var evidence runMetadata
	if err := json.Unmarshal(contents, &evidence); err != nil || evidence.Status != "failed" || stdout.Len() != 0 {
		t.Fatalf("close failure produced a successful result: status=%q err=%v", evidence.Status, err)
	}
}

type fixedPutServer struct {
	etcdserverpb.UnimplementedKVServer
	calls *atomic.Int32
}

func (server *fixedPutServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	server.calls.Add(1)
	return &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}}, nil
}
