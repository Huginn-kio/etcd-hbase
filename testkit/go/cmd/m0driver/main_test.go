package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
)

func TestRunRejectsEvidenceScopesOutsideTheGoDriver(t *testing.T) {
	for _, scope := range []string{"native-etcd", "target-hbase"} {
		if err := run([]string{"--scope", scope}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "scope must be transport or transport-fixture") {
			t.Fatalf("scope %s was not rejected", scope)
		}
	}
}

type compactedFixture struct {
	etcdserverpb.UnimplementedKVServer
}

func (compactedFixture) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, rpctypes.ErrGRPCCompacted
}

func TestFailedRangeRecordsTypedNilWithoutMaskingTheOriginalError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, &compactedFixture{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	for _, mode := range []string{"raw", "clientv3"} {
		var output bytes.Buffer
		err := run([]string{"--endpoint", listener.Addr().String(), "--mode", mode, "--timeout", "2s"}, &output, &bytes.Buffer{})
		if err == nil {
			t.Fatal("compacted request appeared successful")
		}
		records, readErr := trace.ReadJSONL(&output)
		if readErr != nil || len(records) != 1 {
			t.Fatalf("failed request trace was lost: %v", readErr)
		}
		record := records[0]
		if string(record.Response) != "null" || record.Error == nil || record.Error.SemanticKind != "etcd/Compacted" || record.Error.GRPCCode != "OutOfRange" {
			t.Fatalf("original failure was masked: %#v", record)
		}
	}
}

func TestRangeInputRejectsNonCanonicalRevisionAndUnboundedTimeout(t *testing.T) {
	for _, args := range [][]string{{"--revision", "01"}, {"--revision", "-1"}, {"--revision", "9223372036854775808"}, {"--timeout", "31s"}, {"--key-b64", "//=="}} {
		if _, err := parseRange(args, &bytes.Buffer{}); err == nil {
			t.Fatal("invalid driver input accepted")
		}
	}
}
