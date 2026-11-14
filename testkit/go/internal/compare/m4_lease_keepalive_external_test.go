package compare_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestCompareM4LeaseKeepAliveRejectsMissingTerminal(t *testing.T) {
	records := nativeM4LeaseKeepAliveTrace(t)
	changed := cloneKeepAliveRecords(t, records)
	changed[9].StreamTerminal = nil
	if err := compare.CompareM4LeaseKeepAliveRecords(records, changed); err == nil {
		t.Fatal("KeepAlive comparison accepted a missing clean stream terminal")
	}
}

func TestCompareM4LeaseKeepAliveRejectsChangedRevisionOrUnknownTtl(t *testing.T) {
	records := nativeM4LeaseKeepAliveTrace(t)
	t.Run("revision", func(t *testing.T) {
		changed := cloneKeepAliveRecords(t, records)
		changed[16].Response = bytes.Replace(changed[16].Response,
			[]byte(`"revision":"1"`), []byte(`"revision":"2"`), 1)
		if bytes.Equal(changed[16].Response, records[16].Response) {
			t.Fatal("KeepAlive fixture did not contain the expected revision")
		}
		if err := compare.CompareM4LeaseKeepAliveRecords(records, changed); err == nil {
			t.Fatal("KeepAlive comparison accepted a changed KV revision")
		}
	})
	t.Run("unknown lease TTL", func(t *testing.T) {
		changed := cloneKeepAliveRecords(t, records)
		changed[8].Response = bytes.Replace(changed[8].Response,
			[]byte(`"TTL":"0"`), []byte(`"TTL":"1"`), 1)
		if bytes.Equal(changed[8].Response, records[8].Response) {
			t.Fatal("KeepAlive fixture did not contain the expected unknown-ID TTL")
		}
		if err := compare.CompareM4LeaseKeepAliveRecords(records, changed); err == nil {
			t.Fatal("KeepAlive comparison accepted a positive TTL for an unknown Lease")
		}
	})
	t.Run("active lease TTL", func(t *testing.T) {
		changed := cloneKeepAliveRecords(t, records)
		changed[6].Response = bytes.Replace(changed[6].Response,
			[]byte(`"TTL":"30"`), []byte(`"TTL":"29"`), 1)
		if bytes.Equal(changed[6].Response, records[6].Response) {
			t.Fatal("KeepAlive fixture did not contain the expected active TTL")
		}
		if err := compare.CompareM4LeaseKeepAliveRecords(records, changed); err == nil {
			t.Fatal("KeepAlive comparison accepted a non-granted TTL in a renewal response")
		}
	})
}

func nativeM4LeaseKeepAliveTrace(t *testing.T) []trace.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var output bytes.Buffer
	if err := scenario.RunM4NativeLeaseKeepAlive(ctx, filepath.Join(t.TempDir(), "native"),
		"etcd-hbase-m4-test/compare/keepalive/", &output); err != nil {
		t.Fatalf("run native M4 KeepAlive trace: %v", err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("read native M4 KeepAlive trace: %v", err)
	}
	return records
}

func cloneKeepAliveRecords(t *testing.T, records []trace.Record) []trace.Record {
	t.Helper()
	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("encode KeepAlive fixture: %v", err)
	}
	var cloned []trace.Record
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatalf("clone KeepAlive fixture: %v", err)
	}
	return cloned
}
