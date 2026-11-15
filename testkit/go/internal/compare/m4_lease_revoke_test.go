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

func TestCompareM4LeaseRevokeRequiresOneRevisionForAllBoundKeys(t *testing.T) {
	baseline := nativeM4LeaseRevokeTrace(t)
	changed := cloneRevokeRecords(t, baseline)
	setRevokeHeaderRevision(t, &changed[6], "999")
	if err := compare.CompareM4LeaseRevokeRecords(baseline, changed); err == nil {
		t.Fatal("Revoke comparison accepted a changed atomic-delete revision")
	}
}

func TestCompareM4LeaseRevokeRejectsChangedTerminalState(t *testing.T) {
	baseline := nativeM4LeaseRevokeTrace(t)
	changed := cloneRevokeRecords(t, baseline)
	setRevokeResponseField(t, &changed[9], "TTL", "0")
	if err := compare.CompareM4LeaseRevokeRecords(baseline, changed); err == nil {
		t.Fatal("Revoke comparison accepted an active TTL for a revoked Lease")
	}
}

func nativeM4LeaseRevokeTrace(t *testing.T) []trace.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var output bytes.Buffer
	if err := scenario.RunM4NativeLeaseRevoke(ctx, filepath.Join(t.TempDir(), "native"),
		"etcd-hbase-m4-test/compare/revoke/", &output); err != nil {
		t.Fatalf("run native M4 Revoke trace: %v", err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("read native M4 Revoke trace: %v", err)
	}
	return records
}

func cloneRevokeRecords(t *testing.T, records []trace.Record) []trace.Record {
	t.Helper()
	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("encode Revoke trace fixture: %v", err)
	}
	var cloned []trace.Record
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatalf("clone Revoke trace fixture: %v", err)
	}
	return cloned
}

func setRevokeHeaderRevision(t *testing.T, record *trace.Record, revision string) {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(record.Response, &snapshot); err != nil {
		t.Fatalf("decode Revoke response fixture: %v", err)
	}
	fields, ok := snapshot["fields"].(map[string]any)
	if !ok {
		t.Fatal("Revoke response fixture has no protobuf fields")
	}
	header, ok := fields["header"].(map[string]any)
	if !ok {
		t.Fatal("Revoke response fixture has no header")
	}
	headerFields, ok := header["fields"].(map[string]any)
	if !ok {
		t.Fatal("Revoke response fixture has no header fields")
	}
	headerFields["revision"] = revision
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode Revoke response fixture: %v", err)
	}
	record.Response = encoded
}

func setRevokeResponseField(t *testing.T, record *trace.Record, name string, value any) {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(record.Response, &snapshot); err != nil {
		t.Fatalf("decode Revoke response fixture: %v", err)
	}
	fields, ok := snapshot["fields"].(map[string]any)
	if !ok {
		t.Fatal("Revoke response fixture has no protobuf fields")
	}
	fields[name] = value
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode Revoke response fixture: %v", err)
	}
	record.Response = encoded
}
