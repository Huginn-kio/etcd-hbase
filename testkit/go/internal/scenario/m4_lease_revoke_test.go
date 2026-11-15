package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"google.golang.org/grpc/codes"
)

func TestM4LeaseRevokeScenarioMatchesItsPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var runs [][]trace.Record
	for _, name := range []string{"revoke-native-one", "revoke-native-two"} {
		var output bytes.Buffer
		if err := RunM4NativeLeaseRevoke(ctx, filepath.Join(t.TempDir(), name),
			"etcd-hbase-m4-test/revoke/", &output); err != nil {
			t.Fatalf("native M4 Revoke scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatalf("read native M4 Revoke trace: %v", err)
		}
		if len(records) != len(expectedM4LeaseRevokeSteps) {
			t.Fatalf("native M4 Revoke trace has %d records, want %d",
				len(records), len(expectedM4LeaseRevokeSteps))
		}
		for index, step := range expectedM4LeaseRevokeSteps {
			if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
				t.Fatalf("record %d does not match the pinned J43 step: %#v", index, records[index])
			}
		}
		if records[10].Error == nil || records[10].Error.GRPCCode != codes.NotFound.String() {
			t.Fatal("Revoke of an already revoked Lease did not return native NotFound")
		}
		runs = append(runs, records)
	}
	if err := compare.CompareM4LeaseRevokeRecords(runs[0], runs[1]); err != nil {
		t.Fatalf("independent native J43 traces differ: %v", err)
	}

	changed := append([]trace.Record(nil), runs[1]...)
	changed[10] = cloneM4RevokeRecord(t, changed[10])
	changed[10].Error.GRPCCode = codes.Internal.String()
	if err := compare.CompareM4LeaseRevokeRecords(runs[0], changed); err == nil {
		t.Fatal("J43 comparison accepted a changed unknown-Revoke error")
	}
}

func TestLoadM4LeaseRevokeSuiteRejectsMissingOrChangedCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m4-lease-revoke.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{
		"",
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "    id: expiry-ttl-after\n", "", 1),
		string(original) + "unknown: true\n",
	} {
		path := filepath.Join(t.TempDir(), "m4-lease-revoke.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM4LeaseRevokeSuite(path); err == nil {
			t.Fatal("modified J43 scenario manifest was accepted")
		}
	}
}

func cloneM4RevokeRecord(t *testing.T, record trace.Record) trace.Record {
	t.Helper()
	var cloned trace.Record
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}
