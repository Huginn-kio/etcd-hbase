package scenario

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestM4LeaseKeepAliveScenarioMatchesItsPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var runs [][]trace.Record
	for _, name := range []string{"keepalive-native-one", "keepalive-native-two"} {
		var output bytes.Buffer
		if err := RunM4NativeLeaseKeepAlive(ctx, filepath.Join(t.TempDir(), name),
			"etcd-hbase-m4-test/keepalive/", &output); err != nil {
			t.Fatalf("native M4 KeepAlive scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatalf("read native M4 KeepAlive trace: %v", err)
		}
		if len(records) != len(expectedM4LeaseKeepAliveSteps) {
			t.Fatalf("native M4 KeepAlive trace has %d records, want %d",
				len(records), len(expectedM4LeaseKeepAliveSteps))
		}
		for index, step := range expectedM4LeaseKeepAliveSteps {
			if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
				t.Fatalf("record %d does not match the pinned J42 step: %#v", index, records[index])
			}
		}
		runs = append(runs, records)
	}
	if err := compare.CompareM4LeaseKeepAliveRecords(runs[0], runs[1]); err != nil {
		t.Fatalf("independent native KeepAlive traces differ: %v", err)
	}

	reordered := append([]trace.Record(nil), runs[0]...)
	reordered[6], reordered[7] = reordered[7], reordered[6]
	if err := compare.CompareM4LeaseKeepAliveRecords(runs[0], reordered); err == nil {
		t.Fatal("KeepAlive comparator accepted reordered Lease IDs")
	}
	missingTerminal := append([]trace.Record(nil), runs[0]...)
	missingTerminal[9].StreamTerminal = nil
	if err := compare.CompareM4LeaseKeepAliveRecords(runs[0], missingTerminal); err == nil {
		t.Fatal("KeepAlive comparator accepted a missing stream terminal")
	}
}

func TestLoadM4LeaseKeepAliveSuiteRejectsMissingOrChangedCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m4-lease-keepalive.yaml")
	if err != nil {
		t.Fatalf("read pinned J42 scenario: %v", err)
	}
	path := filepath.Join(t.TempDir(), "m4-lease-keepalive.yaml")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write pinned J42 scenario: %v", err)
	}
	if suite, _, err := LoadM4LeaseKeepAliveSuite(path); err != nil || suite.ID != "m4-lease-keepalive-v1" {
		t.Fatalf("valid J42 scenario was rejected: suite=%#v err=%v", suite, err)
	}
	changed := bytes.Replace(original, []byte("keepalive-stream-missing"),
		[]byte("keepalive-stream-dropped"), 1)
	if bytes.Equal(changed, original) {
		t.Fatal("J42 scenario fixture did not contain the expected coverage marker")
	}
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatalf("write changed J42 scenario: %v", err)
	}
	if _, _, err := LoadM4LeaseKeepAliveSuite(path); err == nil {
		t.Fatal("J42 loader accepted a scenario with missing unknown-ID coverage")
	}
}
