package scenario

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestM4WatchStartScenarioMatchesPinnedNativeOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, name := range []string{"watch-start-native-one", "watch-start-native-two"} {
		var output bytes.Buffer
		prefix := "etcd-hbase-m4-test/" + name + "/watch/"
		if err := RunM4NativeWatchStart(ctx, filepath.Join(t.TempDir(), name), prefix, &output); err != nil {
			t.Fatalf("native M4 Watch start scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatalf("read native M4 Watch start trace: %v", err)
		}
		if len(records) != len(expectedM4WatchStartSteps) {
			t.Fatalf("native M4 Watch start trace has %d records, want %d",
				len(records), len(expectedM4WatchStartSteps))
		}
		for index, step := range expectedM4WatchStartSteps {
			if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
				t.Fatalf("record %d does not match the pinned J45 step: %#v", index, records[index])
			}
		}
	}
}

func TestLoadM4WatchStartSuiteRejectsMissingOrChangedCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m4-watch-starts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{
		"",
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "    id: initial-minus-two-compacted\n", "", 1),
		strings.Replace(string(original), "    id: initial-future-created\n", "", 1),
		string(original) + "unknown: true\n",
	} {
		path := filepath.Join(t.TempDir(), "m4-watch-starts.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM4WatchStartSuite(path); err == nil {
			t.Fatal("J45 loader accepted a changed Watch start matrix")
		}
	}
}
