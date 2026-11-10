package scenario

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestM3SortScenarioMatchesIndependentNativeServers(t *testing.T) {
	ctx := testContext(t)
	run := func() []trace.Record {
		server, err := startEmbedded(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer server.close()
		var output bytes.Buffer
		if err := RunM3Sort(ctx, server.client, "native-etcd", "etcd-hbase-m3-test/sort/", &output); err != nil {
			t.Fatalf("native sort scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		return records
	}
	first, second := run(), run()
	if err := compare.Compare(first, second); err != nil {
		t.Fatalf("independent native sort traces differ: %v", err)
	}
	suite, _, err := LoadM3Suite("../../../scenarios/m3-sort.yaml")
	if err != nil || len(first) != len(suite.Steps) {
		t.Fatalf("sort coverage is incomplete: %d records, %v", len(first), err)
	}
	for index, step := range suite.Steps {
		if first[index].CaseID != step.ID || first[index].EvidenceScope != "native-etcd" {
			t.Fatalf("sort evidence differs at step %d", index+1)
		}
	}
}

func TestM3SortSuiteRejectsMissingCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m3-sort.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []string{
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "  - {case_id: C03, id: J35-desc-next}\n", "", 1),
	} {
		path := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(path, []byte(altered), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM3Suite(path); err == nil {
			t.Fatal("invalid sort manifest was accepted")
		}
	}
}
