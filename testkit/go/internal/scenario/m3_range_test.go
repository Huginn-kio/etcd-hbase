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

func TestM3RangeScenarioMatchesIndependentNativeServers(t *testing.T) {
	ctx := testContext(t)
	run := func() []trace.Record {
		server, err := startEmbedded(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer server.close()
		var output bytes.Buffer
		if err := RunM3Range(ctx, server.client, "native-etcd", "etcd-hbase-m3-test/range/", &output); err != nil {
			t.Fatalf("native Range scenario failed: %v", err)
		}
		records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		return records
	}
	first, second := run(), run()
	if err := compare.Compare(first, second); err != nil {
		t.Fatalf("independent native Range traces differ: %v", err)
	}
	suite, _, err := LoadM3Suite("../../../scenarios/m3-range.yaml")
	if err != nil || len(first) != len(suite.Steps) {
		t.Fatalf("Range coverage is incomplete: %d records, %v", len(first), err)
	}
	for index, step := range suite.Steps {
		if first[index].CaseID != step.ID || first[index].EvidenceScope != "native-etcd" {
			t.Fatalf("Range evidence differs at step %d", index+1)
		}
	}
}

func TestM3RangeSuiteRejectsMissingCoverage(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m3-range.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []string{
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "  - {case_id: C05, id: J34-count-range-max-create}\n", "", 1),
	} {
		path := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(path, []byte(altered), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM3Suite(path); err == nil {
			t.Fatal("invalid Range manifest was accepted")
		}
	}
}
