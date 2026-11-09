package scenario

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestM3NestedTxnScenarioRunsAgainstIndependentNativeServers(t *testing.T) {
	ctx := testContext(t)
	first := runM3NestedOnNative(t, ctx)
	second := runM3NestedOnNative(t, ctx)
	if err := compare.Compare(first, second); err != nil {
		t.Fatalf("independent native nested Txn traces differ: %v", err)
	}
	suite, _, err := LoadM3Suite("../../../scenarios/m3-nested-txn.yaml")
	if err != nil || len(first) != len(suite.Steps) || suite.ID != "m3-nested-txn-v1" {
		t.Fatalf("nested transaction evidence or manifest is incomplete: %d records, %v", len(first), err)
	}
	for index, step := range suite.Steps {
		if first[index].CaseID != step.ID || first[index].EvidenceScope != "native-etcd" {
			t.Fatalf("nested transaction evidence differs at step %d", index+1)
		}
	}
}

func TestM3NestedTxnSuiteRejectsMissingOrAlteredSteps(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m3-nested-txn.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []string{
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "  - case_id: C13\n    id: J32-dynamic-priority\n", "", 1),
		string(original) + "schema_version: 1\n",
	} {
		path := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(path, []byte(altered), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM3Suite(path); err == nil {
			t.Fatal("invalid nested transaction manifest was accepted")
		}
	}
}

func runM3NestedOnNative(t *testing.T, ctx context.Context) []trace.Record {
	t.Helper()
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	var output bytes.Buffer
	if err := RunM3NestedTxn(ctx, server.client, "native-etcd", "etcd-hbase-m3-test/isolated/", &output); err != nil {
		t.Fatalf("native nested Txn scenario failed: %v", err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("read nested transaction trace: %v", err)
	}
	return records
}
