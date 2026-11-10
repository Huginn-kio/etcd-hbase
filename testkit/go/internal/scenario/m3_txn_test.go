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

func TestM3TxnScenarioRunsAgainstIndependentNativeServers(t *testing.T) {
	ctx := testContext(t)
	first := runM3TxnOnNative(t, ctx)
	second := runM3TxnOnNative(t, ctx)
	if err := compare.Compare(first, second); err != nil {
		t.Fatalf("independent native Txn traces differ: %v", err)
	}
	suite, _, err := LoadM3Suite("../../../scenarios/m3-txn.yaml")
	if err != nil || len(first) != len(suite.Steps) || suite.ID != "m3-txn-v1" {
		t.Fatalf("transaction evidence or manifest is incomplete: %d records, %v", len(first), err)
	}
	for index, step := range suite.Steps {
		if first[index].CaseID != step.ID || first[index].EvidenceScope != "native-etcd" {
			t.Fatalf("transaction evidence differs from manifest at step %d", index+1)
		}
	}
	for _, record := range first[:len(first)-2] {
		if record.Error != nil {
			t.Fatalf("unexpected error in %s", record.CaseID)
		}
	}
	if first[len(first)-2].Error == nil || first[len(first)-2].Error.SemanticKind != "etcd/EmptyKey" ||
		first[len(first)-1].Error == nil || first[len(first)-1].Error.SemanticKind != "etcd/DuplicateKey" {
		t.Fatal("native transaction validation errors were not observed")
	}
}

func TestM3TxnSuiteRejectsMissingOrAlteredSteps(t *testing.T) {
	original, err := os.ReadFile("../../../scenarios/m3-txn.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []string{
		strings.Replace(string(original), "etcd-v3.5.23", "etcd-v3.6.0", 1),
		strings.Replace(string(original), "  - case_id: C12\n    id: J31-no-change\n", "", 1),
		string(original) + "schema_version: 1\n",
	} {
		path := filepath.Join(t.TempDir(), "suite.yaml")
		if err := os.WriteFile(path, []byte(altered), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadM3Suite(path); err == nil {
			t.Fatal("invalid transaction manifest was accepted")
		}
	}
	if _, _, err := LoadM3PutSuite("../../../scenarios/m3-txn.yaml"); err == nil {
		t.Fatal("transaction coverage was accepted as the Put suite")
	}
}

func runM3TxnOnNative(t *testing.T, ctx context.Context) []trace.Record {
	t.Helper()
	server, err := startEmbedded(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	var output bytes.Buffer
	if err := RunM3Txn(ctx, server.client, "native-etcd", "etcd-hbase-m3-test/isolated/", &output); err != nil {
		t.Fatalf("native Txn scenario failed: %v", err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("read transaction trace: %v", err)
	}
	return records
}
