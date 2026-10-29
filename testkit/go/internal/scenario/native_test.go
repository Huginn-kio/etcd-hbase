package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestNativeScenarioRunsTwiceAndPreservesExpectedEvidence(t *testing.T) {
	suitePath := filepath.Join("..", "..", "..", "..", "testkit", "scenarios", "m0-native.yaml")
	suite, err := LoadSuite(suitePath)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	firstTrace := new(bytes.Buffer)
	metadata, err := Run(testContext(t), suite, filepath.Join(root, "first"), firstTrace)
	if err != nil {
		t.Fatalf("first native run: %v", err)
	}
	if metadata.Protocol != protocolVersion || metadata.SourceRevision != sourceRevision || len(metadata.Modules) != 3 {
		t.Fatalf("unexpected native build metadata: %#v", metadata)
	}
	firstRecords, err := trace.ReadJSONL(bytes.NewReader(firstTrace.Bytes()))
	if err != nil {
		t.Fatalf("read first native trace: %v", err)
	}
	assertNativeEvidence(t, firstRecords)

	secondTrace := new(bytes.Buffer)
	if _, err := Run(testContext(t), suite, filepath.Join(root, "second"), secondTrace); err != nil {
		t.Fatalf("second native run: %v", err)
	}
	secondRecords, err := trace.ReadJSONL(bytes.NewReader(secondTrace.Bytes()))
	if err != nil {
		t.Fatalf("read second native trace: %v", err)
	}
	if err := compare.Compare(firstRecords, secondRecords); err != nil {
		t.Fatalf("independent native traces differ: %v", err)
	}
}

func TestLoadSuiteRejectsChangedProtocolOrSteps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	content := "schema_version: 1\nid: m0-native-v1\nprotocol: etcd-v3.5.22\nsteps: []\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSuite(path); err == nil {
		t.Fatal("scenario with a different protocol was accepted")
	}
}

func TestRunRejectsChangedInMemorySteps(t *testing.T) {
	suite := Suite{
		SchemaVersion: 1,
		ID:            "m0-native-v1",
		Protocol:      protocolVersion,
		Steps:         append([]Step(nil), expectedSteps...),
	}
	suite.Steps[0].ID = "unreviewed-step"
	if _, err := Run(testContext(t), suite, filepath.Join(t.TempDir(), "must-not-start"), &bytes.Buffer{}); err == nil {
		t.Fatal("Run accepted steps that differ from the pinned native manifest")
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func assertNativeEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	for _, record := range records {
		if record.EvidenceScope != "native-etcd" {
			t.Fatalf("record %s has wrong evidence scope %q", record.CaseID, record.EvidenceScope)
		}
	}
	assertRangeEvidence(t, records)
	assertTxnEvidence(t, records)
	assertDeleteEvidence(t, records)
	assertLeaseEvidence(t, records)
	assertWatchEvidence(t, records)
	assertCompactionEvidence(t, records)
}

func assertRangeEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	var sawMore, sawCountOnly bool
	for _, record := range records {
		switch record.CaseID {
		case "C03":
			if responseBool(decodeSnapshot(t, record.Response), "more") {
				sawMore = true
			}
		case "C05":
			if isCountOnlyEvidence(t, record) {
				sawCountOnly = true
			}
		}
	}
	requireNativeEvidence(t, "Limit/More", sawMore)
	requireNativeEvidence(t, "CountOnly", sawCountOnly)
}

func isCountOnlyEvidence(t *testing.T, record trace.Record) bool {
	t.Helper()
	request := decodeSnapshot(t, record.Request)
	response := decodeSnapshot(t, record.Response)
	count, countOK := response["count"].(string)
	more, moreOK := response["more"].(bool)
	return requestBool(request, "countOnly") && countOK && count == "3" && moreOK && !more && responseArrayLen(response, "kvs") == 0
}

func assertTxnEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	var sawSuccess, sawFailure bool
	for _, record := range records {
		switch record.CaseID {
		case "C11":
			sawSuccess = responseBool(decodeSnapshot(t, record.Response), "succeeded")
		case "C12":
			sawFailure = !responseBool(decodeSnapshot(t, record.Response), "succeeded")
		}
	}
	requireNativeEvidence(t, "Txn success branch", sawSuccess)
	requireNativeEvidence(t, "Txn failure branch", sawFailure)
}

func assertDeleteEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	sawPrevKV := false
	for _, record := range records {
		if record.CaseID == "C09" && record.Method == "etcdserverpb.KV/DeleteRange" {
			sawPrevKV = responseArrayLen(decodeSnapshot(t, record.Response), "prevKvs") == 1
		}
	}
	requireNativeEvidence(t, "Delete PrevKV", sawPrevKV)
}

func assertLeaseEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	var sawKeepAlive, sawRevoke bool
	for _, record := range records {
		if record.CaseID == "C17" && record.StreamTerminal != nil && record.StreamTerminal.Kind == "eof" {
			sawKeepAlive = true
		}
		if record.CaseID == "C18" && record.Method == "etcdserverpb.Lease/LeaseRevoke" && record.Error == nil {
			sawRevoke = true
		}
	}
	requireNativeEvidence(t, "LeaseKeepAlive EOF", sawKeepAlive)
	requireNativeEvidence(t, "LeaseRevoke", sawRevoke)
}

type watchEvidence struct {
	created  bool
	event    bool
	terminal bool
}

func assertWatchEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	evidence := watchEvidence{}
	for _, record := range records {
		if record.CaseID == "C19" {
			evidence.observe(t, record)
		}
	}
	requireNativeEvidence(t, "Watch created response", evidence.created)
	requireNativeEvidence(t, "Watch event", evidence.event)
	requireNativeEvidence(t, "Watch cancellation terminal", evidence.terminal)
}

func (evidence *watchEvidence) observe(t *testing.T, record trace.Record) {
	t.Helper()
	if record.StreamTerminal != nil && record.StreamTerminal.Kind == "context-canceled" {
		evidence.terminal = true
	}
	if len(record.Response) == 0 || string(record.Response) == "null" {
		return
	}
	response := decodeSnapshot(t, record.Response)
	if responseBool(response, "created") {
		watchID, watchIDOK := response["watchId"].(string)
		evidence.created = watchIDOK && watchID == "0" && response["header"] != nil
	}
	if isExpectedWatchEvent(t, record, response) {
		evidence.event = true
	}
}

func isExpectedWatchEvent(t *testing.T, record trace.Record, response map[string]any) bool {
	t.Helper()
	events, ok := response["events"].([]any)
	if !ok || len(events) != 1 {
		return false
	}
	request := decodeSnapshot(t, record.Request)
	create := snapshotChildFields(request, "createRequest")
	event, _ := events[0].(map[string]any)
	eventFields, _ := event["fields"].(map[string]any)
	keyValue := snapshotChildFields(eventFields, "kv")
	header := snapshotChildFields(response, "header")
	startRevision, startOK := create["startRevision"].(string)
	modRevision, modOK := keyValue["modRevision"].(string)
	headerRevision, headerOK := header["revision"].(string)
	matchingRevisions := startOK && modOK && headerOK && startRevision == modRevision && modRevision == headerRevision
	expectedValue := keyValue["value"] == "YWZ0ZXItd2F0Y2gtcmV2aXNpb24="
	return matchingRevisions && expectedValue
}

func assertCompactionEvidence(t *testing.T, records []trace.Record) {
	t.Helper()
	for _, record := range records {
		if record.CaseID == "C06" && record.Error != nil && record.Error.GRPCCode == "OutOfRange" && record.Error.SemanticKind == "etcd/Compacted" {
			return
		}
	}
	requireNativeEvidence(t, "compacted revision error", false)
}

func requireNativeEvidence(t *testing.T, name string, passed bool) {
	t.Helper()
	if !passed {
		t.Errorf("native trace is missing %s evidence", name)
	}
}

func decodeSnapshot(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	fields, ok := snapshot["fields"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot fields missing: %s", raw)
	}
	return fields
}

func responseBool(fields map[string]any, key string) bool {
	value, _ := fields[key].(bool)
	return value
}

func requestBool(fields map[string]any, key string) bool {
	value, _ := fields[key].(bool)
	return value
}

func responseArrayLen(fields map[string]any, key string) int {
	value, _ := fields[key].([]any)
	return len(value)
}

func snapshotChildFields(fields map[string]any, key string) map[string]any {
	snapshot, _ := fields[key].(map[string]any)
	child, _ := snapshot["fields"].(map[string]any)
	return child
}

func TestLoadSuiteRejectsOversizedOrDuplicateManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	for _, data := range [][]byte{bytes.Repeat([]byte("x"), 65_537), []byte("schema_version: 1\nschema_version: 1\n")} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSuite(path); err == nil {
			t.Fatal("damaged manifest was accepted")
		}
	}
}
