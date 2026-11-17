package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
)

func TestRunNativeLeaseWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native.jsonl")
	metadataPath := filepath.Join(artifact, "native.json")
	dataPath := filepath.Join(artifact, "native-data")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"run", "--scenario", filepath.Join(root, "testkit/scenarios/m4-lease.yaml"),
		"--native-data-dir", dataPath, "--scope", "native-etcd",
		"--prefix", "etcd-hbase-m4-test/driver/", "--trace", tracePath,
		"--metadata", metadataPath}
	if err := run(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatalf("run native M4 scenario: %v", err)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != 11 {
		t.Fatalf("native M4 trace has %d records, err=%v", len(records), err)
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.EvidenceScope != "native-etcd" || !metadata.EmbeddedNative ||
		metadata.TraceSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("M4 native metadata does not describe the trace: %#v", metadata)
	}

	copyPath := filepath.Join(artifact, "native-copy.jsonl")
	if err := os.WriteFile(copyPath, traceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseTraces([]string{"--left", tracePath, "--right", copyPath}, io.Discard, io.Discard); err != nil {
		t.Fatalf("compare equivalent native traces: %v", err)
	}
}

func TestRunNativeLeaseBindingWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native-bindings.jsonl")
	metadataPath := filepath.Join(artifact, "native-bindings.json")
	dataPath := filepath.Join(artifact, "native-bindings-data")
	scenarioPath := filepath.Join(root, "testkit/scenarios/m4-lease-bindings.yaml")
	suite, suiteDigest, err := scenario.LoadM4LeaseBindingSuite(scenarioPath)
	if err != nil {
		t.Fatalf("load registered M4 binding scenario: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"run", "--scenario", scenarioPath,
		"--native-data-dir", dataPath, "--scope", "native-etcd",
		"--prefix", "etcd-hbase-m4-test/driver/bindings/", "--trace", tracePath,
		"--metadata", metadataPath}
	if err := run(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatalf("run native M4 binding scenario: %v", err)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != len(suite.Steps) {
		t.Fatalf("native M4 binding trace has %d records, want %d, err=%v",
			len(records), len(suite.Steps), err)
	}
	for index, step := range suite.Steps {
		if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
			t.Fatalf("native M4 binding trace record %d does not match its registered step: %#v",
				index+1, records[index])
		}
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.EvidenceScope != "native-etcd" || !metadata.EmbeddedNative || metadata.Layout != "" ||
		metadata.SuiteSHA256 != suiteDigest || metadata.TraceSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("M4 native binding metadata does not describe the trace: %#v", metadata)
	}

	copyPath := filepath.Join(artifact, "native-bindings-copy.jsonl")
	if err := os.WriteFile(copyPath, traceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseBindingTraces(
		[]string{"--left", tracePath, "--right", copyPath}, io.Discard, io.Discard); err != nil {
		t.Fatalf("compare equivalent native M4 binding traces: %v", err)
	}
}

func TestRunNativeLeaseKeepAliveWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native-keepalive.jsonl")
	metadataPath := filepath.Join(artifact, "native-keepalive.json")
	scenarioPath := filepath.Join(root, "testkit/scenarios/m4-lease-keepalive.yaml")
	suite, suiteDigest, err := scenario.LoadM4LeaseKeepAliveSuite(scenarioPath)
	if err != nil {
		t.Fatalf("load registered M4 KeepAlive scenario: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"run", "--scenario", scenarioPath,
		"--native-data-dir", filepath.Join(artifact, "native-keepalive-data"),
		"--scope", "native-etcd", "--prefix", "etcd-hbase-m4-test/driver/keepalive/",
		"--trace", tracePath, "--metadata", metadataPath}
	if err := run(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatalf("run native M4 KeepAlive scenario: %v", err)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != len(suite.Steps) {
		t.Fatalf("native M4 KeepAlive trace has %d records, want %d, err=%v",
			len(records), len(suite.Steps), err)
	}
	for index, step := range suite.Steps {
		if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
			t.Fatalf("native M4 KeepAlive record %d does not match its registered step: %#v",
				index+1, records[index])
		}
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.EvidenceScope != "native-etcd" || !metadata.EmbeddedNative || metadata.Layout != "" ||
		metadata.SuiteSHA256 != suiteDigest || metadata.TraceSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("M4 native KeepAlive metadata does not describe the trace: %#v", metadata)
	}

	copyPath := filepath.Join(artifact, "native-keepalive-copy.jsonl")
	if err := os.WriteFile(copyPath, traceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseKeepAliveTraces(
		[]string{"--left", tracePath, "--right", copyPath}, io.Discard, io.Discard); err != nil {
		t.Fatalf("compare equivalent native M4 KeepAlive traces: %v", err)
	}
}

func TestRunNativeLeaseRevokeWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native-revoke.jsonl")
	metadataPath := filepath.Join(artifact, "native-revoke.json")
	scenarioPath := filepath.Join(root, "testkit/scenarios/m4-lease-revoke.yaml")
	suite, suiteDigest, err := scenario.LoadM4LeaseRevokeSuite(scenarioPath)
	if err != nil {
		t.Fatalf("load registered M4 Revoke scenario: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"run", "--scenario", scenarioPath,
		"--native-data-dir", filepath.Join(artifact, "native-revoke-data"),
		"--scope", "native-etcd", "--prefix", "etcd-hbase-m4-test/driver/revoke/",
		"--trace", tracePath, "--metadata", metadataPath}
	if err := run(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatalf("run native M4 Revoke scenario: %v", err)
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != len(suite.Steps) {
		t.Fatalf("native M4 Revoke trace has %d records, want %d, err=%v",
			len(records), len(suite.Steps), err)
	}
	for index, step := range suite.Steps {
		if records[index].CaseID != step.ID || records[index].EvidenceScope != "native-etcd" {
			t.Fatalf("native M4 Revoke record %d does not match its registered step: %#v",
				index+1, records[index])
		}
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(traceBytes)
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.EvidenceScope != "native-etcd" || !metadata.EmbeddedNative || metadata.Layout != "" ||
		metadata.SuiteSHA256 != suiteDigest || metadata.TraceSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("M4 native Revoke metadata does not describe the trace: %#v", metadata)
	}

	copyPath := filepath.Join(artifact, "native-revoke-copy.jsonl")
	if err := os.WriteFile(copyPath, traceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseRevokeTraces(
		[]string{"--left", tracePath, "--right", copyPath}, io.Discard, io.Discard); err != nil {
		t.Fatalf("compare equivalent native M4 Revoke traces: %v", err)
	}
}

func TestRunNativeWatchStartWritesFreshTraceAndVerifiableMetadata(t *testing.T) {
	root := repositoryRoot(t)
	artifact := t.TempDir()
	tracePath := filepath.Join(artifact, "native-watch-starts.jsonl")
	metadataPath := filepath.Join(artifact, "native-watch-starts.json")
	dataPath := filepath.Join(artifact, "native-watch-starts-data")
	scenarioPath := filepath.Join(root, "testkit/scenarios/m4-watch-starts.yaml")
	suite, suiteDigest, err := scenario.LoadM4WatchStartSuite(scenarioPath)
	if err != nil {
		t.Fatalf("load registered J45 Watch start scenario: %v", err)
	}
	prefix := "etcd-hbase-m4-test/driver/watch-starts/"
	t.Setenv("M4_RUN_ID", "j45-driver-test")
	t.Setenv("M4_SOURCE_COMMIT", strings.Repeat("a", 40))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var stdout bytes.Buffer
	args := []string{"run", "--scenario", scenarioPath,
		"--native-data-dir", dataPath, "--scope", "native-etcd", "--prefix", prefix,
		"--trace", tracePath, "--metadata", metadataPath}
	if err := run(ctx, args, &stdout, io.Discard); err != nil {
		t.Fatalf("run native J45 Watch start scenario: %v", err)
	}
	if stdout.String() != "M4 Watch start scenario passed\n" {
		t.Fatalf("unexpected J45 success marker: %q", stdout.String())
	}
	traceBytes, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := trace.ReadJSONL(bytes.NewReader(traceBytes))
	if err != nil || len(records) != len(suite.Steps) {
		t.Fatalf("J45 native trace has %d records, want %d, err=%v", len(records), len(suite.Steps), err)
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	traceDigest := sha256.Sum256(traceBytes)
	prefixDigest := sha256.Sum256([]byte(prefix))
	if metadata.Schema != "etcd-hbase-m4-run/v1" || metadata.Status != "passed" ||
		metadata.RunID != "j45-driver-test" || metadata.SourceCommit != strings.Repeat("a", 40) ||
		metadata.Build.ScenarioID != suite.ID || metadata.SuiteSHA256 != suiteDigest ||
		metadata.EvidenceScope != "native-etcd" || metadata.PrefixSHA256 != hex.EncodeToString(prefixDigest[:]) ||
		metadata.TraceSHA256 != hex.EncodeToString(traceDigest[:]) || !metadata.EmbeddedNative || metadata.Layout != "" {
		t.Fatalf("J45 native metadata does not describe its trace: %#v", metadata)
	}
}

func TestParseRunOptionsRejectsMissingLayoutAndUnscopedPrefix(t *testing.T) {
	base := []string{"--scenario", "suite.yaml", "--scope", "target-hbase",
		"--prefix", "etcd-hbase-m4-test/run/", "--endpoint", "127.0.0.1:2379",
		"--trace", "trace.jsonl", "--metadata", "metadata.json"}
	if _, err := parseRunOptions(base, io.Discard); err == nil {
		t.Fatal("target mode without a storage layout was accepted")
	}
	base[5] = "customer/production/"
	base = append(base, "--layout", "HISTORY")
	if _, err := parseRunOptions(base, io.Discard); err == nil {
		t.Fatal("unscoped M4 prefix was accepted")
	}
}

func TestParseRunOptionsPinsLocalWatchPhaseAndCheckpoint(t *testing.T) {
	base := []string{"--scenario", "watch.yaml", "--scope", "local-service",
		"--prefix", "etcd-hbase-m4-test/run/watch/", "--endpoint", "127.0.0.1:2379",
		"--trace", "trace.jsonl", "--metadata", "metadata.json", "--watch-phase", "initial",
		"--watch-revision", "1", "--watch-compact-revision", "-1"}
	options, err := parseRunOptions(base, io.Discard)
	if err != nil || options.watchPhase != "initial" || options.watchRevision != 1 ||
		options.watchCompactRevision != -1 {
		t.Fatalf("valid local Watch checkpoint was rejected or changed: options=%#v err=%v", options, err)
	}
	lifecycleOptions, err := parseRunOptions(append(append([]string(nil), base...), "--watch-lifecycle"), io.Discard)
	if err != nil || !lifecycleOptions.watchLifecycle {
		t.Fatalf("local Watch lifecycle option was rejected or changed: options=%#v err=%v", lifecycleOptions, err)
	}

	for _, invalid := range [][]string{
		{"--watch-phase", "compacted", "--watch-revision", "4", "--watch-compact-revision", "5"},
		{"--watch-phase", "initial", "--watch-revision", "1", "--watch-compact-revision", "0"},
	} {
		arguments := append(append([]string(nil), base[:len(base)-6]...), invalid...)
		if _, err := parseRunOptions(arguments, io.Discard); err == nil {
			t.Fatalf("invalid local Watch checkpoint was accepted: %v", invalid)
		}
	}
}

func TestOpenEvidenceWritesLocalWatchCheckpointMetadata(t *testing.T) {
	artifact := t.TempDir()
	options := runOptions{
		tracePath: filepath.Join(artifact, "trace.jsonl"),
		metadata:  filepath.Join(artifact, "metadata.json"),
		prefix:    "etcd-hbase-m4-test/run/watch/", scope: "local-service",
		watchPhase: "compacted", watchRevision: 8, watchCompactRevision: 4,
	}
	var stdout bytes.Buffer
	output, err := openEvidence(options, scenario.Metadata{}, "suite", "m4-watch-starts-v1", &stdout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(output.trace, "trace\n"); err != nil {
		t.Fatal(err)
	}
	if err := output.finish(nil); err != nil {
		t.Fatal(err)
	}
	var metadata runMetadata
	encoded, err := os.ReadFile(options.metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.EvidenceScope != "local-service" || metadata.WatchPhase != "compacted" ||
		metadata.WatchRevision != 8 || metadata.WatchCompactRevision != 4 || metadata.EmbeddedNative ||
		metadata.Status != "passed" || stdout.String() != "M4 Watch start scenario passed\n" {
		t.Fatalf("local Watch metadata does not match the requested fixture: %#v", metadata)
	}
}

func TestKeepAliveOnceRejectsInvalidLeaseIDBeforeDial(t *testing.T) {
	for _, test := range []struct {
		name    string
		leaseID string
	}{
		{name: "missing", leaseID: ""},
		{name: "zero", leaseID: "0"},
		{name: "malformed", leaseID: "not-a-number"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"keepalive-once", "--endpoint", "127.0.0.1:1", "--lease-id", test.leaseID}
			if err := run(context.Background(), args, io.Discard, io.Discard); err == nil {
				t.Fatal("invalid Lease ID was accepted")
			}
		})
	}
}

func TestKeepAliveOnceRequiresAnExplicitEndpoint(t *testing.T) {
	args := []string{"keepalive-once", "--lease-id", "1"}
	if err := run(context.Background(), args, io.Discard, io.Discard); err == nil {
		t.Fatal("missing endpoint was accepted")
	}
}

func TestCompareRejectsCorruptOrEmptyTrace(t *testing.T) {
	directory := t.TempDir()
	left := filepath.Join(directory, "left.jsonl")
	right := filepath.Join(directory, "right.jsonl")
	if err := os.WriteFile(left, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(right, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseTraces([]string{"--left", left, "--right", right}, io.Discard, io.Discard); err == nil {
		t.Fatal("incomplete M4 traces were accepted")
	}
}

func TestCompareBindingsRejectsCorruptOrEmptyTrace(t *testing.T) {
	for _, test := range []struct {
		name        string
		left, right []byte
	}{
		{name: "corrupt", left: []byte("{}\n")},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			left := filepath.Join(directory, "left.jsonl")
			right := filepath.Join(directory, "right.jsonl")
			if err := os.WriteFile(left, test.left, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(right, test.right, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := compareLeaseBindingTraces(
				[]string{"--left", left, "--right", right}, io.Discard, io.Discard); err == nil {
				t.Fatal("incomplete M4 binding traces were accepted")
			}
		})
	}
}

func TestCompareKeepAliveRejectsCorruptOrMismatchedTrace(t *testing.T) {
	directory := t.TempDir()
	left := filepath.Join(directory, "left.jsonl")
	right := filepath.Join(directory, "right.jsonl")
	if err := os.WriteFile(left, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(right, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := compareLeaseKeepAliveTraces(
		[]string{"--left", left, "--right", right}, io.Discard, io.Discard); err == nil {
		t.Fatal("corrupt M4 KeepAlive traces were accepted")
	}
}

func TestCompareRevokeRejectsCorruptOrEmptyTrace(t *testing.T) {
	for _, test := range []struct {
		name        string
		left, right []byte
	}{
		{name: "corrupt", left: []byte("{}\n")},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			left := filepath.Join(directory, "left.jsonl")
			right := filepath.Join(directory, "right.jsonl")
			if err := os.WriteFile(left, test.left, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(right, test.right, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := compareLeaseRevokeTraces(
				[]string{"--left", left, "--right", right}, io.Discard, io.Discard); err == nil {
				t.Fatal("incomplete M4 Revoke traces were accepted")
			}
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path is unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../.."))
}
