package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if ctx == nil || stdout == nil || stderr == nil || len(arguments) == 0 {
		return errors.New("M4 driver requires a command and process streams")
	}
	switch arguments[0] {
	case "run":
		return runLeaseScenario(ctx, arguments[1:], stdout, stderr)
	case "compare":
		return compareLeaseTraces(arguments[1:], stdout, stderr)
	case "compare-bindings":
		return compareLeaseBindingTraces(arguments[1:], stdout, stderr)
	case "compare-keepalive":
		return compareLeaseKeepAliveTraces(arguments[1:], stdout, stderr)
	case "compare-revoke":
		return compareLeaseRevokeTraces(arguments[1:], stdout, stderr)
	case "keepalive-once":
		return runLeaseKeepAliveOnce(ctx, arguments[1:], stdout, stderr)
	default:
		return errors.New("M4 driver command must be run, compare, compare-bindings, compare-keepalive, compare-revoke, or keepalive-once")
	}
}

func runLeaseKeepAliveOnce(parent context.Context, arguments []string, stdout, stderr io.Writer) (runErr error) {
	if parent == nil || stdout == nil || stderr == nil {
		return errors.New("M4 KeepAliveOnce requires context and process streams")
	}
	flags := flag.NewFlagSet("m4driver keepalive-once", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var endpoint string
	var leaseIDText string
	var timeout time.Duration
	flags.StringVar(&endpoint, "endpoint", "", "explicit etcd-compatible endpoint")
	flags.StringVar(&leaseIDText, "lease-id", "", "existing non-zero Lease ID")
	flags.DurationVar(&timeout, "timeout", 30*time.Second, "KeepAliveOnce deadline")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	leaseID, err := strconv.ParseInt(leaseIDText, 10, 64)
	if flags.NArg() != 0 || endpoint == "" || leaseIDText == "" || err != nil || leaseID == 0 || timeout <= 0 || timeout > 5*time.Minute {
		return errors.New("M4 KeepAliveOnce requires an endpoint, non-zero Lease ID, and deadline up to five minutes")
	}
	connection, err := client.New(client.Options{Endpoint: endpoint, Timeout: timeout})
	if err != nil {
		return errors.New("M4 KeepAliveOnce client initialization failed")
	}
	defer func() { runErr = errors.Join(runErr, connection.Close()) }()
	requestContext, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	response, err := connection.KeepAliveOnce(requestContext, clientv3.LeaseID(leaseID))
	if err != nil {
		return fmt.Errorf("M4 KeepAliveOnce request failed: %s: %s",
			status.Code(err), status.Convert(err).Message())
	}
	if response == nil || response.ResponseHeader == nil || response.TTL <= 0 ||
		response.ID != clientv3.LeaseID(leaseID) {
		return errors.New("M4 KeepAliveOnce response did not confirm a live Lease")
	}
	result := struct {
		TTL      int64 `json:"ttl"`
		Revision int64 `json:"revision"`
	}{TTL: response.TTL, Revision: response.ResponseHeader.Revision}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return errors.New("M4 KeepAliveOnce result could not be written")
	}
	return nil
}

type runOptions struct {
	endpoint             string
	nativeData           string
	scenario             string
	scope                string
	prefix               string
	tracePath            string
	metadata             string
	layout               string
	watchPhase           string
	watchRevision        int64
	watchCompactRevision int64
	watchLifecycle       bool
	timeout              time.Duration
}

func runLeaseScenario(parent context.Context, arguments []string, stdout, stderr io.Writer) (runErr error) {
	options, err := parseRunOptions(arguments, stderr)
	if err != nil {
		return err
	}
	suite, suiteDigest, err := scenario.LoadM4LeaseSuite(options.scenario)
	if err != nil {
		suite, suiteDigest, err = scenario.LoadM4LeaseBindingSuite(options.scenario)
	}
	if err != nil {
		suite, suiteDigest, err = scenario.LoadM4LeaseKeepAliveSuite(options.scenario)
	}
	if err != nil {
		suite, suiteDigest, err = scenario.LoadM4LeaseRevokeSuite(options.scenario)
	}
	if err != nil {
		suite, suiteDigest, err = scenario.LoadM4WatchStartSuite(options.scenario)
	}
	if err != nil {
		return err
	}
	if options.scope == "local-service" && suite.ID != "m4-watch-starts-v1" {
		return errors.New("local-service scope is registered only for the J45 Watch start scenario")
	}
	build, err := scenario.BuildMetadata(suite)
	if err != nil {
		return errors.New("M4 pinned build metadata is unavailable")
	}
	if options.nativeData == "" {
		build.Distribution = "remote endpoint; Go build metadata does not attest the server version"
		if options.scope == "local-service" {
			build.Distribution = "local Java WatchService endpoint; Go build metadata does not attest Java runtime"
		}
	}
	evidence, err := openEvidence(options, build, suiteDigest, suite.ID, stdout)
	if err != nil {
		return err
	}
	defer func() { runErr = evidence.finish(runErr) }()

	ctx, cancel := context.WithTimeout(parent, options.timeout)
	defer cancel()
	if options.scope == "local-service" {
		connection, err := client.New(client.Options{Endpoint: options.endpoint, Timeout: options.timeout})
		if err != nil {
			return errors.New("M4 local Watch service client initialization failed")
		}
		defer func() { runErr = errors.Join(runErr, connection.Close()) }()
		if err := scenario.RunM4WatchStartEndpoint(ctx, connection, options.prefix,
			options.watchPhase, options.watchRevision, options.watchCompactRevision, evidence.trace); err != nil {
			return err
		}
		if options.watchLifecycle {
			return scenario.RunM4WatchLifecycleInterop(ctx, connection, options.prefix, stdout)
		}
		return nil
	}
	if options.nativeData != "" {
		if suite.ID == "m4-watch-starts-v1" {
			return scenario.RunM4NativeWatchStart(ctx, options.nativeData, options.prefix, evidence.trace)
		}
		if suite.ID == "m4-lease-bindings-v1" {
			return scenario.RunM4NativeLeaseBindings(ctx, options.nativeData, options.prefix, evidence.trace)
		}
		if suite.ID == "m4-lease-keepalive-v1" {
			return scenario.RunM4NativeLeaseKeepAlive(ctx, options.nativeData, options.prefix, evidence.trace)
		}
		if suite.ID == "m4-lease-revoke-v1" {
			return scenario.RunM4NativeLeaseRevoke(ctx, options.nativeData, options.prefix, evidence.trace)
		}
		return scenario.RunM4NativeLease(ctx, options.nativeData, options.prefix, evidence.trace)
	}
	if suite.ID == "m4-watch-starts-v1" {
		return errors.New("M4 Watch start oracle requires an isolated embedded native etcd run")
	}
	connection, err := client.New(client.Options{Endpoint: options.endpoint, Timeout: options.timeout})
	if err != nil {
		return errors.New("M4 target client initialization failed")
	}
	defer func() { runErr = errors.Join(runErr, connection.Close()) }()
	if suite.ID == "m4-lease-bindings-v1" {
		return scenario.RunM4LeaseBindings(ctx, connection, options.scope, options.prefix, evidence.trace)
	}
	if suite.ID == "m4-lease-keepalive-v1" {
		return scenario.RunM4LeaseKeepAlive(ctx, connection, options.scope, options.prefix, evidence.trace)
	}
	if suite.ID == "m4-lease-revoke-v1" {
		return scenario.RunM4LeaseRevoke(ctx, connection, options.scope, options.prefix, evidence.trace)
	}
	return scenario.RunM4Lease(ctx, connection, options.scope, options.prefix, evidence.trace)
}

func parseRunOptions(arguments []string, stderr io.Writer) (runOptions, error) {
	flags := flag.NewFlagSet("m4driver run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var options runOptions
	flags.StringVar(&options.endpoint, "endpoint", "", "explicit etcd-compatible endpoint")
	flags.StringVar(&options.nativeData, "native-data-dir", "", "new isolated embedded etcd data directory")
	flags.StringVar(&options.scenario, "scenario", "", "registered pinned M4 scenario manifest")
	flags.StringVar(&options.scope, "scope", "", "native-etcd, target-hbase, or local-service")
	flags.StringVar(&options.layout, "layout", "", "target layout HISTORY or NATIVE")
	flags.StringVar(&options.prefix, "prefix", "", "fresh isolated M4 test prefix")
	flags.StringVar(&options.tracePath, "trace", "", "new JSONL trace path")
	flags.StringVar(&options.metadata, "metadata", "", "new JSON evidence metadata path")
	flags.StringVar(&options.watchPhase, "watch-phase", "", "expected local Watch phase: initial or compacted")
	flags.Int64Var(&options.watchRevision, "watch-revision", 0, "expected local Watch published revision")
	flags.Int64Var(&options.watchCompactRevision, "watch-compact-revision", 0,
		"expected local Watch compact revision, or -1 for the initial state")
	flags.BoolVar(&options.watchLifecycle, "watch-lifecycle", false,
		"also verify the local Java WatchService live-event lifecycle")
	flags.DurationVar(&options.timeout, "timeout", 2*time.Minute, "whole-scenario deadline")
	if err := flags.Parse(arguments); err != nil {
		return runOptions{}, err
	}
	if flags.NArg() != 0 || options.scenario == "" || options.tracePath == "" ||
		options.metadata == "" || options.timeout <= 0 || options.timeout > 5*time.Minute {
		return runOptions{}, errors.New("M4 run requires scenario, trace, metadata, and a deadline up to five minutes")
	}
	if options.nativeData != "" {
		if options.endpoint != "" || options.scope != "native-etcd" || options.layout != "" {
			return runOptions{}, errors.New("embedded M4 mode requires native-etcd scope without endpoint or layout")
		}
	} else {
		if err := (client.Options{Endpoint: options.endpoint, Timeout: options.timeout}).Validate(); err != nil {
			return runOptions{}, err
		}
		if options.scope == "local-service" {
			if options.layout != "" || options.watchRevision < 1 || options.watchCompactRevision < -1 ||
				(options.watchPhase != "initial" && options.watchPhase != "compacted") ||
				(options.watchPhase == "initial" && options.watchCompactRevision != -1) ||
				(options.watchPhase == "compacted" && (options.watchCompactRevision < 1 ||
					options.watchCompactRevision > options.watchRevision)) {
				return runOptions{}, errors.New("local M4 Watch mode requires a valid phase and revision checkpoint")
			}
		} else if options.layout != "HISTORY" && options.layout != "NATIVE" {
			return runOptions{}, errors.New("M4 target mode requires an explicit HISTORY or NATIVE layout")
		}
	}
	if err := scenario.ValidateM4Lease(options.scope, options.prefix); err != nil {
		return runOptions{}, err
	}
	if options.scope != "local-service" &&
		(options.watchPhase != "" || options.watchRevision != 0 || options.watchCompactRevision != 0) {
		return runOptions{}, errors.New("Watch checkpoint options require local-service scope")
	}
	if options.watchLifecycle && options.scope != "local-service" {
		return runOptions{}, errors.New("Watch lifecycle interop requires local-service scope")
	}
	if (options.scope == "native-etcd") != (options.nativeData != "") {
		return runOptions{}, errors.New("M4 evidence scope does not match the selected endpoint mode")
	}
	return options, nil
}

type runMetadata struct {
	Schema               string            `json:"schema"`
	RunID                string            `json:"run_id,omitempty"`
	SourceCommit         string            `json:"source_commit,omitempty"`
	Build                scenario.Metadata `json:"build"`
	SuiteSHA256          string            `json:"suite_sha256"`
	EvidenceScope        string            `json:"evidence_scope"`
	PrefixSHA256         string            `json:"prefix_sha256"`
	TraceSHA256          string            `json:"trace_sha256"`
	EmbeddedNative       bool              `json:"embedded_native"`
	Layout               string            `json:"layout"`
	WatchPhase           string            `json:"watch_phase,omitempty"`
	WatchRevision        int64             `json:"watch_revision,omitempty"`
	WatchCompactRevision int64             `json:"watch_compact_revision,omitempty"`
	Status               string            `json:"status"`
}

type evidenceOutput struct {
	options     runOptions
	build       scenario.Metadata
	suiteSHA256 string
	successText string
	stdout      io.Writer
	trace       io.Writer
	traceHash   hash.Hash
	closeTrace  func() error
	metadata    *os.File
}

func openEvidence(options runOptions, build scenario.Metadata, suiteSHA256, scenarioID string,
	stdout io.Writer) (*evidenceOutput, error) {
	if filepath.Clean(options.tracePath) == filepath.Clean(options.metadata) {
		return nil, errors.New("M4 trace and metadata paths must differ")
	}
	if err := os.MkdirAll(filepath.Dir(options.tracePath), 0o700); err != nil {
		return nil, errors.New("M4 trace directory could not be created")
	}
	if err := os.MkdirAll(filepath.Dir(options.metadata), 0o700); err != nil {
		return nil, errors.New("M4 metadata directory could not be created")
	}
	traceFile, err := os.OpenFile(options.tracePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.New("M4 trace output could not be opened exclusively")
	}
	metadataFile, err := os.OpenFile(options.metadata, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.Join(errors.New("M4 metadata output could not be opened exclusively"), traceFile.Close())
	}
	traceHash := sha256.New()
	successText := "M4 Lease scenario passed"
	if scenarioID == "m4-watch-starts-v1" {
		successText = "M4 Watch start scenario passed"
	}
	return &evidenceOutput{options: options, build: build, suiteSHA256: suiteSHA256,
		successText: successText, stdout: stdout,
		trace: io.MultiWriter(traceFile, traceHash), traceHash: traceHash,
		closeTrace: traceFile.Close, metadata: metadataFile}, nil
}

func (output *evidenceOutput) finish(runErr error) error {
	closeErr := output.closeTrace()
	if closeErr != nil {
		runErr = errors.Join(runErr, closeErr)
	}
	status := "passed"
	if runErr != nil {
		status = "failed"
	}
	prefixHash := sha256.Sum256([]byte(output.options.prefix))
	metadata := runMetadata{
		Schema: "etcd-hbase-m4-run/v1", RunID: os.Getenv("M4_RUN_ID"),
		SourceCommit: os.Getenv("M4_SOURCE_COMMIT"), Build: output.build,
		SuiteSHA256: output.suiteSHA256, EvidenceScope: output.options.scope,
		PrefixSHA256: hex.EncodeToString(prefixHash[:]), TraceSHA256: hex.EncodeToString(output.traceHash.Sum(nil)),
		EmbeddedNative: output.options.nativeData != "", Layout: output.options.layout, Status: status,
	}
	if output.options.scope == "local-service" {
		metadata.WatchPhase = output.options.watchPhase
		metadata.WatchRevision = output.options.watchRevision
		metadata.WatchCompactRevision = output.options.watchCompactRevision
	}
	encodeErr := json.NewEncoder(output.metadata).Encode(metadata)
	closeMetadataErr := output.metadata.Close()
	if encodeErr != nil || closeMetadataErr != nil {
		runErr = errors.Join(runErr, errors.New("M4 metadata could not be finalized"), encodeErr, closeMetadataErr)
	}
	if runErr == nil && output.options.tracePath != "-" {
		_, runErr = fmt.Fprintln(output.stdout, output.successText)
	}
	return runErr
}

func compareLeaseTraces(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m4driver compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var left, right string
	flags.StringVar(&left, "left", "", "native trace JSONL")
	flags.StringVar(&right, "right", "", "target trace JSONL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || left == "" || right == "" || left == right {
		return errors.New("M4 compare requires distinct left and right trace files")
	}
	leftFile, err := os.Open(left)
	if err != nil {
		return errors.New("native M4 trace cannot be opened")
	}
	defer leftFile.Close()
	rightFile, err := os.Open(right)
	if err != nil {
		return errors.New("target M4 trace cannot be opened")
	}
	defer rightFile.Close()
	if err := compare.CompareM4LeaseTraces(leftFile, rightFile); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "M4 Lease traces match the registered native comparison policy")
	return err
}

func compareLeaseBindingTraces(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m4driver compare-bindings", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var left, right string
	flags.StringVar(&left, "left", "", "native binding trace JSONL")
	flags.StringVar(&right, "right", "", "target binding trace JSONL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || left == "" || right == "" || left == right {
		return errors.New("M4 binding compare requires distinct left and right trace files")
	}
	leftFile, err := os.Open(left)
	if err != nil {
		return errors.New("native M4 binding trace cannot be opened")
	}
	defer leftFile.Close()
	rightFile, err := os.Open(right)
	if err != nil {
		return errors.New("target M4 binding trace cannot be opened")
	}
	defer rightFile.Close()
	if err := compare.CompareM4LeaseBindingTraces(leftFile, rightFile); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "M4 Lease binding traces match the registered native comparison policy")
	return err
}

func compareLeaseKeepAliveTraces(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m4driver compare-keepalive", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var left, right string
	flags.StringVar(&left, "left", "", "native KeepAlive trace JSONL")
	flags.StringVar(&right, "right", "", "target KeepAlive trace JSONL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || left == "" || right == "" || left == right {
		return errors.New("M4 KeepAlive compare requires distinct native and target trace files")
	}
	leftFile, err := os.Open(left)
	if err != nil {
		return errors.New("native M4 KeepAlive trace cannot be opened")
	}
	defer leftFile.Close()
	rightFile, err := os.Open(right)
	if err != nil {
		return errors.New("target M4 KeepAlive trace cannot be opened")
	}
	defer rightFile.Close()
	if err := compare.CompareM4LeaseKeepAliveTraces(leftFile, rightFile); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "M4 KeepAlive traces match the registered native comparison policy")
	return err
}

func compareLeaseRevokeTraces(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m4driver compare-revoke", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var left, right string
	flags.StringVar(&left, "left", "", "native Revoke trace JSONL")
	flags.StringVar(&right, "right", "", "target Revoke trace JSONL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || left == "" || right == "" || left == right {
		return errors.New("M4 Revoke compare requires distinct native and target trace files")
	}
	leftFile, err := os.Open(left)
	if err != nil {
		return errors.New("native M4 Revoke trace cannot be opened")
	}
	defer leftFile.Close()
	rightFile, err := os.Open(right)
	if err != nil {
		return errors.New("target M4 Revoke trace cannot be opened")
	}
	defer rightFile.Close()
	if err := compare.CompareM4LeaseRevokeTraces(leftFile, rightFile); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "M4 Revoke traces match the registered native comparison policy")
	return err
}
