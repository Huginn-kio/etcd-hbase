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
	"syscall"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
)

type options struct {
	endpoint   string
	nativeData string
	scenario   string
	scope      string
	prefix     string
	trace      string
	metadata   string
	layout     string
	timeout    time.Duration
}

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

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (runErr error) {
	options, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	suite, suiteSHA256, err := scenario.LoadM3Suite(options.scenario)
	if err != nil {
		return err
	}
	build, err := scenario.BuildMetadata(suite)
	if err != nil {
		return errors.New("M3 pinned build metadata is unavailable")
	}
	if options.nativeData == "" {
		build.Distribution = "remote endpoint; Go build metadata does not attest the server version"
	}
	evidence, err := openEvidence(options, build, suiteSHA256, stdout)
	if err != nil {
		return err
	}
	defer func() { runErr = evidence.finish(runErr) }()
	return executeScenario(ctx, options, suite.ID, evidence.trace)
}

type evidenceOutput struct {
	options     options
	build       scenario.Metadata
	suiteSHA256 string
	stdout      io.Writer
	trace       io.Writer
	traceHash   hash.Hash
	closeTrace  func() error
	metadata    *os.File
}

func openEvidence(options options, build scenario.Metadata, suiteSHA256 string, stdout io.Writer) (*evidenceOutput, error) {
	output, closeOutput, err := openTrace(options.trace, stdout)
	if err != nil {
		return nil, err
	}
	metadataFile, err := os.OpenFile(options.metadata, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.Join(errors.New("M3 metadata output could not be opened"), closeOutput())
	}
	traceHash := sha256.New()
	return &evidenceOutput{options: options, build: build, suiteSHA256: suiteSHA256, stdout: stdout,
		trace: io.MultiWriter(output, traceHash), traceHash: traceHash, closeTrace: closeOutput, metadata: metadataFile}, nil
}

func (output *evidenceOutput) finish(runErr error) error {
	runErr = errors.Join(runErr, output.closeTrace())
	status := "passed"
	if runErr != nil {
		status = "failed"
	}
	prefixHash := sha256.Sum256([]byte(output.options.prefix))
	evidence := runMetadata{Build: output.build, SuiteSHA256: output.suiteSHA256, EvidenceScope: output.options.scope,
		PrefixSHA256: hex.EncodeToString(prefixHash[:]), TraceSHA256: hex.EncodeToString(output.traceHash.Sum(nil)),
		EmbeddedNative: output.options.nativeData != "", Layout: output.options.layout, Status: status}
	runErr = errors.Join(runErr, json.NewEncoder(output.metadata).Encode(evidence), output.metadata.Close())
	if runErr == nil && output.options.trace != "-" {
		_, runErr = fmt.Fprintln(output.stdout, "M3 scenario passed")
	}
	return runErr
}

func executeScenario(ctx context.Context, options options, suiteID string, output io.Writer) (runErr error) {
	requestContext, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	if options.nativeData != "" {
		runNative := scenario.RunM3NativePut
		switch suiteID {
		case "m3-txn-v1":
			runNative = scenario.RunM3NativeTxn
		case "m3-nested-txn-v1":
			runNative = scenario.RunM3NativeNestedTxn
		case "m3-delete-v1":
			runNative = scenario.RunM3NativeDelete
		case "m3-range-v1":
			runNative = scenario.RunM3NativeRange
		}
		if err := runNative(requestContext, options.nativeData, options.prefix, output); err != nil {
			return err
		}
	} else {
		connection, err := client.New(client.Options{Endpoint: options.endpoint, Timeout: options.timeout})
		if err != nil {
			return errors.New("M3 client initialization failed")
		}
		defer func() { runErr = errors.Join(runErr, connection.Close()) }()
		runScenario := scenario.RunM3Put
		switch suiteID {
		case "m3-txn-v1":
			runScenario = scenario.RunM3Txn
		case "m3-nested-txn-v1":
			runScenario = scenario.RunM3NestedTxn
		case "m3-delete-v1":
			runScenario = scenario.RunM3Delete
		case "m3-range-v1":
			runScenario = scenario.RunM3Range
		}
		if err := runScenario(requestContext, connection, options.scope, options.prefix, output); err != nil {
			return err
		}
	}
	return nil
}

type runMetadata struct {
	Build          scenario.Metadata `json:"build"`
	SuiteSHA256    string            `json:"suite_sha256"`
	EvidenceScope  string            `json:"evidence_scope"`
	PrefixSHA256   string            `json:"prefix_sha256"`
	TraceSHA256    string            `json:"trace_sha256"`
	EmbeddedNative bool              `json:"embedded_native"`
	Layout         string            `json:"layout"`
	Status         string            `json:"status"`
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("m3driver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var result options
	flags.StringVar(&result.endpoint, "endpoint", "", "explicit etcd-compatible endpoint")
	flags.StringVar(&result.nativeData, "native-data-dir", "", "new isolated embedded etcd data directory")
	flags.StringVar(&result.scenario, "scenario", "", "registered pinned M3 scenario manifest")
	flags.StringVar(&result.scope, "scope", "", "native-etcd or target-hbase")
	flags.StringVar(&result.layout, "layout", "", "target layout HISTORY or NATIVE")
	flags.StringVar(&result.prefix, "prefix", "", "fresh isolated M3 test prefix")
	flags.StringVar(&result.trace, "trace", "", "new JSONL trace path, or - for stdout")
	flags.StringVar(&result.metadata, "metadata", "", "new JSON evidence metadata path")
	flags.DurationVar(&result.timeout, "timeout", 2*time.Minute, "whole-scenario deadline")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 || result.trace == "" || result.metadata == "" || result.metadata == "-" || result.scenario == "" ||
		result.timeout <= 0 || result.timeout > 5*time.Minute {
		return options{}, errors.New("M3 driver requires a scenario, trace, metadata, and deadline up to five minutes")
	}
	if result.nativeData != "" {
		if result.endpoint != "" || result.scope != "native-etcd" {
			return options{}, errors.New("embedded native mode requires native-etcd scope and no endpoint")
		}
	} else {
		if err := (client.Options{Endpoint: result.endpoint, Timeout: result.timeout}).Validate(); err != nil {
			return options{}, err
		}
	}
	if err := scenario.ValidateM3Put(result.scope, result.prefix); err != nil {
		return options{}, err
	}
	if result.scope == "native-etcd" && result.layout != "" ||
		result.scope == "target-hbase" && result.layout != "HISTORY" && result.layout != "NATIVE" {
		return options{}, errors.New("target scope requires an explicit HISTORY or NATIVE layout")
	}
	return result, nil
}

func openTrace(path string, stdout io.Writer) (io.Writer, func() error, error) {
	if path == "-" {
		return stdout, func() error { return nil }, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nil, errors.New("M3 trace output could not be opened")
	}
	return file, file.Close, nil
}
