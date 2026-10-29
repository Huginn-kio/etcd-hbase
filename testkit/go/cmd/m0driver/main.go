package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/wire"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "wire" {
		return runWire(args[1:], stdout, stderr)
	}
	command, err := parseRange(args, stderr)
	if err != nil {
		return err
	}
	return command.execute(stdout)
}

func (command rangeCommand) execute(stdout io.Writer) (runErr error) {
	requestSnapshot, err := trace.SnapshotProto(command.request)
	if err != nil {
		return errors.New("request snapshot failed")
	}
	record := trace.Record{Schema: trace.SchemaVersion, CaseID: command.caseID, Ordinal: 1, Method: "etcdserverpb.KV/Range", Request: requestSnapshot, EvidenceScope: command.scope, ElapsedNS: "0"}
	if err := record.ValidateMetadata(); err != nil {
		return errors.New("case metadata is invalid")
	}
	connection, err := client.New(client.Options{Endpoint: command.endpoint, Timeout: command.timeout})
	if err != nil {
		return errors.New("client initialization failed")
	}
	defer func() { runErr = errors.Join(runErr, connection.Close()) }()
	output, closeOutput, err := openTrace(command.tracePath, stdout)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeOutput()) }()
	ctx, cancel, err := client.WithDeadline(context.Background(), command.timeout)
	if err != nil {
		return err
	}
	defer cancel()
	started := time.Now()
	response, callErr := command.call(ctx, connection)
	record.ElapsedNS = strconv.FormatInt(time.Since(started).Nanoseconds(), 10)
	record.Response, err = trace.SnapshotNullableProto(response)
	if err != nil {
		return errors.New("response snapshot failed")
	}
	record.Error = trace.ClassifyError(callErr)
	if err := trace.WriteJSONL(output, record); err != nil {
		return errors.New("trace write failed")
	}
	if callErr != nil {
		return fmt.Errorf("request failed: grpc_code=%s", record.Error.GRPCCode)
	}
	return nil
}

func runWire(args []string, stdout, stderr io.Writer) (runErr error) {
	flags := flag.NewFlagSet("m0driver wire", flag.ContinueOnError)
	flags.SetOutput(stderr)
	skeleton := flags.String("skeleton-endpoint", "", "Java skeleton gRPC endpoint")
	fixture := flags.String("fixture-endpoint", "", "test-only failure fixture endpoint")
	tracePath := flags.String("trace", "-", "JSONL output path, or - for stdout")
	timeout := flags.Duration("timeout", 3*time.Second, "per-request deadline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	options := wire.Options{SkeletonEndpoint: *skeleton, FixtureEndpoint: *fixture, Timeout: *timeout}
	if err := options.Validate(); err != nil {
		return err
	}
	output, closeOutput, err := openTrace(*tracePath, stdout)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeOutput()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := wire.Run(ctx, options, output); err != nil {
		return err
	}
	if *tracePath != "-" {
		_, err = fmt.Fprintln(stdout, "wire interoperability probe passed")
	}
	return err
}

func openTrace(path string, stdout io.Writer) (io.Writer, func() error, error) {
	if path == "-" {
		return stdout, func() error { return nil }, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nil, errors.New("trace output could not be opened")
	}
	return file, file.Close, nil
}
