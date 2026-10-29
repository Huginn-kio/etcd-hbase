package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
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
	if len(args) == 0 {
		return errors.New("usage: native-etcd run|compare|normalize [options]")
	}
	switch args[0] {
	case "run":
		return runScenario(args[1:], stdout, stderr)
	case "compare":
		return compareTraces(args[1:], stderr)
	case "normalize":
		return normalizeTrace(args[1:], stderr)
	default:
		return errors.New("usage: native-etcd run|compare|normalize [options]")
	}
}

func normalizeTrace(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("native-etcd normalize", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "", "complete input trace JSONL")
	outputPath := flags.String("output", "", "normalized evidence JSONL output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *inputPath == "" || *outputPath == "" {
		return errors.New("normalize requires --input and --output")
	}
	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("open input trace: %w", err)
	}
	defer input.Close()
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open normalized output: %w", err)
	}
	normalizeErr := compare.NormalizeTrace(input, output)
	closeErr := output.Close()
	if normalizeErr != nil {
		return normalizeErr
	}
	if closeErr != nil {
		return fmt.Errorf("close normalized output: %w", closeErr)
	}
	return nil
}

type scenarioOptions struct {
	scenarioPath string
	artifactRoot string
	tracePath    string
	metadataPath string
}

type scenarioPaths struct {
	artifactRoot  string
	traceFile     string
	metadataFile  string
	dataDirectory string
}

func runScenario(args []string, stdout, stderr io.Writer) error {
	options, err := parseScenarioOptions(args, stderr)
	if err != nil {
		return err
	}
	paths, err := prepareScenarioPaths(options)
	if err != nil {
		return err
	}
	suite, err := scenario.LoadSuite(options.scenarioPath)
	if err != nil {
		return err
	}
	artifactFS, err := os.OpenRoot(paths.artifactRoot)
	if err != nil {
		return errors.New("open native artifact directory failed")
	}
	defer artifactFS.Close()
	return executeNativeScenario(suite, paths, artifactFS, stdout)
}

func parseScenarioOptions(args []string, stderr io.Writer) (scenarioOptions, error) {
	options := scenarioOptions{}
	flags := flag.NewFlagSet("native-etcd run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.scenarioPath, "scenario", "../scenarios/m0-native.yaml", "pinned native scenario manifest")
	flags.StringVar(&options.artifactRoot, "artifact-root", "", "ignored directory for all native etcd data and outputs")
	flags.StringVar(&options.tracePath, "trace", "", "JSONL request/response trace output")
	flags.StringVar(&options.metadataPath, "metadata", "", "JSON build and distribution metadata output")
	if err := flags.Parse(args); err != nil {
		return scenarioOptions{}, err
	}
	if flags.NArg() != 0 || options.artifactRoot == "" || options.tracePath == "" || options.metadataPath == "" {
		return scenarioOptions{}, errors.New("run requires --artifact-root, --trace, and --metadata")
	}
	return options, nil
}

func prepareScenarioPaths(options scenarioOptions) (scenarioPaths, error) {
	root, err := filepath.Abs(options.artifactRoot)
	if err != nil {
		return scenarioPaths{}, fmt.Errorf("resolve artifact root: %w", err)
	}
	traceFile, err := insideRoot(root, options.tracePath)
	if err != nil {
		return scenarioPaths{}, err
	}
	metadataFile, err := insideRoot(root, options.metadataPath)
	if err != nil {
		return scenarioPaths{}, err
	}
	dataDirectory, err := insideRoot(root, "etcd-data")
	if err != nil {
		return scenarioPaths{}, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return scenarioPaths{}, fmt.Errorf("create native artifact root: %w", err)
	}
	return scenarioPaths{
		artifactRoot:  root,
		traceFile:     traceFile,
		metadataFile:  metadataFile,
		dataDirectory: dataDirectory,
	}, nil
}

func executeNativeScenario(suite scenario.Suite, paths scenarioPaths, artifactFS *os.Root, stdout io.Writer) error {
	traceFile, err := openTraceOutput(artifactFS, paths.traceFile)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metadata, runErr := scenario.Run(ctx, suite, paths.dataDirectory, traceFile)
	closeErr := traceFile.Close()
	if closeErr != nil {
		return fmt.Errorf("close trace output: %w", closeErr)
	}
	if metadata.ScenarioID != "" {
		if err := writeMetadata(artifactFS, paths.metadataFile, metadata); err != nil {
			return err
		}
	}
	if runErr != nil {
		return runErr
	}
	if _, err := fmt.Fprintf(stdout, "native scenario %s passed; trace=%s metadata=%s\n", suite.ID, paths.traceFile, paths.metadataFile); err != nil {
		return err
	}
	return nil
}

func openTraceOutput(artifactFS *os.Root, path string) (*os.File, error) {
	relative, _ := filepath.Rel(artifactFS.Name(), path)
	if err := artifactFS.MkdirAll(filepath.Dir(relative), 0o700); err != nil {
		return nil, errors.New("create trace directory failed")
	}
	file, err := artifactFS.OpenFile(relative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open trace output: %w", err)
	}
	return file, nil
}

func compareTraces(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("native-etcd compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	leftPath := flags.String("left", "", "left complete trace JSONL")
	rightPath := flags.String("right", "", "right complete trace JSONL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *leftPath == "" || *rightPath == "" {
		return errors.New("compare requires --left and --right")
	}
	left, err := os.Open(*leftPath)
	if err != nil {
		return fmt.Errorf("open left trace: %w", err)
	}
	defer left.Close()
	right, err := os.Open(*rightPath)
	if err != nil {
		return fmt.Errorf("open right trace: %w", err)
	}
	defer right.Close()
	if err := compare.CompareTraces(left, right); err != nil {
		return err
	}
	return nil
}

func writeMetadata(root *os.Root, path string, metadata scenario.Metadata) error {
	relative, err := filepath.Rel(root.Name(), path)
	if err != nil {
		return errors.New("invalid metadata path")
	}
	if err := root.MkdirAll(filepath.Dir(relative), 0o700); err != nil {
		return fmt.Errorf("create metadata directory: %w", err)
	}
	file, err := root.OpenFile(relative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open metadata output: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(metadata)
	closeErr := file.Close()
	if encodeErr != nil {
		return fmt.Errorf("write metadata output: %w", encodeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close metadata output: %w", closeErr)
	}
	return nil
}
