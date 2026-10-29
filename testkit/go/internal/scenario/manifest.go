package scenario

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sigs.k8s.io/yaml"
	"sort"
	"time"
)

const (
	protocolVersion = "etcd-v3.5.23"
	sourceRevision  = "1cf4bc40111fd95d1e14a90a09a146357b15eb13"
	requestTimeout  = 8 * time.Second
	readyTimeout    = 30 * time.Second
)

var expectedSteps = []Step{
	{CaseID: "C01", ID: "binary-put-get"},
	{CaseID: "C03", ID: "prefix-limit-more"},
	{CaseID: "C05", ID: "prefix-count-only-limit-one"},
	{CaseID: "C11", ID: "txn-compare-success-branch"},
	{CaseID: "C12", ID: "txn-compare-failure-branch"},
	{CaseID: "C09", ID: "delete-prev-kv"},
	{CaseID: "C15", ID: "lease-grant"},
	{CaseID: "C16", ID: "put-with-lease"},
	{CaseID: "C17", ID: "lease-keepalive-stream"},
	{CaseID: "C18", ID: "lease-revoke-and-key-removal"},
	{CaseID: "C19", ID: "list-revision-then-watch-r-plus-one"},
	{CaseID: "C14", ID: "compact"},
	{CaseID: "C06", ID: "old-revision-after-compact"},
}

type Suite struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Protocol      string `json:"protocol"`
	Steps         []Step `json:"steps"`
}

type Step struct {
	CaseID string `json:"case_id"`
	ID     string `json:"id"`
}

type Module struct {
	Path    string `json:"path"`
	Version string `json:"version"`
}

type Metadata struct {
	ScenarioID     string   `json:"scenario_id"`
	Protocol       string   `json:"protocol"`
	SourceRevision string   `json:"source_revision"`
	GoVersion      string   `json:"go_version"`
	Distribution   string   `json:"distribution"`
	Modules        []Module `json:"modules"`
}

func LoadSuite(path string) (Suite, error) {
	file, err := os.Open(path)
	if err != nil {
		return Suite{}, fmt.Errorf("read native scenario: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65_537))
	if err != nil || len(data) > 65_536 {
		return Suite{}, errors.New("native scenario exceeds the readable 65536-byte limit")
	}
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return Suite{}, fmt.Errorf("parse native scenario: %w", err)
	}
	if err := validateSuite(suite); err != nil {
		return Suite{}, err
	}
	return suite, nil
}

func validateSuite(suite Suite) error {
	if suite.SchemaVersion != 1 || suite.ID != "m0-native-v1" || suite.Protocol != protocolVersion {
		return errors.New("native scenario identity or protocol version is not pinned")
	}
	if len(suite.Steps) != len(expectedSteps) {
		return fmt.Errorf("native scenario has %d steps, expected %d", len(suite.Steps), len(expectedSteps))
	}
	for index, expected := range expectedSteps {
		if suite.Steps[index] != expected {
			return fmt.Errorf("native scenario step %d differs from the pinned manifest", index+1)
		}
	}
	return nil
}

func BuildMetadata(suite Suite) (Metadata, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return Metadata{}, errors.New("Go build information is unavailable")
	}
	modules := make([]Module, 0, len(info.Deps))
	for _, dependency := range info.Deps {
		if dependency.Path == "go.etcd.io/etcd/server/v3" ||
			dependency.Path == "go.etcd.io/etcd/api/v3" ||
			dependency.Path == "go.etcd.io/etcd/client/v3" {
			modules = append(modules, Module{Path: dependency.Path, Version: dependency.Version})
		}
	}
	sort.Slice(modules, func(left, right int) bool { return modules[left].Path < modules[right].Path })
	if len(modules) != 3 {
		return Metadata{}, fmt.Errorf("expected build metadata for server, api, and client modules; got %d", len(modules))
	}
	for _, module := range modules {
		if module.Version != "v3.5.23" {
			return Metadata{}, fmt.Errorf("module %s is %s, expected v3.5.23", module.Path, module.Version)
		}
	}
	return Metadata{
		ScenarioID: suite.ID, Protocol: suite.Protocol, SourceRevision: sourceRevision,
		GoVersion: runtime.Version(), Distribution: "embedded Go library; no upstream etcd binary or container image validated",
		Modules: modules,
	}, nil
}
