package scenario

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/yaml"
)

var expectedM3PutSteps = []Step{
	{CaseID: "C01", ID: "native-baseline"},
	{CaseID: "C07", ID: "J30-update"},
	{CaseID: "C07", ID: "J30-same-value"},
	{CaseID: "C07", ID: "J30-ignore-value"},
	{CaseID: "C07", ID: "J30-ignore-lease"},
	{CaseID: "C07", ID: "J30-ignore-both"},
	{CaseID: "C07", ID: "J30-client-ignore-value"},
	{CaseID: "C07", ID: "J30-client-ignore-lease"},
	{CaseID: "C01", ID: "J30-long-key"},
	{CaseID: "C01", ID: "J30-empty-key"},
	{CaseID: "C07", ID: "J30-value-provided"},
	{CaseID: "C07", ID: "J30-lease-provided"},
	{CaseID: "C07", ID: "J30-missing-ignore"},
	{CaseID: "C07", ID: "J30-request-too-large"},
}

var expectedM3TxnSteps = txnSteps()

var expectedM3NestedTxnSteps = func() []Step {
	steps := []Step{{CaseID: "C13", ID: "native-baseline"}}
	for _, id := range []string{"branch-base", "historical-reads", "nested-else", "unselected-dynamic",
		"mutually-exclusive", "sibling-duplicate", "parent-overlap", "dynamic-priority",
		"future-revision", "negative-revision", "unselected-empty-key"} {
		steps = append(steps, Step{CaseID: "C13", ID: "J32-" + id})
	}
	return steps
}()

func txnSteps() []Step {
	steps := []Step{{CaseID: "C11", ID: "native-baseline"}}
	for _, target := range []string{"version", "create", "mod", "value", "lease"} {
		for _, relation := range []string{"equal", "not-equal", "greater", "less"} {
			steps = append(steps, Step{CaseID: "C11", ID: fmt.Sprintf("J31-compare-%s-%s", target, relation)})
		}
	}
	for _, id := range []string{"J31-compare-missing-version", "J31-compare-missing-value",
		"J31-compare-range", "J31-then-write", "J31-else-write", "J31-no-change",
		"J31-client-read", "J31-empty-compare", "J31-duplicate-put"} {
		steps = append(steps, Step{CaseID: "C12", ID: id})
	}
	return steps
}

// LoadM3PutSuite rejects partial, modified, or version-mismatched coverage declarations.
// The returned digest describes the exact manifest bytes used by this run.
func LoadM3PutSuite(path string) (Suite, string, error) {
	suite, digest, err := LoadM3Suite(path)
	if err != nil {
		return Suite{}, "", err
	}
	if suite.ID != "m3-put-v1" {
		return Suite{}, "", errors.New("M3 scenario is not the Put suite")
	}
	return suite, digest, nil
}

// LoadM3Suite accepts only explicitly registered, complete coverage manifests.
func LoadM3Suite(path string) (Suite, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return Suite{}, "", errors.New("M3 scenario manifest cannot be opened")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65_537))
	if err != nil || len(data) == 0 || len(data) > 65_536 {
		return Suite{}, "", errors.New("M3 scenario manifest is empty or oversized")
	}
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return Suite{}, "", errors.New("M3 scenario manifest is invalid")
	}
	var expected []Step
	switch suite.ID {
	case "m3-put-v1":
		expected = expectedM3PutSteps
	case "m3-txn-v1":
		expected = expectedM3TxnSteps
	case "m3-nested-txn-v1":
		expected = expectedM3NestedTxnSteps
	default:
		return Suite{}, "", errors.New("M3 scenario identity is not registered")
	}
	if suite.SchemaVersion != 1 || suite.Protocol != protocolVersion || len(suite.Steps) != len(expected) {
		return Suite{}, "", errors.New("M3 scenario identity or coverage is not pinned")
	}
	for index, step := range expected {
		if suite.Steps[index] != step {
			return Suite{}, "", errors.New("M3 scenario coverage differs from its pinned steps")
		}
	}
	digest := sha256.Sum256(data)
	return suite, hex.EncodeToString(digest[:]), nil
}
