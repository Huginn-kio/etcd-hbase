package scenario

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// LoadM3PutSuite rejects partial, modified, or version-mismatched coverage declarations.
// The returned digest describes the exact manifest bytes used by this run.
func LoadM3PutSuite(path string) (Suite, string, error) {
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
	if suite.SchemaVersion != 1 || suite.ID != "m3-put-v1" || suite.Protocol != protocolVersion ||
		len(suite.Steps) != len(expectedM3PutSteps) {
		return Suite{}, "", errors.New("M3 scenario identity or coverage is not pinned")
	}
	for index, expected := range expectedM3PutSteps {
		if suite.Steps[index] != expected {
			return Suite{}, "", errors.New("M3 scenario coverage differs from its pinned steps")
		}
	}
	digest := sha256.Sum256(data)
	return suite, hex.EncodeToString(digest[:]), nil
}
