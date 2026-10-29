package compare

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"io"
	"reflect"
)

// CompareTraces compares two complete JSONL traces after applying only the
// protocol normalizations approved for M0: elapsed time, stable entity ID
// correspondence, and one scenario-relative revision offset.
func CompareTraces(left, right io.Reader) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read left trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read right trace: %w", err)
	}
	return Compare(leftRecords, rightRecords)
}

// NormalizeTrace writes the permitted comparison representation for one
// complete trace so both normalized sides remain inspectable as evidence.
func NormalizeTrace(input io.Reader, output io.Writer) error {
	if output == nil {
		return errors.New("normalized trace writer is required")
	}
	records, err := trace.ReadJSONL(input)
	if err != nil {
		return fmt.Errorf("read trace: %w", err)
	}
	normalized, err := normalize(records)
	if err != nil {
		return fmt.Errorf("normalize trace: %w", err)
	}
	encoder := json.NewEncoder(output)
	for _, record := range normalized {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write normalized trace: %w", err)
		}
	}
	return nil
}

// Compare rejects missing, reordered, or semantically different records.
func Compare(left, right []trace.Record) error {
	leftScope, rightScope, err := comparisonScopes(left, right)
	if err != nil {
		return err
	}
	leftNormalized, err := normalize(left)
	if err != nil {
		return fmt.Errorf("normalize left trace: %w", err)
	}
	rightNormalized, err := normalize(right)
	if err != nil {
		return fmt.Errorf("normalize right trace: %w", err)
	}
	if len(leftNormalized) != len(rightNormalized) {
		return fmt.Errorf("record count differs: left=%d right=%d", len(leftNormalized), len(rightNormalized))
	}
	for index := range leftNormalized {
		// Evidence provenance is checked above; it is not a wire field.
		rightNormalized[index]["evidence_scope"] = leftNormalized[index]["evidence_scope"]
		if leftScope != rightScope && existenceCountRequest(leftNormalized[index]) {
			if !equalWithCountAllowance(leftNormalized[index], rightNormalized[index], leftScope, rightScope) {
				return fmt.Errorf("trace record %d violates the existence Count policy", index+1)
			}
			continue
		}
		if reflect.DeepEqual(leftNormalized[index], rightNormalized[index]) {
			continue
		}
		if !equalWithCountAllowance(leftNormalized[index], rightNormalized[index], leftScope, rightScope) {
			return fmt.Errorf("trace record %d differs", index+1)
		}
	}
	return nil
}

func comparisonScopes(left, right []trace.Record) (string, string, error) {
	scopes := make([]string, 0, 2)
	for _, records := range [][]trace.Record{left, right} {
		if len(records) == 0 {
			return "", "", errors.New("trace is empty")
		}
		scope := records[0].EvidenceScope
		if scope != "native-etcd" && scope != "target-hbase" {
			return "", "", errors.New("oracle requires native-etcd or target-hbase evidence")
		}
		for _, record := range records {
			if record.EvidenceScope != scope {
				return "", "", errors.New("oracle cannot mix evidence scopes within one trace")
			}
			if record.Error != nil && record.Error.SemanticKind == "unclassified" {
				return "", "", errors.New("unclassified errors cannot establish equivalence")
			}
		}
		scopes = append(scopes, scope)
	}
	return scopes[0], scopes[1], nil
}
