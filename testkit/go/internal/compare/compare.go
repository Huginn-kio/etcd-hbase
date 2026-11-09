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
	return compareTraces(left, right, false)
}

// CompareM3PutTraces also applies the explicitly disclosed non-Raft header term projection.
// It is restricted to the J30 Put suite; other M3 methods need their own registered policy.
func CompareM3PutTraces(left, right io.Reader) error {
	return compareTraces(left, right, true)
}

func compareTraces(left, right io.Reader, m3Put bool) error {
	leftRecords, err := trace.ReadJSONL(left)
	if err != nil {
		return fmt.Errorf("read left trace: %w", err)
	}
	rightRecords, err := trace.ReadJSONL(right)
	if err != nil {
		return fmt.Errorf("read right trace: %w", err)
	}
	return compare(leftRecords, rightRecords, m3Put)
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
	return compare(left, right, false)
}

func compare(left, right []trace.Record, m3Put bool) error {
	leftScope, rightScope, err := comparisonScopes(left, right)
	if err != nil {
		return err
	}
	if m3Put && (leftScope == rightScope ||
		(leftScope != "native-etcd" && rightScope != "native-etcd") ||
		(leftScope != "target-hbase" && rightScope != "target-hbase")) {
		return errors.New("M3 Put comparison requires native and target traces")
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
		bothSucceeded := left[index].Error == nil && right[index].Error == nil &&
			left[index].StreamTerminal == nil && right[index].StreamTerminal == nil
		if m3Put {
			if err := projectM3PutTerm(leftNormalized[index], rightNormalized[index], leftScope, bothSucceeded); err != nil {
				return fmt.Errorf("trace record %d has invalid M3 Put header: %w", index+1, err)
			}
		}
		policy := strictCount
		if leftScope != rightScope && bothSucceeded {
			policy = countPolicyForRequest(leftNormalized[index])
		}
		// A successful target Count must satisfy its contract even when both
		// observed Counts happen to be equal. Failed observations stay strict.
		if policy != strictCount {
			if !equalWithCountAllowance(leftNormalized[index], rightNormalized[index], leftScope, policy) {
				return fmt.Errorf("trace record %d violates the %s Count policy", index+1, policy)
			}
			continue
		}
		if !reflect.DeepEqual(leftNormalized[index], rightNormalized[index]) {
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
