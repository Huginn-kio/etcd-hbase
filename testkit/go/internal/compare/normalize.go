package compare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"strconv"
)

// A normalizer owns correspondence for the whole trace, including requests
// and responses. Record and container helpers never reset that correspondence.
type traceNormalizer struct {
	baseRevision      int64
	identities        map[string]map[string]string
	identitySequences map[string]uint64
}

func normalize(records []trace.Record) ([]map[string]any, error) {
	if err := validateTrace(records); err != nil {
		return nil, err
	}
	baseRevision, err := baselineRevision(records)
	if err != nil {
		return nil, err
	}
	normalizer := traceNormalizer{
		baseRevision:      baseRevision,
		identities:        map[string]map[string]string{"cluster": {}, "member": {}, "lease": {}},
		identitySequences: make(map[string]uint64),
	}
	normalized := make([]map[string]any, 0, len(records))
	for _, record := range records {
		converted, err := normalizer.record(record)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, converted)
	}
	return normalized, nil
}

func validateTrace(records []trace.Record) error {
	if len(records) == 0 {
		return errors.New("trace is empty")
	}
	lastOrdinalByCase := make(map[string]uint64)
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if record.Ordinal <= lastOrdinalByCase[record.CaseID] {
			return errors.New("record ordinals must increase within each case")
		}
		lastOrdinalByCase[record.CaseID] = record.Ordinal
	}
	return nil
}

func baselineRevision(records []trace.Record) (int64, error) {
	if records[0].CaseID != "native-baseline" || records[0].Error != nil {
		return 0, errors.New("trace must begin with a successful native baseline")
	}
	for _, record := range records {
		if record.CaseID != "native-baseline" || len(record.Response) == 0 || bytes.Equal(record.Response, []byte("null")) {
			continue
		}
		response, err := decodeObject(record.Response)
		if err != nil {
			return 0, fmt.Errorf("decode baseline response: %w", err)
		}
		revision, err := responseRevision(response)
		if err != nil {
			return 0, fmt.Errorf("read scenario baseline revision: %w", err)
		}
		if revision < 1 {
			return 0, errors.New("baseline revision must be positive")
		}
		return revision, nil
	}
	return 0, errors.New("trace has no native-baseline scenario record")
}

func (normalizer *traceNormalizer) record(record trace.Record) (map[string]any, error) {
	request, err := decodeNullable(record.Request)
	if err != nil {
		return nil, fmt.Errorf("decode request in trace record %d: invalid snapshot JSON", record.Ordinal)
	}
	response, err := decodeNullable(record.Response)
	if err != nil {
		return nil, fmt.Errorf("decode response in trace record %d: invalid snapshot JSON", record.Ordinal)
	}
	request, err = normalizer.value(request, record.Method)
	if err != nil {
		return nil, fmt.Errorf("normalize request in trace record %d: %w", record.Ordinal, err)
	}
	response, err = normalizer.value(response, record.Method)
	if err != nil {
		return nil, fmt.Errorf("normalize response in trace record %d: %w", record.Ordinal, err)
	}
	return map[string]any{
		"schema":          record.Schema,
		"case_id":         record.CaseID,
		"ordinal":         record.Ordinal,
		"method":          record.Method,
		"request":         request,
		"response":        response,
		"error":           record.Error,
		"stream_terminal": record.StreamTerminal,
		"evidence_scope":  record.EvidenceScope,
	}, nil
}

func decodeNullable(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid snapshot JSON")
	}
	return value, nil
}

func decodeObject(raw json.RawMessage) (map[string]any, error) {
	value, err := decodeNullable(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("expected protobuf snapshot object")
	}
	return object, nil
}

func responseRevision(response map[string]any) (int64, error) {
	fields, err := nestedFields(response, "fields", "header", "fields")
	if err != nil {
		return 0, err
	}
	return decimalField(fields, "revision")
}

func nestedFields(value map[string]any, keys ...string) (map[string]any, error) {
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("missing object field %q", key)
		}
		current = next
	}
	return current, nil
}

func decimalField(fields map[string]any, key string) (int64, error) {
	value, ok := fields[key].(string)
	if !ok {
		return 0, fmt.Errorf("field %q is not a decimal string", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("field %q is not a canonical int64", key)
	}
	return parsed, nil
}
