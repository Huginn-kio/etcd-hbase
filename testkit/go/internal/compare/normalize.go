package compare

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/trace"
	"strconv"
)

func normalize(records []trace.Record) ([]map[string]any, error) {
	if len(records) == 0 {
		return nil, errors.New("trace is empty")
	}
	last := make(map[string]uint64)
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return nil, err
		}
		if record.Ordinal <= last[record.CaseID] {
			return nil, errors.New("record ordinals must increase within each case")
		}
		last[record.CaseID] = record.Ordinal
	}
	if records[0].CaseID != "native-baseline" || records[0].Error != nil {
		return nil, errors.New("trace must begin with a successful native baseline")
	}
	baseRevision, found := int64(0), false
	for _, record := range records {
		if record.CaseID != "native-baseline" || len(record.Response) == 0 || bytes.Equal(record.Response, []byte("null")) {
			continue
		}
		response, err := decodeObject(record.Response)
		if err != nil {
			return nil, fmt.Errorf("decode baseline response: %w", err)
		}
		revision, err := responseRevision(response)
		if err != nil {
			return nil, fmt.Errorf("read scenario baseline revision: %w", err)
		}
		if revision < 1 {
			return nil, errors.New("baseline revision must be positive")
		}
		baseRevision, found = revision, true
		break
	}
	if !found {
		return nil, errors.New("trace has no native-baseline scenario record")
	}

	ids := map[string]map[string]string{
		"cluster": {},
		"member":  {},
		"lease":   {},
	}
	sequences := map[string]uint64{}
	normalized := make([]map[string]any, 0, len(records))
	for _, record := range records {
		request, err := decodeNullable(record.Request)
		if err != nil {
			return nil, fmt.Errorf("decode request in trace record %d: invalid snapshot JSON", record.Ordinal)
		}
		response, err := decodeNullable(record.Response)
		if err != nil {
			return nil, fmt.Errorf("decode response in trace record %d: invalid snapshot JSON", record.Ordinal)
		}
		request, err = normalizeValue(request, record.Method, baseRevision, ids, sequences)
		if err != nil {
			return nil, fmt.Errorf("normalize request in trace record %d: %w", record.Ordinal, err)
		}
		response, err = normalizeValue(response, record.Method, baseRevision, ids, sequences)
		if err != nil {
			return nil, fmt.Errorf("normalize response in trace record %d: %w", record.Ordinal, err)
		}
		normalized = append(normalized, map[string]any{
			"schema":          record.Schema,
			"case_id":         record.CaseID,
			"ordinal":         record.Ordinal,
			"method":          record.Method,
			"request":         request,
			"response":        response,
			"error":           record.Error,
			"stream_terminal": record.StreamTerminal,
			"evidence_scope":  record.EvidenceScope,
		})
	}
	return normalized, nil
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
