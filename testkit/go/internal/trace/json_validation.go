package trace

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const maxJSONDepth = 256
const maxJSONValues = 1_000_000

func validateObject(raw json.RawMessage, label string, allowNull, authMethod bool) error {
	if len(raw) == 0 {
		return fmt.Errorf("%s is required", label)
	}
	if len(raw) > maxRecordBytes {
		return fmt.Errorf("%s exceeds maximum trace snapshot size", label)
	}
	if _, err := inspectJSON(raw); err != nil {
		return fmt.Errorf("%s contains invalid JSON: %w", label, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("%s is invalid JSON", label)
	}
	if decoded == nil && allowNull {
		return nil
	}
	snapshot, ok := decoded.(map[string]any)
	if !ok || len(snapshot) != 2 {
		return fmt.Errorf("%s must contain a protobuf snapshot envelope", label)
	}
	_, hasFields := snapshot["fields"].(map[string]any)
	oneofs, hasOneofs := snapshot["oneofs"].(map[string]any)
	if !hasFields || !hasOneofs {
		return fmt.Errorf("%s must contain object fields and oneofs", label)
	}
	for _, selected := range oneofs {
		if _, ok := selected.(string); !ok {
			return fmt.Errorf("%s oneof selections must be strings", label)
		}
	}
	if findSecretKey(decoded, authMethod) != "" {
		return fmt.Errorf("%s contains a forbidden secret field", label)
	}
	return nil
}

func inspectJSON(raw []byte) (map[string]struct{}, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("JSON is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	guard := jsonGuard{decoder: decoder}
	keys, err := guard.value(0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("JSON has trailing data")
	}
	return keys, nil
}

type jsonGuard struct {
	decoder *json.Decoder
	values  int
}

func (guard *jsonGuard) value(depth int) (map[string]struct{}, error) {
	guard.values++
	if depth > maxJSONDepth || guard.values > maxJSONValues {
		return nil, errors.New("JSON nesting or value budget exceeded")
	}
	token, err := guard.decoder.Token()
	if err != nil {
		return nil, errors.New("invalid JSON value")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil, nil
	}
	switch delimiter {
	case '{':
		return guard.object(depth + 1)
	case '[':
		return nil, guard.array(depth + 1)
	default:
		return nil, errors.New("invalid JSON delimiter")
	}
}

func (guard *jsonGuard) object(depth int) (map[string]struct{}, error) {
	seen := make(map[string]struct{})
	for guard.decoder.More() {
		token, err := guard.decoder.Token()
		if err != nil {
			return nil, errors.New("invalid JSON object key")
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("JSON object key is not a string")
		}
		if _, exists := seen[key]; exists {
			return nil, errors.New("duplicate JSON object key")
		}
		seen[key] = struct{}{}
		if _, err := guard.value(depth); err != nil {
			return nil, err
		}
	}
	if err := guard.end('}'); err != nil {
		return nil, err
	}
	return seen, nil
}

func (guard *jsonGuard) array(depth int) error {
	for guard.decoder.More() {
		if _, err := guard.value(depth); err != nil {
			return err
		}
	}
	return guard.end(']')
}

func (guard *jsonGuard) end(expected json.Delim) error {
	token, err := guard.decoder.Token()
	if err != nil || token != expected {
		return errors.New("unterminated JSON container")
	}
	return nil
}
