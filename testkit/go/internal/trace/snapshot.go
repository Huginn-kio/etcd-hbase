package trace

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

type legacyProtoMessage interface {
	Reset()
	String() string
	ProtoMessage()
}
type messageSnapshot struct {
	Fields map[string]any    `json:"fields"`
	Oneofs map[string]string `json:"oneofs"`
}

// SnapshotProto serializes known etcd protobuf fields. Authentication names,
// passwords, and tokens are recorded only as presence flags. Unknown wire
// fields fail closed rather than disappearing from a supposedly complete trace.
func SnapshotProto(message legacyProtoMessage) (json.RawMessage, error) {
	if message == nil {
		return nil, errors.New("nil protobuf message")
	}
	value := reflect.ValueOf(message)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("protobuf message must be a non-nil pointer")
	}
	typeOf := value.Type().Elem()
	if typeOf.Kind() != reflect.Struct {
		return nil, errors.New("protobuf message must point to a generated struct")
	}
	if typeOf.PkgPath() == "" || !strings.HasPrefix(typeOf.PkgPath(), "go.etcd.io/etcd/api/v3/") {
		return nil, fmt.Errorf("unsupported protobuf package %q", typeOf.PkgPath())
	}
	guard := protoGuard{}
	if err := guard.visit(value, 0); err != nil {
		return nil, err
	}
	authContext := strings.HasPrefix(typeOf.Name(), "Auth") || strings.HasSuffix(typeOf.PkgPath(), "/authpb")
	snapshot, err := snapshotMessage(value, authContext)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshot)
}

func snapshotMessage(value reflect.Value, authContext bool) (messageSnapshot, error) {
	value = indirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return messageSnapshot{}, errors.New("invalid generated protobuf struct")
	}
	snapshot := messageSnapshot{Fields: make(map[string]any), Oneofs: make(map[string]string)}
	typeOf := value.Type()
	for index := 0; index < value.NumField(); index++ {
		field := typeOf.Field(index)
		fieldValue := value.Field(index)
		if field.Name == "XXX_unrecognized" {
			if fieldValue.Len() != 0 {
				return messageSnapshot{}, fmt.Errorf("unknown protobuf fields in %s cannot be represented losslessly", typeOf.Name())
			}
			continue
		}
		oneofName := field.Tag.Get("protobuf_oneof")
		if oneofName != "" {
			if fieldValue.IsNil() {
				continue
			}
			wrapper := indirect(fieldValue)
			if !wrapper.IsValid() || wrapper.Kind() != reflect.Struct {
				return messageSnapshot{}, fmt.Errorf("invalid oneof wrapper %s.%s", typeOf.Name(), field.Name)
			}
			for childIndex := 0; childIndex < wrapper.NumField(); childIndex++ {
				child := wrapper.Type().Field(childIndex)
				wireKind, protoName, jsonName := parseProtoTag(child.Tag.Get("protobuf"))
				if protoName == "" {
					continue
				}
				if isSensitiveName(protoName) || (authContext && containsName(protoName)) {
					snapshot.Fields[protoName+"_present"] = fieldPresent(wrapper.Field(childIndex))
					continue
				}
				converted, err := snapshotValue(wrapper.Field(childIndex), wireKind, authContext)
				if err != nil {
					return messageSnapshot{}, fmt.Errorf("snapshot %s.%s: %w", typeOf.Name(), protoName, err)
				}
				snapshot.Fields[jsonName] = converted
				snapshot.Oneofs[oneofName] = protoName
			}
			continue
		}
		wireKind, protoName, jsonName := parseProtoTag(field.Tag.Get("protobuf"))
		if protoName == "" || !fieldValue.CanInterface() {
			continue
		}
		if authContext && containsName(protoName) {
			if fieldValue.Kind() == reflect.Slice {
				snapshot.Fields[jsonName+"_count"] = fieldValue.Len()
			} else {
				snapshot.Fields[jsonName+"_present"] = fieldPresent(fieldValue)
			}
			continue
		}
		if isSensitiveName(protoName) {
			if authContext {
				snapshot.Fields[jsonName+"_present"] = fieldPresent(fieldValue)
			}
			continue
		}
		converted, err := snapshotValue(fieldValue, wireKind, authContext)
		if err != nil {
			return messageSnapshot{}, fmt.Errorf("snapshot %s.%s: %w", typeOf.Name(), protoName, err)
		}
		snapshot.Fields[jsonName] = converted
	}
	return snapshot, nil
}

func snapshotValue(value reflect.Value, wireKind string, authContext bool) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		if value.CanInterface() {
			if _, ok := value.Interface().(legacyProtoMessage); ok {
				return snapshotMessage(value, authContext)
			}
		}
		value = value.Elem()
	}
	if value.CanInterface() {
		if _, ok := value.Interface().(legacyProtoMessage); ok {
			return snapshotMessage(value, authContext)
		}
	}
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int64:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return value.Int(), nil
	case reflect.Uint64:
		return strconv.FormatUint(value.Uint(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		if wireKind == "bytes" && value.Kind() == reflect.Uint8 {
			return base64.StdEncoding.EncodeToString([]byte{byte(value.Uint())}), nil
		}
		return value.Uint(), nil
	case reflect.Float32, reflect.Float64:
		floatValue := value.Float()
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return strconv.FormatFloat(floatValue, 'g', -1, value.Type().Bits()), nil
		}
		return floatValue, nil
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return nil, errors.New("protobuf string is not valid UTF-8")
		}
		return value.String(), nil
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return base64.StdEncoding.EncodeToString(value.Bytes()), nil
		}
		items := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			converted, err := snapshotValue(value.Index(index), wireKind, authContext)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", index, err)
			}
			items[index] = converted
		}
		return items, nil
	case reflect.Array:
		items := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			converted, err := snapshotValue(value.Index(index), wireKind, authContext)
			if err != nil {
				return nil, err
			}
			items[index] = converted
		}
		return items, nil
	case reflect.Map:
		result := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := fmt.Sprint(iterator.Key().Interface())
			converted, err := snapshotValue(iterator.Value(), wireKind, authContext)
			if err != nil {
				return nil, fmt.Errorf("map value %q: %w", key, err)
			}
			result[key] = converted
		}
		return result, nil
	case reflect.Struct:
		return snapshotMessage(value, authContext)
	default:
		return nil, fmt.Errorf("unsupported generated field kind %s", value.Kind())
	}
}

func parseProtoTag(tag string) (wireKind, protoName, jsonName string) {
	if tag == "" || tag == "-" {
		return "", "", ""
	}
	parts := strings.Split(tag, ",")
	wireKind = parts[0]
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "name=") {
			protoName = strings.TrimPrefix(part, "name=")
		}
		if strings.HasPrefix(part, "json=") {
			jsonName = strings.TrimPrefix(part, "json=")
		}
	}
	if jsonName == "" {
		jsonName = lowerCamel(protoName)
	}
	return wireKind, protoName, jsonName
}

func lowerCamel(value string) string {
	if value == "" || !strings.Contains(value, "_") {
		return value
	}
	parts := strings.Split(value, "_")
	for index := 1; index < len(parts); index++ {
		if parts[index] != "" {
			parts[index] = strings.ToUpper(parts[index][:1]) + parts[index][1:]
		}
	}
	return strings.Join(parts, "")
}

func indirect(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func fieldPresent(value reflect.Value) bool {
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice {
		return !value.IsNil()
	}
	return !value.IsZero()
}

// A failed RPC commonly returns a typed nil pointer; its error is recorded independently.
func SnapshotNullableProto(message legacyProtoMessage) (json.RawMessage, error) {
	if message == nil || !indirect(reflect.ValueOf(message)).IsValid() {
		return json.RawMessage("null"), nil
	}
	return SnapshotProto(message)
}
