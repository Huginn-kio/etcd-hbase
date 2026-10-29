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
		if err := snapshot.messageField(typeOf.Field(index), value.Field(index), typeOf.Name(), authContext); err != nil {
			return messageSnapshot{}, err
		}
	}
	return snapshot, nil
}

func (snapshot *messageSnapshot) messageField(field reflect.StructField, value reflect.Value, messageName string, authContext bool) error {
	if field.Name == "XXX_unrecognized" {
		if value.Len() != 0 {
			return fmt.Errorf("unknown protobuf fields in %s cannot be represented losslessly", messageName)
		}
		return nil
	}
	if oneofName := field.Tag.Get("protobuf_oneof"); oneofName != "" {
		return snapshot.oneofFields(value, oneofName, messageName, field.Name, authContext)
	}
	return snapshot.ordinaryField(field, value, messageName, authContext)
}

func (snapshot *messageSnapshot) ordinaryField(field reflect.StructField, value reflect.Value, messageName string, authContext bool) error {
	wireKind, protoName, jsonName := parseProtoTag(field.Tag.Get("protobuf"))
	if protoName == "" || !value.CanInterface() {
		return nil
	}
	if snapshot.redactOrdinaryField(value, protoName, jsonName, authContext) {
		return nil
	}
	converted, err := snapshotValue(value, wireKind, authContext)
	if err != nil {
		return fmt.Errorf("snapshot %s.%s: %w", messageName, protoName, err)
	}
	snapshot.Fields[jsonName] = converted
	return nil
}

func (snapshot *messageSnapshot) redactOrdinaryField(value reflect.Value, protoName, jsonName string, authContext bool) bool {
	if authContext && containsName(protoName) {
		if value.Kind() == reflect.Slice {
			snapshot.Fields[jsonName+"_count"] = value.Len()
		} else {
			snapshot.Fields[jsonName+"_present"] = fieldPresent(value)
		}
		return true
	}
	if !isSensitiveName(protoName) {
		return false
	}
	if authContext {
		snapshot.Fields[jsonName+"_present"] = fieldPresent(value)
	}
	return true
}

func (snapshot *messageSnapshot) oneofFields(value reflect.Value, oneofName, messageName, fieldName string, authContext bool) error {
	if value.IsNil() {
		return nil
	}
	wrapper := indirect(value)
	if !wrapper.IsValid() || wrapper.Kind() != reflect.Struct {
		return fmt.Errorf("invalid oneof wrapper %s.%s", messageName, fieldName)
	}
	for index := 0; index < wrapper.NumField(); index++ {
		if err := snapshot.oneofField(wrapper.Type().Field(index), wrapper.Field(index), oneofName, messageName, authContext); err != nil {
			return err
		}
	}
	return nil
}

func (snapshot *messageSnapshot) oneofField(field reflect.StructField, value reflect.Value, oneofName, messageName string, authContext bool) error {
	wireKind, protoName, jsonName := parseProtoTag(field.Tag.Get("protobuf"))
	if protoName == "" {
		return nil
	}
	// Oneof redaction retains the existing proto-name presence representation.
	if isSensitiveName(protoName) || (authContext && containsName(protoName)) {
		snapshot.Fields[protoName+"_present"] = fieldPresent(value)
		return nil
	}
	converted, err := snapshotValue(value, wireKind, authContext)
	if err != nil {
		return fmt.Errorf("snapshot %s.%s: %w", messageName, protoName, err)
	}
	snapshot.Fields[jsonName] = converted
	snapshot.Oneofs[oneofName] = protoName
	return nil
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
	return snapshotFieldValue(value, wireKind, authContext)
}

func snapshotFieldValue(value reflect.Value, wireKind string, authContext bool) (any, error) {
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
		return snapshotFloat(value), nil
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return nil, errors.New("protobuf string is not valid UTF-8")
		}
		return value.String(), nil
	case reflect.Slice:
		return snapshotSlice(value, wireKind, authContext)
	case reflect.Array:
		return snapshotArray(value, wireKind, authContext)
	case reflect.Map:
		return snapshotMap(value, wireKind, authContext)
	case reflect.Struct:
		return snapshotMessage(value, authContext)
	default:
		return nil, fmt.Errorf("unsupported generated field kind %s", value.Kind())
	}
}

func snapshotFloat(value reflect.Value) any {
	floatValue := value.Float()
	if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
		return strconv.FormatFloat(floatValue, 'g', -1, value.Type().Bits())
	}
	return floatValue
}

func snapshotSlice(value reflect.Value, wireKind string, authContext bool) (any, error) {
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
}

func snapshotArray(value reflect.Value, wireKind string, authContext bool) (any, error) {
	items := make([]any, value.Len())
	for index := 0; index < value.Len(); index++ {
		converted, err := snapshotValue(value.Index(index), wireKind, authContext)
		if err != nil {
			return nil, err
		}
		items[index] = converted
	}
	return items, nil
}

func snapshotMap(value reflect.Value, wireKind string, authContext bool) (any, error) {
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
