package trace

import (
	"errors"
	"reflect"
	"unicode/utf8"
)

const maxProtoDepth = 64

// Bound the generated object graph before allocating its JSON representation.
// This also rejects cycles in recursive Txn messages and unknown fields inside
// redacted authentication messages.
type protoGuard struct {
	values int
	bytes  int
}

func (guard *protoGuard) visit(value reflect.Value, depth int) error {
	guard.values++
	if depth > maxProtoDepth || guard.values > maxJSONValues || guard.bytes > maxRecordBytes/2 {
		return errors.New("protobuf snapshot budget exceeded")
	}
	value = indirect(value)
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Struct:
		if err := guard.visitStruct(value, depth); err != nil {
			return err
		}
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return errors.New("protobuf string is not valid UTF-8")
		}
		guard.bytes += value.Len()
	case reflect.Slice, reflect.Array:
		if err := guard.visitSequence(value, depth); err != nil {
			return err
		}
	case reflect.Map:
		if err := guard.visitMap(value, depth); err != nil {
			return err
		}
	}
	if guard.bytes > maxRecordBytes/2 {
		return errors.New("protobuf byte budget exceeded")
	}
	return nil
}

func (guard *protoGuard) visitStruct(value reflect.Value, depth int) error {
	for index := 0; index < value.NumField(); index++ {
		field := value.Type().Field(index)
		child := value.Field(index)
		if field.Name == "XXX_unrecognized" && child.Len() != 0 {
			return errors.New("unknown protobuf fields cannot be represented losslessly")
		}
		if field.Tag.Get("protobuf") == "" && field.Tag.Get("protobuf_oneof") == "" {
			continue
		}
		if err := guard.visit(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (guard *protoGuard) visitSequence(value reflect.Value, depth int) error {
	if value.Type().Elem().Kind() == reflect.Uint8 {
		guard.bytes += value.Len()
		return nil
	}
	if value.Len() > maxJSONValues-guard.values {
		return errors.New("protobuf collection budget exceeded")
	}
	for index := 0; index < value.Len(); index++ {
		if err := guard.visit(value.Index(index), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (guard *protoGuard) visitMap(value reflect.Value, depth int) error {
	if value.Len() > (maxJSONValues-guard.values)/2 {
		return errors.New("protobuf collection budget exceeded")
	}
	iterator := value.MapRange()
	for iterator.Next() {
		if err := guard.visit(iterator.Key(), depth+1); err != nil {
			return err
		}
		if err := guard.visit(iterator.Value(), depth+1); err != nil {
			return err
		}
	}
	return nil
}
