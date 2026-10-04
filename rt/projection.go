package proprdbrt

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func ProjectScalarPath(message proto.Message, path []protoreflect.FieldNumber) (value any, err error) {
	if message == nil || len(path) == 0 {
		return nil, errors.New("projection requires a message and field path")
	}
	current := message.ProtoReflect()
	if !current.IsValid() {
		return nil, errors.New("projection requires a valid message")
	}
	for index, number := range path {
		field := current.Descriptor().Fields().ByNumber(number)
		if field == nil {
			return nil, fmt.Errorf("projection field %d missing in %s", number, current.Descriptor().FullName())
		}
		if field.IsList() || field.IsMap() {
			return nil, fmt.Errorf("projection field %s must be singular", field.FullName())
		}
		if field.HasPresence() && !current.Has(field) {
			return nil, nil
		}
		scalar := current.Get(field)
		if index < len(path)-1 {
			if field.Kind() != protoreflect.MessageKind {
				return nil, fmt.Errorf("projection field %s must be a message", field.FullName())
			}
			current = scalar.Message()
			continue
		}
		switch field.Kind() {
		case protoreflect.BoolKind:
			return scalar.Bool(), nil
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			return scalar.Int(), nil
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			return int64(scalar.Uint()), nil
		case protoreflect.EnumKind:
			return int64(scalar.Enum()), nil
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			return scalar.Float(), nil
		case protoreflect.StringKind:
			return scalar.String(), nil
		case protoreflect.BytesKind:
			if len(scalar.Bytes()) == 0 {
				return []byte{}, nil
			}
			return scalar.Bytes(), nil
		default:
			return nil, fmt.Errorf("unsupported projection kind %s", field.Kind())
		}
	}
	return nil, errors.New("projection path has no scalar")
}
