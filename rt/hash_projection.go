package proprdbrt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	hashFormat       = "PDBH\x01"
	hashMaxDepth     = 100
	hashModePresence = 1
	hashModeRepeated = 2
	hashModeMap      = 3
)

type hashSelection struct {
	Path  []int  `json:"p"`
	Field [4]int `json:"f"`
}
type hashSchema struct {
	Paths    []hashSelection `json:"p"`
	Messages [][][4]int      `json:"m"`
}
type hashWireValue struct {
	wire  protowire.Type
	value uint64
	data  []byte
}

var hashSchemas sync.Map

func ProjectHash(message proto.Message, definition string) (fingerprint []byte, err error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, errors.New("hash projection requires a message")
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal hash input: %w", err)
	}
	canonical, err := CanonicalHashBytes(data, definition)
	if err != nil {
		return nil, err
	}
	return binary.BigEndian.AppendUint64(nil, xxh3.Hash(canonical)), nil
}

func CanonicalHashBytes(data []byte, definition string) (canonical []byte, err error) {
	var schema *hashSchema
	if cached, ok := hashSchemas.Load(definition); ok {
		schema = cached.(*hashSchema)
	} else {
		schema = &hashSchema{}
		if err := json.Unmarshal([]byte(definition), schema); err != nil {
			return nil, fmt.Errorf("decode hash schema: %w", err)
		}
		if err := schema.validate(); err != nil {
			return nil, err
		}
		cached, _ := hashSchemas.LoadOrStore(definition, schema)
		schema = cached.(*hashSchema)
	}
	canonical = append([]byte(hashFormat), hashU32(len(schema.Paths))...)
	for _, selection := range schema.Paths {
		canonical = append(canonical, hashU32(len(selection.Path))...)
		for _, number := range selection.Path {
			canonical = append(canonical, hashU32(number)...)
		}
		current := data
		depth := 0
		for _, number := range selection.Path[:len(selection.Path)-1] {
			fields, parseErr := hashParseWire(current)
			if parseErr != nil {
				return nil, parseErr
			}
			values := fields[number]
			if len(values) == 0 {
				break
			}
			current = nil
			for _, value := range values {
				if value.wire != protowire.BytesType {
					return nil, errors.New("invalid hash message wire type")
				}
				current = append(current, value.data...)
			}
			depth++
		}
		canonical = append(canonical, hashU32(depth)...)
		if depth != len(selection.Path)-1 {
			continue
		}
		fields, parseErr := hashParseWire(current)
		if parseErr != nil {
			return nil, parseErr
		}
		encoded, encodeErr := schema.field(selection.Field, fields[selection.Field[0]], 0)
		if encodeErr != nil {
			return nil, encodeErr
		}
		canonical = append(canonical, encoded...)
	}
	return canonical, nil
}

func (s *hashSchema) validate() error {
	if len(s.Paths) == 0 {
		return errors.New("empty hash selections")
	}
	validate := func(f [4]int) error {
		if f[0] <= 0 || f[1] < 1 || f[1] > 18 || f[1] == 10 || f[2] < 0 || f[2] > hashModeMap || (f[1] == 11 && (f[3] < 0 || f[3] >= len(s.Messages))) {
			return errors.New("invalid hash field descriptor")
		}
		return nil
	}
	for _, p := range s.Paths {
		if len(p.Path) == 0 || p.Path[len(p.Path)-1] != p.Field[0] {
			return errors.New("invalid hash path")
		}
		if err := validate(p.Field); err != nil {
			return err
		}
	}
	for _, m := range s.Messages {
		for _, f := range m {
			if err := validate(f); err != nil {
				return err
			}
		}
	}
	return nil
}

func hashU32(value int) []byte { return binary.LittleEndian.AppendUint32(nil, uint32(value)) }
func hashFrame(data []byte) []byte {
	return append(binary.LittleEndian.AppendUint64(nil, uint64(len(data))), data...)
}

func hashParseWire(data []byte) (fields map[int][]hashWireValue, err error) {
	fields = map[int][]hashWireValue{}
	for len(data) > 0 {
		number, wire, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("decode hash tag: %w", protowire.ParseError(n))
		}
		data = data[n:]
		value := hashWireValue{wire: wire}
		switch wire {
		case protowire.VarintType:
			value.value, n = protowire.ConsumeVarint(data)
		case protowire.Fixed32Type:
			var v uint32
			v, n = protowire.ConsumeFixed32(data)
			value.value = uint64(v)
		case protowire.Fixed64Type:
			value.value, n = protowire.ConsumeFixed64(data)
		case protowire.BytesType:
			value.data, n = protowire.ConsumeBytes(data)
		default:
			n = protowire.ConsumeFieldValue(number, wire, data)
		}
		if n < 0 {
			return nil, fmt.Errorf("decode hash field: %w", protowire.ParseError(n))
		}
		if wire != protowire.StartGroupType {
			fields[int(number)] = append(fields[int(number)], value)
		}
		data = data[n:]
	}
	return fields, nil
}

func (s *hashSchema) field(f [4]int, raw []hashWireValue, depth int) ([]byte, error) {
	if depth > hashMaxDepth {
		return nil, errors.New("hash message nesting exceeds limit")
	}
	kind, mode := protoreflect.Kind(f[1]), f[2]
	values := [][]byte{}
	if mode == hashModeRepeated && kind != protoreflect.StringKind && kind != protoreflect.MessageKind && kind != protoreflect.BytesKind {
		expanded := []hashWireValue{}
		for _, v := range raw {
			if v.wire != protowire.BytesType {
				expanded = append(expanded, v)
				continue
			}
			packed := v.data
			for len(packed) > 0 {
				var n int
				item := hashWireValue{wire: protowire.VarintType}
				switch kind {
				case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
					item.wire = protowire.Fixed64Type
					item.value, n = protowire.ConsumeFixed64(packed)
				case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
					item.wire = protowire.Fixed32Type
					var value uint32
					value, n = protowire.ConsumeFixed32(packed)
					item.value = uint64(value)
				default:
					item.value, n = protowire.ConsumeVarint(packed)
				}
				if n < 0 {
					return nil, fmt.Errorf("decode packed hash input: %w", protowire.ParseError(n))
				}
				packed = packed[n:]
				expanded = append(expanded, item)
			}
		}
		raw = expanded
	}
	if mode <= hashModePresence {
		if len(raw) == 0 && mode == 0 {
			v := hashWireValue{}
			switch kind {
			case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
				v.wire = protowire.Fixed64Type
			case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
				v.wire = protowire.Fixed32Type
			case protoreflect.StringKind, protoreflect.MessageKind, protoreflect.BytesKind:
				v.wire = protowire.BytesType
			}
			raw = []hashWireValue{v}
		}
		if len(raw) > 1 {
			if kind == protoreflect.MessageKind {
				joined := hashWireValue{wire: protowire.BytesType}
				for _, v := range raw {
					joined.data = append(joined.data, v.data...)
				}
				raw = []hashWireValue{joined}
			} else {
				raw = raw[len(raw)-1:]
			}
		}
	}
	for _, v := range raw {
		encoded, err := s.value(f, v, depth)
		if err != nil {
			return nil, err
		}
		values = append(values, encoded)
	}
	if mode == hashModeMap {
		slices.SortFunc(values, bytes.Compare)
	}
	result := append(hashU32(f[0]), byte(kind), byte(mode))
	result = binary.LittleEndian.AppendUint64(result, uint64(len(values)))
	for _, v := range values {
		result = append(result, hashFrame(v)...)
	}
	return result, nil
}

func (s *hashSchema) value(f [4]int, v hashWireValue, depth int) ([]byte, error) {
	kind := protoreflect.Kind(f[1])
	expected := protowire.VarintType
	switch kind {
	case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		expected = protowire.Fixed64Type
	case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		expected = protowire.Fixed32Type
	case protoreflect.StringKind, protoreflect.MessageKind, protoreflect.BytesKind:
		expected = protowire.BytesType
	}
	if v.wire != expected {
		return nil, errors.New("invalid hash scalar wire type")
	}
	switch kind {
	case protoreflect.MessageKind:
		fields, err := hashParseWire(v.data)
		if err != nil {
			return nil, err
		}
		result := []byte{}
		for _, child := range s.Messages[f[3]] {
			encoded, err := s.field(child, fields[child[0]], depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, encoded...)
		}
		return result, nil
	case protoreflect.StringKind, protoreflect.BytesKind:
		return v.data, nil
	case protoreflect.DoubleKind:
		value := math.Float64frombits(v.value)
		if math.IsNaN(value) {
			v.value = 0x7ff8000000000000
		} else if value == 0 {
			v.value = 0
		}
	case protoreflect.FloatKind:
		value := math.Float32frombits(uint32(v.value))
		if math.IsNaN(float64(value)) {
			v.value = 0x7fc00000
		} else if value == 0 {
			v.value = 0
		}
	case protoreflect.Int32Kind, protoreflect.EnumKind, protoreflect.Sfixed32Kind:
		v.value = uint64(int64(int32(v.value)))
	case protoreflect.BoolKind:
		if v.value != 0 {
			v.value = 1
		}
	case protoreflect.Uint32Kind:
		v.value = uint64(uint32(v.value))
	case protoreflect.Sint32Kind:
		v.value = uint64(int64(int32(protowire.DecodeZigZag(v.value))))
	case protoreflect.Sint64Kind:
		v.value = uint64(protowire.DecodeZigZag(v.value))
	}
	if kind == protoreflect.FloatKind || kind == protoreflect.Fixed32Kind || kind == protoreflect.Uint32Kind || kind == protoreflect.Sfixed32Kind || kind == protoreflect.Int32Kind || kind == protoreflect.EnumKind || kind == protoreflect.Sint32Kind {
		return binary.LittleEndian.AppendUint32(nil, uint32(v.value)), nil
	}
	if kind == protoreflect.BoolKind {
		return []byte{byte(v.value)}, nil
	}
	return binary.LittleEndian.AppendUint64(nil, v.value), nil
}
