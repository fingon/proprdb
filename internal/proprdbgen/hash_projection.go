package proprdbgen

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	proprdbpb "github.com/fingon/proprdb/proto/proprdb"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const hashColumnPrefix = "_proprdb_hash_"

type hashSelection struct {
	Path  []int  `json:"p"`
	Field [4]int `json:"f"`
}
type hashSchema struct {
	Oneofs   []string        `json:"o,omitempty"`
	Paths    []hashSelection `json:"p"`
	Messages [][][4]int      `json:"m"`
}

func (c modelCollector) hashProjections(message *protogen.Message) (fields []projectedField, err error) {
	if !proto.HasExtension(message.Desc.Options(), proprdbpb.E_HashProjections) {
		return nil, nil
	}
	declarations, ok := proto.GetExtension(message.Desc.Options(), proprdbpb.E_HashProjections).([]*proprdbpb.HashProjection)
	if !ok {
		return nil, errors.New("invalid hash_projections option")
	}
	names := map[string]bool{}
	for _, declaration := range declarations {
		if declaration == nil || !dirtyWindowNamePattern.MatchString(declaration.Name) {
			return nil, errors.New("hash projection name must match [a-z][a-z0-9_]*")
		}
		if names[declaration.Name] {
			return nil, fmt.Errorf("duplicate hash projection %q", declaration.Name)
		}
		names[declaration.Name] = true
		if len(declaration.Fields) == 0 {
			return nil, fmt.Errorf("hash projection %q requires fields", declaration.Name)
		}
		schema := hashSchema{Messages: [][][4]int{}}
		messages := map[protoreflect.FullName]int{}
		recordOneof := func(f protoreflect.FieldDescriptor) {
			oneof := f.ContainingOneof()
			if oneof == nil || oneof.IsSynthetic() {
				return
			}
			members := make([]int, oneof.Fields().Len())
			for index := range members {
				members[index] = int(oneof.Fields().Get(index).Number())
			}
			slices.Sort(members)
			schema.Oneofs = append(schema.Oneofs, fmt.Sprintf("%s=%v", f.FullName(), members))
		}
		var describeMessage func(protoreflect.MessageDescriptor) (int, error)
		describeField := func(f protoreflect.FieldDescriptor) ([4]int, error) {
			recordOneof(f)
			value := [4]int{int(f.Number()), int(f.Kind()), 0, -1}
			if f.ParentFile().Syntax() != protoreflect.Proto3 || f.Kind() == protoreflect.GroupKind {
				return value, fmt.Errorf("hash field %s must use proto3", f.FullName())
			}
			switch {
			case f.IsMap():
				value[2] = 3
			case f.IsList():
				value[2] = 2
			case f.HasPresence():
				value[2] = 1
			}
			if f.Message() != nil {
				child, childErr := describeMessage(f.Message())
				if childErr != nil {
					return value, childErr
				}
				value[3] = child
			}
			return value, nil
		}
		describeMessage = func(m protoreflect.MessageDescriptor) (int, error) {
			if index, exists := messages[m.FullName()]; exists {
				return index, nil
			}
			index := len(schema.Messages)
			messages[m.FullName()] = index
			schema.Messages = append(schema.Messages, nil)
			descriptors := make([]protoreflect.FieldDescriptor, m.Fields().Len())
			for i := range descriptors {
				descriptors[i] = m.Fields().Get(i)
			}
			sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Number() < descriptors[j].Number() })
			for _, f := range descriptors {
				value, fieldErr := describeField(f)
				if fieldErr != nil {
					return 0, fieldErr
				}
				schema.Messages[index] = append(schema.Messages[index], value)
			}
			if schema.Messages[index] == nil {
				schema.Messages[index] = [][4]int{}
			}
			return index, nil
		}
		paths := slices.Clone(declaration.Fields)
		sort.Strings(paths)
		for i, path := range paths {
			for _, previous := range paths[:i] {
				if path == previous || strings.HasPrefix(path, previous+".") {
					return nil, fmt.Errorf("overlapping hash fields %q and %q", previous, path)
				}
			}
			current := message.Desc
			segments := strings.Split(path, ".")
			selection := hashSelection{}
			for j, segment := range segments {
				f := current.Fields().ByName(protoreflect.Name(segment))
				if f == nil {
					return nil, fmt.Errorf("unknown hash field %q", path)
				}
				recordOneof(f)
				selection.Path = append(selection.Path, int(f.Number()))
				if j == len(segments)-1 {
					value, fieldErr := describeField(f)
					if fieldErr != nil {
						return nil, fieldErr
					}
					selection.Field = value
				} else {
					if f.Message() == nil || f.IsList() || f.IsMap() {
						return nil, fmt.Errorf("hash path %q must traverse singular messages", path)
					}
					current = f.Message()
				}
			}
			schema.Paths = append(schema.Paths, selection)
		}
		slices.SortFunc(schema.Paths, func(a, b hashSelection) int { return slices.Compare(a.Path, b.Path) })
		slices.Sort(schema.Oneofs)
		schema.Oneofs = slices.Compact(schema.Oneofs)
		encoded, encodeErr := json.Marshal(schema)
		if encodeErr != nil {
			return nil, fmt.Errorf("encode hash schema: %w", encodeErr)
		}
		signature := sha256.Sum256(encoded)
		column := hashColumnPrefix + declaration.Name
		fields = append(fields, projectedField{ColumnName: column, ProtoFieldName: declaration.Name, SQLiteType: "BLOB", SQLiteDefault: "X''", SchemaSignature: column + ":bytes:hash=xxh3-64-v1-" + hex.EncodeToString(signature[:]), SwiftPropertyName: swiftFieldName(column), HashSchema: string(encoded)})
	}
	slices.SortFunc(fields, func(a, b projectedField) int { return strings.Compare(a.ColumnName, b.ColumnName) })
	return fields, nil
}
