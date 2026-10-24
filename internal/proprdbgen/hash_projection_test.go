package proprdbgen

import (
	"testing"

	proprdbpb "github.com/fingon/proprdb/proto/proprdb"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
	"gotest.tools/v3/assert"
)

func TestHashProjectionDefinitionSignature(t *testing.T) {
	const detailType = ".hashsig.Root.Detail"
	makeFile := func() *descriptorpb.FileDescriptorProto {
		options := &descriptorpb.MessageOptions{}
		proto.SetExtension(options, proprdbpb.E_HashProjections, []*proprdbpb.HashProjection{{Name: "support", Fields: []string{"detail"}}})
		field := func(name string, number, group int32) *descriptorpb.FieldDescriptorProto {
			return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), OneofIndex: proto.Int32(group)}
		}
		return &descriptorpb.FileDescriptorProto{Name: proto.String("hash.proto"), Syntax: proto.String("proto3"), Package: proto.String("hashsig"), Options: &descriptorpb.FileOptions{GoPackage: proto.String("example.com/hashsig;hashsig")}, MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Root"), Options: options, Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("detail"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(detailType)}}, NestedType: []*descriptorpb.DescriptorProto{{Name: proto.String("Detail"), OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("first")}, {Name: proto.String("second")}}, Field: []*descriptorpb.FieldDescriptorProto{field("a", 1, 0), field("b", 2, 0), field("c", 3, 1)}}}}}}
	}
	signature := func(file *descriptorpb.FileDescriptorProto) string {
		plugin, err := (protogen.Options{}).New(&pluginpb.CodeGeneratorRequest{ProtoFile: []*descriptorpb.FileDescriptorProto{file}})
		assert.NilError(t, err)
		fields, err := (modelCollector{}).hashProjections(plugin.Files[0].Messages[0])
		assert.NilError(t, err)
		return fields[0].SchemaSignature
	}
	expected := signature(makeFile())
	for _, tc := range []struct {
		name   string
		change func(*descriptorpb.FileDescriptorProto)
		equal  bool
	}{
		{"unchanged", func(*descriptorpb.FileDescriptorProto) {}, true},
		{"unselected root field", func(f *descriptorpb.FileDescriptorProto) {
			f.MessageType[0].Field = append(f.MessageType[0].Field, &descriptorpb.FieldDescriptorProto{Name: proto.String("label"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()})
		}, true},
		{"nested field", func(f *descriptorpb.FileDescriptorProto) {
			m := f.MessageType[0].NestedType[0]
			m.Field = append(m.Field, &descriptorpb.FieldDescriptorProto{Name: proto.String("added"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()})
		}, false},
		{"oneof regrouping", func(f *descriptorpb.FileDescriptorProto) {
			f.MessageType[0].NestedType[0].Field[1].OneofIndex = proto.Int32(1)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := makeFile()
			tc.change(file)
			assert.Equal(t, signature(file) == expected, tc.equal)
		})
	}
}
