package proprdbgen

import (
	"sort"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func rustDescriptors(plugin *protogen.Plugin, files []*protogen.File) (data []byte, err error) {
	selected := make(map[string]*descriptorpb.FileDescriptorProto)
	var addFile func(file *protogen.File)
	addFile = func(file *protogen.File) {
		path := file.Desc.Path()
		if selected[path] != nil {
			return
		}
		descriptor := proto.Clone(file.Proto).(*descriptorpb.FileDescriptorProto)
		selected[path] = descriptor
		descriptor.SourceCodeInfo = nil
		descriptor.Options = nil
		descriptor.Dependency = nil
		descriptor.PublicDependency = nil
		descriptor.WeakDependency = nil
		descriptor.Service = nil
		descriptor.Extension = nil
		dependencies := make(map[string]bool)
		var visit func(messages []*protogen.Message)
		visit = func(messages []*protogen.Message) {
			for _, message := range messages {
				for _, field := range message.Fields {
					var dependency string
					if field.Message != nil {
						dependency = field.Message.Desc.ParentFile().Path()
					}
					if field.Enum != nil {
						dependency = field.Enum.Desc.ParentFile().Path()
					}
					if dependency != "" && dependency != path {
						dependencies[dependency] = true
						addFile(plugin.FilesByPath[dependency])
					}
				}
				visit(message.Messages)
			}
		}
		visit(file.Messages)
		for dependency := range dependencies {
			descriptor.Dependency = append(descriptor.Dependency, dependency)
		}
		sort.Strings(descriptor.Dependency)
		normalizeRustMessages(descriptor.MessageType)
		normalizeRustEnums(descriptor.EnumType)
	}
	for _, file := range files {
		addFile(file)
	}
	set := &descriptorpb.FileDescriptorSet{}
	for _, file := range selected {
		set.File = append(set.File, file)
	}
	sort.Slice(set.File, func(i, j int) bool { return set.File[i].GetName() < set.File[j].GetName() })
	return proto.MarshalOptions{Deterministic: true}.Marshal(set)
}

func normalizeRustMessages(messages []*descriptorpb.DescriptorProto) {
	for _, message := range messages {
		if message.GetOptions().GetMapEntry() {
			message.Options = &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)}
		} else {
			message.Options = nil
		}
		message.Extension = nil
		message.ExtensionRange = nil
		for _, oneof := range message.OneofDecl {
			oneof.Options = nil
		}
		for _, field := range message.Field {
			if field.Options != nil && field.Options.Packed != nil {
				field.Options = &descriptorpb.FieldOptions{Packed: field.Options.Packed}
			} else {
				field.Options = nil
			}
		}
		normalizeRustMessages(message.NestedType)
		normalizeRustEnums(message.EnumType)
	}
}

func normalizeRustEnums(enums []*descriptorpb.EnumDescriptorProto) {
	for _, enum := range enums {
		if enum.GetOptions().GetAllowAlias() {
			enum.Options = &descriptorpb.EnumOptions{AllowAlias: proto.Bool(true)}
		} else {
			enum.Options = nil
		}
		for _, value := range enum.Value {
			value.Options = nil
		}
	}
}
