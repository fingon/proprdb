package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	pluginpb "google.golang.org/protobuf/types/pluginpb"
)

func run() (err error) {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read generator request: %w", err)
	}
	request := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(input, request); err != nil {
		return fmt.Errorf("decode generator request: %w", err)
	}
	for _, file := range request.ProtoFile {
		if file.Options == nil {
			file.Options = &descriptorpb.FileOptions{}
		}
		if file.Options.GoPackage == nil {
			file.Options.GoPackage = proto.String("proprdb/rust/" + strings.ReplaceAll(file.GetPackage(), ".", "/") + ";rustproto")
		}
	}
	options := protogen.Options{}
	plugin, err := options.New(request)
	if err != nil {
		return fmt.Errorf("initialize generator: %w", err)
	}
	plugin.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	files := make([]*protogen.File, 0)
	for _, file := range plugin.Files {
		if file.Generate {
			files = append(files, file)
		}
	}
	if err := proprdbgen.GenerateRustFiles(plugin, files); err != nil {
		plugin.Error(err)
	}
	output, err := proto.Marshal(plugin.Response())
	if err != nil {
		return fmt.Errorf("encode generator response: %w", err)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		return fmt.Errorf("write generator response: %w", err)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("Rust generator failed", "error", err)
		os.Exit(1)
	}
}
