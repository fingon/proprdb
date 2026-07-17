package main

import (
	"fmt"
	"sort"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
	pluginpb "google.golang.org/protobuf/types/pluginpb"
)

func main() {
	opts := protogen.Options{}
	opts.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures |= uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		filesByPackage := make(map[string][]*protogen.File)
		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}
			key := string(file.GoImportPath)
			filesByPackage[key] = append(filesByPackage[key], file)
		}
		keys := make([]string, 0, len(filesByPackage))
		for key := range filesByPackage {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := proprdbgen.GenerateFiles(plugin, filesByPackage[key]); err != nil {
				return fmt.Errorf("generate Go package %s: %w", key, err)
			}
		}

		return nil
	})
}
