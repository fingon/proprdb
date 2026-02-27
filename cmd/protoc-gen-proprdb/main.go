package main

import (
	"fmt"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
	pluginpb "google.golang.org/protobuf/types/pluginpb"
)

func main() {
	opts := protogen.Options{}
	opts.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures |= uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}

			if err := proprdbgen.GenerateFile(plugin, file); err != nil {
				return fmt.Errorf("generate %s: %w", file.Desc.Path(), err)
			}
		}

		return nil
	})
}
