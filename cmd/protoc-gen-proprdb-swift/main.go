package main

import (
	"flag"
	"fmt"

	"github.com/fingon/proprdb/internal/proprdbgen"
	"google.golang.org/protobuf/compiler/protogen"
	pluginpb "google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	rawOptions := make(map[string]string)
	flags.Func("Visibility", "Swift access level for generated declarations", func(value string) error {
		rawOptions["Visibility"] = value
		return nil
	})

	opts := protogen.Options{ParamFunc: flags.Set}
	opts.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures |= uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		options, err := proprdbgen.ParseSwiftGeneratorOptions(rawOptions)
		if err != nil {
			return err
		}

		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}

			if err := proprdbgen.GenerateSwiftFile(plugin, file, options); err != nil {
				return fmt.Errorf("generate %s: %w", file.Desc.Path(), err)
			}
		}

		return nil
	})
}
